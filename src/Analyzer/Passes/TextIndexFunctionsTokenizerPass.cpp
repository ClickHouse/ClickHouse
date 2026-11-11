#include <Analyzer/Passes/TextIndexFunctionsTokenizerPass.h>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/LambdaNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/TableFunctionNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/UnionNode.h>
#include <Analyzer/Utils.h>
#include <DataTypes/DataTypeMapHelpers.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/ExpressionActions.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/MergeTreeIndexConditionText.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeIndexJSONSubcolumnHelper.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/RPNBuilder.h>
#include <Storages/StorageDistributed.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <algorithm>
#include <map>
#include <tuple>

namespace DB
{

namespace
{

constexpr size_t max_substitutions = 64;

struct Resolution
{
    QueryTreeNodePtr expression;
    StoragePtr storage;
    /// Only the Map then describes the expression.
    bool foreign_map_key = false;
};

StoragePtr getSourceStorage(const IQueryTreeNode & source)
{
    if (const auto * table_node = source.as<TableNode>())
        return table_node->getStorage();

    if (const auto * table_function_node = source.as<TableFunctionNode>())
        return table_function_node->getStorage();

    return nullptr;
}

/// For a `Distributed`, the shards' local table; `storage` unless this node is provably a shard.
StoragePtr getIndexStorage(const StoragePtr & storage, const ContextPtr & context)
{
    const auto * distributed = typeid_cast<const StorageDistributed *>(storage.get());
    if (!distributed)
        return storage;

    /// An initiator outside the cluster can hold an unrelated table of that name.
    const auto cluster = distributed->getCluster();
    if (!cluster || cluster->getLocalShardCount() == 0)
        return storage;

    const auto remote_database = distributed->getRemoteDatabaseName();
    const auto remote_table = distributed->getRemoteTableName();
    if (remote_database.empty() || remote_table.empty())
        return storage;

    auto local_id = context->tryResolveStorageID(StorageID{remote_database, remote_table});
    if (!local_id)
        return storage;

    auto local_table = DatabaseCatalog::instance().tryGetTable(local_id, context);
    if (!local_table)
        return storage;

    return local_table;
}

/// For a subcolumn, the column it belongs to.
std::optional<NameAndTypePair> columnOwning(const StorageInMemoryMetadata & metadata, const String & name)
{
    auto options = GetColumnsOptions(GetColumnsOptions::All).withRegularSubcolumns();
    const auto column = metadata.getColumns().tryGetColumn(options, name);
    if (!column)
        return {};

    return metadata.getColumns().tryGetColumn(options, column->getNameInStorage());
}

/// Whether `carrier` reads through `inside`, both subcolumns of `type`. Compares the substream path the
/// resolver itself picks, so a sibling that merely flattens to the same name is not taken for a path.
bool resolvesInside(const DataTypePtr & type, const String & carrier, const String & inside)
{
    const auto carrier_info = type->tryGetSubcolumnInfo(carrier);
    const auto inside_info = type->tryGetSubcolumnInfo(inside);
    if (!carrier_info || !inside_info)
        return false;

    const auto & outer = inside_info->substreams_path;
    const auto & inner = carrier_info->substreams_path;
    if (inner.size() < outer.size())
        return false;

    for (size_t i = 0; i < outer.size(); ++i)
        if (outer[i].toString() != inner[i].toString())
            return false;

    return true;
}

/// Spelled the way `IndexDescription::column_names` is.
String indexExpressionName(const IQueryTreeNode & resolved)
{
    ConvertToASTOptions ast_options;
    ast_options.add_cast_for_constants = false;
    ast_options.fully_qualified_identifiers = false;
    return resolved.toAST(ast_options)->getColumnName();
}

std::optional<size_t> projectionPosition(const NamesAndTypes & projection_columns, const String & name)
{
    for (size_t i = 0; i < projection_columns.size(); ++i)
    {
        if (projection_columns[i].name == name)
            return i;
    }

    return {};
}

QueryTreeNodePtr projectionAt(const IQueryTreeNode & source, size_t position, const ContextPtr & context, size_t & substitutions);
Resolution resolveToTableColumns(
    const QueryTreeNodePtr & expression, const ContextPtr & context, size_t & substitutions, bool tolerate_foreign_map_key);

/// A row comes from one branch, so every branch must expose the same indexed expression of one table.
QueryTreeNodePtr findAgreedUnionProjection(const UnionNode & union_node, size_t position, const ContextPtr & context, size_t & substitutions)
{
    String agreed_name;
    QueryTreeNodePtr agreed;
    const IStorage * agreed_storage = nullptr;

    for (const auto & branch : union_node.getQueries().getNodes())
    {
        auto projection = projectionAt(*branch, position, context, substitutions);
        if (!projection)
            return nullptr;

        auto resolution = resolveToTableColumns(projection, context, substitutions, /*tolerate_foreign_map_key=*/ false);
        if (!resolution.expression)
            return nullptr;

        const auto name = indexExpressionName(*resolution.expression);
        const auto * storage = resolution.storage.get();

        if (!agreed)
        {
            agreed_name = name;
            agreed = std::move(projection);
            agreed_storage = storage;
        }
        else if (name != agreed_name || storage != agreed_storage)
        {
            return nullptr;
        }
    }

    return agreed;
}

QueryTreeNodePtr projectionAt(const IQueryTreeNode & source, size_t position, const ContextPtr & context, size_t & substitutions)
{
    if (const auto * query_node = source.as<QueryNode>())
    {
        const auto & projection = query_node->getProjection().getNodes();
        return position < projection.size() ? projection[position] : QueryTreeNodePtr{};
    }

    if (const auto * union_node = source.as<UnionNode>())
        return findAgreedUnionProjection(*union_node, position, context, substitutions);

    return nullptr;
}

QueryTreeNodePtr findProjection(const IQueryTreeNode & source, const String & name, const ContextPtr & context, size_t & substitutions)
{
    std::optional<size_t> position;

    if (const auto * query_node = source.as<QueryNode>())
        position = projectionPosition(query_node->getProjectionColumns(), name);
    else if (const auto * union_node = source.as<UnionNode>())
        position = projectionPosition(union_node->computeProjectionColumns(), name);

    return position ? projectionAt(source, *position, context, substitutions) : QueryTreeNodePtr{};
}

/// Null unless the expression reads one table occurrence, which is all an index describes.
Resolution resolveToTableColumns(
    const QueryTreeNodePtr & expression, const ContextPtr & context, size_t & substitutions, bool tolerate_foreign_map_key)
{
    StoragePtr storage;
    /// Not the storage: a self-join shares it while pairing values from two rows.
    const IQueryTreeNode * occurrence = nullptr;

    bool foreign_map_key = false;
    std::vector<const IQueryTreeNode *> local_binders;
    /// Agreed arm by arm, which says nothing about how two of them relate inside one arm.
    size_t union_substitutions = 0;

    /// `{substituted, failed}`, where null means unchanged; a substitution only goes into a copy.
    auto substitute = [&](const QueryTreeNodePtr & current, auto & self) -> std::pair<QueryTreeNodePtr, bool>
    {
        const auto * column_node = current->as<ColumnNode>();
        if (!column_node)
        {
            if (const auto * lambda = current->as<LambdaNode>())
                local_binders.push_back(&lambda->getArguments());

            /// `mapValues(m)` indexes the value whatever the key is, so a foreign key still resolves.
            if (const auto * element = current->as<FunctionNode>();
                element && element->getFunctionName() == "arrayElement" && element->getArguments().getNodes().size() == 2)
            {
                const auto & lookup = element->getArguments().getNodes();

                auto [map, map_failed] = self(lookup.front(), self);
                if (map_failed)
                    return {nullptr, true};

                auto [found_key, key_failed] = self(lookup.back(), self);
                if (key_failed)
                {
                    if (!tolerate_foreign_map_key)
                        return {nullptr, true};

                    foreign_map_key = true;
                    found_key = nullptr;
                }

                if (!map && !found_key)
                    return {nullptr, false};

                auto substituted = current->clone();
                auto & substituted_lookup = substituted->as<FunctionNode &>().getArguments().getNodes();
                if (map)
                    substituted_lookup.front() = std::move(map);
                if (found_key)
                    substituted_lookup.back() = std::move(found_key);

                return {substituted, false};
            }

            QueryTreeNodePtr substituted;
            const auto & children = current->getChildren();

            for (size_t i = 0; i < children.size(); ++i)
            {
                if (!children[i])
                    continue;

                auto [replacement, failed] = self(children[i], self);
                if (failed)
                    return {nullptr, true};

                if (!replacement)
                    continue;

                if (!substituted)
                    substituted = current->clone();
                substituted->getChildren()[i] = std::move(replacement);
            }

            return {substituted, false};
        }

        const auto source = column_node->getColumnSourceOrNull();
        if (!source)
            return {nullptr, true};

        /// An index names its own lambda's parameter the same way; a capture names something else.
        if (source->as<LambdaArgumentsNode>())
            return {nullptr, std::ranges::find(local_binders, static_cast<const IQueryTreeNode *>(source.get())) == local_binders.end()};

        QueryTreeNodePtr what_it_reads;

        if (const auto column_storage = getSourceStorage(*source))
        {
            if (!occurrence)
            {
                occurrence = source.get();
                storage = getIndexStorage(column_storage, context);
            }
            else if (occurrence != source.get())
            {
                return {nullptr, true};
            }

                    if (!column_node->hasExpression())
                return {nullptr, false};

            what_it_reads = column_node->getExpression();
        }
        else
        {
            what_it_reads = findProjection(*source, column_node->getColumnName(), context, substitutions);

            if (what_it_reads && source->as<UnionNode>() && ++union_substitutions > 1)
                return {nullptr, true};
        }

        if (!what_it_reads || ++substitutions > max_substitutions)
            return {nullptr, true};

        auto [replacement, failed] = self(what_it_reads, self);
        if (failed)
            return {nullptr, true};

        return {replacement ? replacement : what_it_reads, false};
    };

    auto [substituted, failed] = substitute(expression, substitute);
    if (failed || !storage)
        return {};

    return {substituted ? substituted : expression, storage, foreign_map_key};
}

/// The operand of a conversion that cannot change a value, by the rule the scan matches under.
const IQueryTreeNode * losslessConversionArgument(const IQueryTreeNode & node)
{
    const auto * function_node = node.as<FunctionNode>();
    if (!function_node)
        return nullptr;

    const auto & arguments = function_node->getArguments().getNodes();
    const auto & function_name = function_node->getFunctionName();
    const bool is_cast = (function_name == "CAST" || function_name == "_CAST") && arguments.size() == 2;
    const bool is_wrapper = (function_name == "toNullable" || function_name == "toLowCardinality") && arguments.size() == 1;

    if (!is_cast && !is_wrapper)
        return nullptr;
    if (!isLosslessConversion(arguments.front()->getResultType(), function_node->getResultType()))
        return nullptr;

    return arguments.front().get();
}

struct Carriers
{
    Names names;
    /// Tried as a JSON path only: the cast that reads one is not lossless.
    String json_subcolumn;
};

Carriers carrierNames(const IQueryTreeNode & resolved, const String & resolved_name, const IsShadowingColumn & is_shadowing_column)
{
    Carriers carriers{.names = {resolved_name}, .json_subcolumn = {}};

    /// To the bottom: the scan matches under any number of them.
    const IQueryTreeNode * node = &resolved;
    while (const auto * under = losslessConversionArgument(*node))
    {
        carriers.names.push_back(indexExpressionName(*under));
        node = under;
    }

    for (size_t i = 0, exposed = carriers.names.size(); i < exposed; ++i)
    {
        if (auto parsed = tryParseMapSubcolumnName(carriers.names[i], is_shadowing_column))
            carriers.names.push_back("mapValues(" + parsed->first + ")");
    }

    const auto * function_node = node->as<FunctionNode>();
    if (!function_node)
        return carriers;

    const auto & arguments = function_node->getArguments().getNodes();
    if (arguments.size() != 2)
        return carriers;

    const auto & function_name = function_node->getFunctionName();

    if (function_name == "arrayElement")
        carriers.names.push_back("mapValues(" + indexExpressionName(*arguments.front()) + ")");
    /// Peeling stopped, so the `CAST` is lossy: only a JSON path is read that way.
    else if (function_name == "CAST" || function_name == "_CAST")
        carriers.json_subcolumn = indexExpressionName(*arguments.front());

    return carriers;
}

class TextIndexFunctionsTokenizerVisitor : public InDepthQueryTreeVisitorWithContext<TextIndexFunctionsTokenizerVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<TextIndexFunctionsTokenizerVisitor>;
    using Base::Base;

    void enterImpl(QueryTreeNodePtr & node)
    {
        /// By value: resolving the node below assigns the member a reference would alias.
        if (const String function_name = forwardTokenizer(node); !function_name.empty())
            resolveOrdinaryFunctionNodeByName(*node->as<FunctionNode>(), function_name, getContext());
    }

private:
    /// Constructing a tokenizer can load a dictionary; forwardability depends on the function.
    std::map<std::tuple<const IStorage *, String, String>, String> tokenizer_cache;

    /// Appends the tokenizer the text index on the haystack implies, and names the function if it did.
    /// The node is left unresolved: the only caller that keeps it re-resolves it.
    String forwardTokenizer(QueryTreeNodePtr & node)
    {
        auto * function_node = node->as<FunctionNode>();
        if (!function_node || !function_node->isOrdinaryFunction())
            return {};

        String function_name = function_node->getFunctionName();
        if (!doesTextSearchFunctionAcceptTokenizer(function_name))
            return {};

        /// No explicit tokenizer is the only case the index decides.
        auto & arguments = function_node->getArguments().getNodes();
        if (arguments.size() != 2)
            return {};

        auto tokenizer = findTextIndexTokenizer(arguments.front(), function_name);
        if (tokenizer.empty())
            return {};

        arguments.push_back(std::make_shared<ConstantNode>(std::move(tokenizer), std::make_shared<DataTypeString>()));
        return function_name;
    }

    void forwardNestedTokenizers(QueryTreeNodePtr & node)
    {
        for (auto & child : node->getChildren())
            if (child)
                forwardNestedTokenizers(child);

        forwardTokenizer(node);
    }

    String findTextIndexTokenizer(const QueryTreeNodePtr & expression, const String & function_name)
    {
        size_t substitutions = 0;
        auto resolution = resolveToTableColumns(expression, getContext(), substitutions, /*tolerate_foreign_map_key=*/ true);
        if (!resolution.expression)
            return {};

        /// Only the outer key is exempt: the Map it reads must still resolve on its own.
        String map_only_name;
        if (resolution.foreign_map_key)
        {
            const IQueryTreeNode * node = resolution.expression.get();
            while (const auto * under = losslessConversionArgument(*node))
                node = under;

            const auto * element = node->as<FunctionNode>();
            if (!element || element->getFunctionName() != "arrayElement" || element->getArguments().getNodes().size() != 2)
                return {};

            resolution = resolveToTableColumns(
                element->getArguments().getNodes().front(), getContext(), substitutions, /*tolerate_foreign_map_key=*/ false);
            if (!resolution.expression)
                return {};

            map_only_name = "mapValues(" + indexExpressionName(*resolution.expression) + ")";
        }

        /// A nested call is rewritten too, so the lookup has to name the expression that will run,
        /// whichever of the two the traversal reaches first. The clone never leaves this function.
        if (map_only_name.empty())
        {
            resolution.expression = resolution.expression->clone();
            forwardNestedTokenizers(resolution.expression);
        }

        const auto & resolved = *resolution.expression;
        const auto & storage = resolution.storage;

        const auto metadata = storage->getInMemoryMetadataPtr(getContext(), /*bypass_metadata_cache=*/ false);
        const auto & indices = metadata->getSecondaryIndices();
        if (indices.empty())
            return {};

        const auto name = map_only_name.empty() ? indexExpressionName(resolved) : map_only_name;
        auto key = std::make_tuple(storage.get(), name, function_name);

        auto [it, inserted] = tokenizer_cache.try_emplace(key);
        if (!inserted)
            return it->second;

        const auto is_shadowing_column = getMapSubcolumnShadowingCheck(metadata);
        const Carriers carriers = map_only_name.empty()
            ? carrierNames(resolved, name, is_shadowing_column)
            : Carriers{.names = {name}, .json_subcolumn = {}};

        for (const auto & index : indices)
        {
            if (index.type != TEXT_INDEX_NAME || index.column_names.size() != 1)
                continue;

            const auto normalized_name = getNormalizedIndexColumnName(index);
            /// What the index condition matches with, so a subcolumn inside a typed path is refused.
            const auto json_argument_types = collectJSONIndexArgumentTypes(*index.expression);

            auto matches_json = [&](const String & carrier)
            {
                auto matched = tryMatchJSONSubcolumnToIndex(carrier, index.column_names, "JSONAllValues", json_argument_types);
                if (!matched)
                    return false;

                /// A declared column of that name is read instead, unless the carrier reads through the
                /// indexed value.
                const auto owner = columnOwning(*metadata, carrier);
                if (!owner || owner->name == matched->json_column_name)
                    return true;
                if (!matched->json_column_name.starts_with(owner->name + "."))
                    return false;

                const size_t prefix = owner->name.size() + 1;
                return resolvesInside(owner->type, carrier.substr(prefix), matched->json_column_name.substr(prefix));
            };

            const bool describes = std::ranges::any_of(carriers.names, [&](const String & carrier)
            {
                return carrier == index.column_names.front()
                    || (normalized_name && *normalized_name == carrier)
                    || matches_json(carrier);
            }) || (!carriers.json_subcolumn.empty() && matches_json(carriers.json_subcolumn));

            if (!describes)
                continue;

            /// Ambiguous when several match; the first is also the one the row scan would use.
            it->second = getTextIndexTokenizerDescription(index, function_name);
            if (!it->second.empty())
                break;
        }

        return it->second;
    }
};

}

void TextIndexFunctionsTokenizerPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    TextIndexFunctionsTokenizerVisitor visitor(context);
    visitor.visit(query_tree_node);
}

}
