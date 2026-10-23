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
#include <DataTypes/DataTypeString.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/ExpressionActions.h>
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

/// A recursive CTE projects columns of its own query, so bound the descent.
constexpr size_t max_substitutions = 64;

struct Resolution
{
    /// Every column substituted by what it reads, so the AST column name can be compared with an index.
    QueryTreeNodePtr expression;
    StoragePtr storage;
    /// A `m[k]` lookup whose key reads another table, so only the Map describes the expression.
    bool foreign_map_key = false;
};

/// A table function reads a table too: `remote()` resolves to a `Distributed` storage.
StoragePtr getSourceStorage(const IQueryTreeNode & source)
{
    if (const auto * table_node = source.as<TableNode>())
        return table_node->getStorage();

    if (const auto * table_function_node = source.as<TableFunctionNode>())
        return table_function_node->getStorage();

    return nullptr;
}

/// The table carrying the index definitions, for a `Distributed` the shards' local one. Only a node that
/// is itself a shard can name that table; on any other initiator the definitions are out of reach without
/// a round trip, so `storage` is returned and the tokenizer stays unresolved rather than being guessed
/// from an unrelated table of the same name.
StoragePtr getIndexStorage(const StoragePtr & storage, const ContextPtr & context)
{
    const auto * distributed = typeid_cast<const StorageDistributed *>(storage.get());
    if (!distributed)
        return storage;

    /// Resolving the name is not enough: an initiator outside the cluster can hold an unrelated table of
    /// that name. Being a shard is the proof, and it is the test that decides where the data is read.
    const auto cluster = distributed->getCluster();
    if (!cluster || cluster->getLocalShardCount() == 0)
        return storage;

    /// Empty when the remote side is a table function (`remote('host', view(...))`).
    const auto remote_database = distributed->getRemoteDatabaseName();
    const auto remote_table = distributed->getRemoteTableName();
    if (remote_database.empty() || remote_table.empty())
        return storage;

    auto local_id = context->tryResolveStorageID(StorageID{remote_database, remote_table});
    if (!local_id)
        return storage;

    auto local_table = DatabaseCatalog::instance().tryGetTable(local_id, context);
    if (!local_table || local_table.get() == storage.get())
        return storage;

    return local_table;
}

/// The index-expression name of a resolved expression, spelled the way `IndexDescription::column_names` is.
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
Resolution resolveToTableColumns(const QueryTreeNodePtr & expression, const ContextPtr & context, size_t & substitutions);

/// A row comes from exactly one branch, so every branch must expose the same indexed expression of the
/// same table; otherwise the tokenizer would depend on which branch produced the row.
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

        auto resolution = resolveToTableColumns(projection, context, substitutions);
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

/// Not only a passed-through column: `SELECT lower(s) AS x` exposes the indexed `lower(s)` as `x`.
QueryTreeNodePtr findProjection(const IQueryTreeNode & source, const String & name, const ContextPtr & context, size_t & substitutions)
{
    if (const auto * query_node = source.as<QueryNode>())
    {
        auto position = projectionPosition(query_node->getProjectionColumns(), name);
        return position ? projectionAt(source, *position, context, substitutions) : QueryTreeNodePtr{};
    }

    if (const auto * union_node = source.as<UnionNode>())
    {
        auto position = projectionPosition(union_node->computeProjectionColumns(), name);
        return position ? findAgreedUnionProjection(*union_node, *position, context, substitutions) : QueryTreeNodePtr{};
    }

    return nullptr;
}

/// Null unless the expression reads one table occurrence, because otherwise no index describes it.
Resolution resolveToTableColumns(const QueryTreeNodePtr & expression, const ContextPtr & context, size_t & substitutions)
{
    StoragePtr storage;
    /// Not the storage: a self-join shares it, and `concat(a.first_name, b.last_name)` serializes like an
    /// index on `concat(first_name, last_name)` while pairing values from two rows.
    const IQueryTreeNode * occurrence = nullptr;

    /// Returns `{substituted, failed}`. `substituted` is null when nothing below `current` changed, so
    /// an expression over plain table columns is resolved without copying anything; a substitution is
    /// only ever written into a copy.
    bool foreign_map_key = false;

    auto substitute = [&](const QueryTreeNodePtr & current, auto & self) -> std::pair<QueryTreeNodePtr, bool>
    {
        const auto * column_node = current->as<ColumnNode>();
        if (!column_node)
        {
            /// `m[k]` reads a value `mapValues(m)` indexes whatever the key is, so a key reading another
            /// table leaves the Map resolvable. Its own name then describes nothing, hence the flag.
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

        /// A lambda parameter is bound inside the expression, which is how an index names it too.
        if (source->as<LambdaArgumentsNode>())
            return {nullptr, false};

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

            /// An ALIAS column stands for an expression, and that is what the index is defined on.
            if (!column_node->hasExpression())
                return {nullptr, false};

            what_it_reads = column_node->getExpression();
        }
        else
        {
            what_it_reads = findProjection(*source, column_node->getColumnName(), context, substitutions);
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

/// The operand of a conversion that cannot change a value, by the rule the scan matches an index column
/// under; null when `node` is not such a conversion.
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

/// The names one indexed expression can be read through, as `MergeTreeIndexConditionText` accepts them.
struct Carriers
{
    /// Names an index can be defined on.
    Names names;
    /// Tried as a JSON path only: reading one casts it, and that conversion is not lossless.
    String json_subcolumn;
};

Carriers carrierNames(const IQueryTreeNode & resolved, const String & resolved_name, const StorageInMemoryMetadata & metadata)
{
    Carriers carriers{.names = {resolved_name}, .json_subcolumn = {}};

    /// Building the shadowing set walks every column, so only ask once the name has the shape.
    if (looksLikeMapSubcolumnName(resolved_name))
    {
        if (auto parsed = tryParseMapSubcolumnName(resolved_name, getColumnsShadowingMapSubcolumns(metadata)))
            carriers.names.push_back("mapValues(" + parsed->first + ")");
    }

    const size_t names_before_conversions = carriers.names.size();

    const auto * function_node = resolved.as<FunctionNode>();
    if (!function_node || function_node->getArguments().getNodes().empty())
        return carriers;

    const auto & function_name = function_node->getFunctionName();
    const auto & arguments = function_node->getArguments().getNodes();

    if (function_name == "arrayElement" && arguments.size() == 2)
    {
        carriers.names.push_back("mapValues(" + indexExpressionName(*arguments.front()) + ")");
        return carriers;
    }

    /// Peeled to the bottom, not one layer: the scan matches an index column under any number of them.
    const IQueryTreeNode * node = &resolved;
    while (const auto * under = losslessConversionArgument(*node))
    {
        carriers.names.push_back(indexExpressionName(*under));
        node = under;
    }

    /// A lossy `CAST` reads an expression no index describes, unless it is how a JSON path is read.
    if (carriers.names.size() == names_before_conversions && (function_name == "CAST" || function_name == "_CAST")
        && arguments.size() == 2)
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
        auto * function_node = node->as<FunctionNode>();
        if (!function_node || !function_node->isOrdinaryFunction()
            || !doesTextSearchFunctionAcceptTokenizer(function_node->getFunctionName()))
            return;

        /// Two arguments means no explicit tokenizer, which is the only case the index decides.
        auto & arguments = function_node->getArguments().getNodes();
        if (arguments.size() != 2)
            return;

        auto tokenizer = findTextIndexTokenizer(arguments.front(), function_node->getFunctionName());
        if (tokenizer.empty())
            return;

        arguments.push_back(std::make_shared<ConstantNode>(std::move(tokenizer), std::make_shared<DataTypeString>()));
        resolveOrdinaryFunctionNodeByName(*function_node, function_node->getFunctionName(), getContext());
    }

private:
    /// Constructing a tokenizer can load a dictionary, so ask each index at most once per function name.
    /// The function is part of the key because whether a tokenizer may be forwarded depends on it.
    std::map<std::tuple<const IStorage *, String, String>, String> tokenizer_cache;

    String findTextIndexTokenizer(const QueryTreeNodePtr & expression, const String & function_name)
    {
        size_t substitutions = 0;
        const auto resolution = resolveToTableColumns(expression, getContext(), substitutions);
        if (!resolution.expression)
            return {};

        const auto & resolved = *resolution.expression;
        const auto & storage = resolution.storage;

        const auto metadata = storage->getInMemoryMetadataPtr(getContext(), /*bypass_metadata_cache=*/ false);
        const auto & indices = metadata->getSecondaryIndices();
        if (indices.empty())
            return {};

        /// A key reading another table leaves the expression name describing nothing, so the Map alone
        /// carries the index - reached through whatever conversions wrap the lookup.
        String map_only_name;
        if (resolution.foreign_map_key)
        {
            const IQueryTreeNode * node = &resolved;
            while (const auto * under = losslessConversionArgument(*node))
                node = under;

            const auto * element = node->as<FunctionNode>();
            if (!element || element->getFunctionName() != "arrayElement" || element->getArguments().getNodes().size() != 2)
                return {};

            map_only_name = "mapValues(" + indexExpressionName(*element->getArguments().getNodes().front()) + ")";
        }

        const auto name = map_only_name.empty() ? indexExpressionName(resolved) : map_only_name;
        auto key = std::make_tuple(storage.get(), name, function_name);

        auto [it, inserted] = tokenizer_cache.try_emplace(key);
        if (!inserted)
            return it->second;

        const Carriers carriers = map_only_name.empty() ? carrierNames(resolved, name, *metadata)
                                                        : Carriers{.names = {name}, .json_subcolumn = {}};

        for (const auto & index : indices)
        {
            if (index.type != TEXT_INDEX_NAME || index.column_names.size() != 1)
                continue;

            const auto normalized_name = getNormalizedIndexColumnName(index);
            /// The same argument types the index condition matches with, so a subcolumn inside a declared
            /// typed path is rejected here too instead of being given a tokenizer the index cannot answer.
            const auto json_argument_types = collectJSONIndexArgumentTypes(*index.expression);

            auto matches_json = [&](const String & carrier)
            {
                return tryMatchJSONSubcolumnToIndex(carrier, index.column_names, "JSONAllValues", json_argument_types).has_value();
            };

            const bool describes = std::ranges::any_of(carriers.names, [&](const String & carrier)
            {
                return carrier == index.column_names.front()
                    || normalized_name == std::optional<String>(carrier)
                    || matches_json(carrier);
            }) || (!carriers.json_subcolumn.empty() && matches_json(carriers.json_subcolumn));

            if (!describes)
                continue;

            /// Several text indexes on one expression are ambiguous; take the first defined whose
            /// tokenizer the function accepts, which is also the one the row scan would use.
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
