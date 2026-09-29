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
#include <Storages/IStorage.h>
#include <Storages/MergeTree/MergeTreeIndexConditionText.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeIndexJSONSubcolumnHelper.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/StorageDistributed.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <algorithm>
#include <map>

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

/// Null unless the expression reads from exactly one table, because otherwise no index describes it.
Resolution resolveToTableColumns(const QueryTreeNodePtr & expression, const ContextPtr & context, size_t & substitutions)
{
    StoragePtr storage;
    bool failed = false;

    /// Returns null when nothing below `current` was substituted, so an expression over plain table
    /// columns is resolved without copying anything. A substitution is only ever written into a copy.
    auto substitute = [&](const QueryTreeNodePtr & current, auto & self) -> QueryTreeNodePtr
    {
        const auto * column_node = current->as<ColumnNode>();
        if (!column_node)
        {
            QueryTreeNodePtr substituted;
            const auto & children = current->getChildren();

            for (size_t i = 0; i < children.size(); ++i)
            {
                if (!children[i])
                    continue;

                auto replacement = self(children[i], self);
                if (failed)
                    return nullptr;

                if (!replacement)
                    continue;

                if (!substituted)
                    substituted = current->clone();
                substituted->getChildren()[i] = std::move(replacement);
            }

            return substituted;
        }

        const auto source = column_node->getColumnSourceOrNull();
        if (!source)
        {
            failed = true;
            return nullptr;
        }

        /// A lambda parameter is bound inside the expression, which is how an index names it too.
        if (source->as<LambdaArgumentsNode>())
            return nullptr;

        QueryTreeNodePtr what_it_reads;

        if (auto column_storage = getSourceStorage(*source))
        {
            column_storage = getIndexStorage(column_storage, context);

            /// Not nodes: the two sides of a self-join, and two `remote()` calls naming one table, are
            /// distinct nodes over the same indexes.
            if (storage && storage.get() != column_storage.get())
            {
                failed = true;
                return nullptr;
            }
            storage = column_storage;

            /// An ALIAS column stands for an expression, and that is what the index is defined on.
            if (!column_node->hasExpression())
                return nullptr;

            what_it_reads = column_node->getExpression();
        }
        else
        {
            what_it_reads = findProjection(*source, column_node->getColumnName(), context, substitutions);
        }

        if (!what_it_reads || ++substitutions > max_substitutions)
        {
            failed = true;
            return nullptr;
        }

        auto replacement = self(what_it_reads, self);
        if (failed)
            return nullptr;

        return replacement ? replacement : what_it_reads;
    };

    auto substituted = substitute(expression, substitute);
    if (failed || !storage)
        return {};

    return {substituted ? substituted : expression, storage};
}

/// The names one indexed expression can be read through, as `MergeTreeIndexConditionText` accepts them:
/// `m['k']` and `m.key_*` for a `mapValues(m)` index, and a CAST around a JSON subcolumn.
Names carrierNames(const IQueryTreeNode & resolved, const String & resolved_name, const StorageInMemoryMetadata & metadata)
{
    Names names{resolved_name};

    /// Building the shadowing set walks every column, so only ask once the name has the shape.
    if (looksLikeMapSubcolumnName(resolved_name))
    {
        if (auto parsed = tryParseMapSubcolumnName(resolved_name, getColumnsShadowingMapSubcolumns(metadata)))
            names.push_back("mapValues(" + parsed->first + ")");
    }

    const auto * function_node = resolved.as<FunctionNode>();
    if (!function_node)
        return names;

    const auto & arguments = function_node->getArguments().getNodes();
    if (arguments.size() != 2)
        return names;

    const auto & function_name = function_node->getFunctionName();
    const String argument_name = indexExpressionName(*arguments.front());

    if (function_name == "arrayElement")
        names.push_back("mapValues(" + argument_name + ")");
    else if (function_name == "CAST" || function_name == "_CAST")
        names.push_back(argument_name);

    return names;
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

        auto tokenizer = findTextIndexTokenizer(arguments.front());
        if (tokenizer.empty())
            return;

        arguments.push_back(std::make_shared<ConstantNode>(std::move(tokenizer), std::make_shared<DataTypeString>()));
        resolveOrdinaryFunctionNodeByName(*function_node, function_node->getFunctionName(), getContext());
    }

private:
    /// Constructing a tokenizer can load a dictionary, so ask each index at most once.
    std::map<std::pair<const IStorage *, String>, String> tokenizer_cache;

    String findTextIndexTokenizer(const QueryTreeNodePtr & expression)
    {
        size_t substitutions = 0;
        auto [resolved, storage] = resolveToTableColumns(expression, getContext(), substitutions);
        if (!resolved)
            return {};

        const auto metadata = storage->getInMemoryMetadataPtr(getContext(), /*bypass_metadata_cache=*/ false);
        const auto & indices = metadata->getSecondaryIndices();
        if (indices.empty())
            return {};

        auto key = std::make_pair(storage.get(), indexExpressionName(*resolved));

        auto [it, inserted] = tokenizer_cache.try_emplace(key);
        if (!inserted)
            return it->second;

        const Names carriers = carrierNames(*resolved, key.second, *metadata);

        for (const auto & index : indices)
        {
            if (index.type != TEXT_INDEX_NAME || index.column_names.size() != 1)
                continue;

            const auto normalized_name = getNormalizedIndexColumnName(index);
            const bool describes = std::ranges::any_of(carriers, [&](const String & carrier)
            {
                return carrier == index.column_names.front()
                    || normalized_name == std::optional<String>(carrier)
                    || tryMatchJSONSubcolumnToIndex(carrier, index.column_names, "JSONAllValues").has_value();
            });

            if (!describes)
                continue;

            /// Several text indexes on one expression are ambiguous; take the first defined.
            it->second = getTextIndexTokenizerDescription(index);
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
