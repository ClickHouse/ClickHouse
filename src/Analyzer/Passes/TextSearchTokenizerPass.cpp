#include <Analyzer/Passes/TextSearchTokenizerPass.h>

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
#include <Interpreters/DatabaseCatalog.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/MergeTreeIndexConditionText.h>
#include <Storages/MergeTree/MergeTreeIndexJSONSubcolumnHelper.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/StorageDistributed.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/StorageSnapshot.h>

#include <algorithm>
#include <map>

namespace DB
{

namespace
{

/// A recursive CTE projects columns of its own query, so bound the descent instead of following it.
constexpr size_t max_substitutions = 64;

struct Resolution
{
    /// `expression` with every column substituted by what it reads, so its AST column name can be
    /// compared with an index definition, and the table those columns come from.
    QueryTreeNodePtr expression;
    StoragePtr storage;
};

/// A table function reads a table too: `remote()` is a `Distributed` storage, whose indexes are reachable.
StoragePtr getSourceStorage(const IQueryTreeNode & source)
{
    if (const auto * table_node = source.as<TableNode>())
        return table_node->getStorage();

    if (const auto * table_function_node = source.as<TableFunctionNode>())
        return table_function_node->getStorage();

    return nullptr;
}

/// The metadata carrying the index definitions of `storage`. A `Distributed` table has none of its own:
/// the indexes live on the shards' local table, which the initiator can reach only by name. Resolving it
/// there is what makes a predicate the initiator evaluates itself agree with the shard-local scans.
StorageMetadataHandle getIndexMetadata(const StoragePtr & storage, const ContextPtr & context)
{
    auto metadata = storage->getInMemoryMetadataPtr(context, /*bypass_metadata_cache=*/ false);
    if (!metadata->getSecondaryIndices().empty())
        return metadata;

    const auto * distributed = typeid_cast<const StorageDistributed *>(storage.get());
    if (!distributed)
        return metadata;

    const auto remote_database = distributed->getRemoteDatabaseName();
    const auto remote_table = distributed->getRemoteTableName();
    if (remote_database.empty() || remote_table.empty())
        return metadata;

    auto local_id = context->tryResolveStorageID(StorageID{remote_database, remote_table});
    if (!local_id)
        return metadata;

    auto local_table = DatabaseCatalog::instance().tryGetTable(local_id, context);
    if (!local_table || local_table.get() == storage.get())
        return metadata;

    return local_table->getInMemoryMetadataPtr(context, /*bypass_metadata_cache=*/ false);
}

Resolution resolveToTableColumns(const QueryTreeNodePtr & expression, size_t & substitutions);

/// The index-expression name of a resolved expression, spelled the way `IndexDescription::column_names` is.
String indexExpressionName(const IQueryTreeNode & resolved)
{
    ConvertToASTOptions ast_options;
    ast_options.add_cast_for_constants = false;
    ast_options.fully_qualified_identifiers = false;
    return resolved.toAST(ast_options)->getColumnName();
}

/// The expression a query exposes at `position` of its projection.
const IQueryTreeNode * projectionAt(const IQueryTreeNode & source, size_t position);

/// The position `name` occupies in a table expression's output.
std::optional<size_t> projectionPosition(const NamesAndTypes & projection_columns, const String & name)
{
    for (size_t i = 0; i < projection_columns.size(); ++i)
    {
        if (projection_columns[i].name == name)
            return i;
    }

    return {};
}

/// A row comes from exactly one branch of a union, so every branch must expose the same indexed
/// expression of the same table; otherwise the tokenizer would depend on which branch produced the row.
const IQueryTreeNode * findAgreedUnionProjection(const UnionNode & union_node, size_t position, size_t & substitutions)
{
    const IQueryTreeNode * agreed = nullptr;
    String agreed_name;
    const IStorage * agreed_storage = nullptr;

    for (const auto & branch : union_node.getQueries().getNodes())
    {
        const auto * projection = projectionAt(*branch, position);
        if (!projection)
            return nullptr;

        auto resolution = resolveToTableColumns(projection->clone(), substitutions);
        if (!resolution.expression)
            return nullptr;

        const auto name = indexExpressionName(*resolution.expression);
        const auto * storage = resolution.storage.get();

        if (!agreed)
        {
            agreed = projection;
            agreed_name = name;
            agreed_storage = storage;
        }
        else if (name != agreed_name || storage != agreed_storage)
        {
            return nullptr;
        }
    }

    return agreed;
}

const IQueryTreeNode * projectionAt(const IQueryTreeNode & source, size_t position)
{
    if (const auto * query_node = source.as<QueryNode>())
    {
        const auto & projection = query_node->getProjection().getNodes();
        return position < projection.size() ? projection[position].get() : nullptr;
    }

    if (const auto * union_node = source.as<UnionNode>())
    {
        size_t substitutions = 0;
        return findAgreedUnionProjection(*union_node, position, substitutions);
    }

    return nullptr;
}

/// The expression a subquery exposes under `name`. Not only a passed-through column: `x` may be any
/// expression, and `SELECT lower(s) AS x` is still the indexed `lower(s)` seen from outside.
const IQueryTreeNode * findProjection(const IQueryTreeNode & source, const String & name, size_t & substitutions)
{
    if (const auto * query_node = source.as<QueryNode>())
    {
        auto position = projectionPosition(query_node->getProjectionColumns(), name);
        return position ? projectionAt(source, *position) : nullptr;
    }

    if (const auto * union_node = source.as<UnionNode>())
    {
        auto position = projectionPosition(union_node->computeProjectionColumns(), name);
        return position ? findAgreedUnionProjection(*union_node, *position, substitutions) : nullptr;
    }

    return nullptr;
}

/// Null when the expression does not read from exactly one table, because then no index describes it:
/// `concat(s, ' zzz') AS s` resolves to that expression, which no index is defined on.
Resolution resolveToTableColumns(const QueryTreeNodePtr & expression, size_t & substitutions)
{
    auto resolved = expression->clone();
    StoragePtr storage;
    bool failed = false;

    auto visit = [&](QueryTreeNodePtr & current, auto & self) -> void
    {
        if (failed)
            return;

        const auto * column_node = current->as<ColumnNode>();
        if (!column_node)
        {
            for (auto & child : current->getChildren())
            {
                if (child)
                    self(child, self);
            }
            return;
        }

        const auto source = column_node->getColumnSourceOrNull();
        if (!source)
        {
            failed = true;
            return;
        }

        /// A lambda parameter is bound inside the expression and an index expression names it the same
        /// way, so it is already resolved.
        if (source->as<LambdaArgumentsNode>())
            return;

        if (auto column_storage = getSourceStorage(*source))
        {
            /// Compare storages, not nodes: the two scans of `t UNION ALL t`, and the two sides of a
            /// self-join, are distinct table nodes carrying the same indexes.
            if (storage && storage.get() != column_storage.get())
            {
                failed = true;
                return;
            }
            storage = column_storage;

            /// An ALIAS column stands for an expression, and that is what the index is defined on.
            if (!column_node->hasExpression())
                return;

            if (++substitutions > max_substitutions)
            {
                failed = true;
                return;
            }

            current = column_node->getExpression()->clone();
            self(current, self);
            return;
        }

        const auto * projection = findProjection(*source, column_node->getColumnName(), substitutions);
        if (!projection || ++substitutions > max_substitutions)
        {
            failed = true;
            return;
        }

        current = projection->clone();
        self(current, self);
    };
    visit(resolved, visit);

    if (failed || !storage)
        return {};

    return {resolved, storage};
}

/// The names one indexed expression can be read through, the carriers MergeTreeIndexConditionText also
/// accepts: `m['k']` and the `m.key_*` subcolumn for a `mapValues(m)` index, and a CAST around a JSON
/// subcolumn (`j.k::String`).
Names carrierNames(const IQueryTreeNode & resolved, const String & resolved_name)
{
    Names names{resolved_name};

    if (auto parsed = tryParseMapSubcolumnName(resolved_name))
        names.push_back("mapValues(" + parsed->first + ")");

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

class TextSearchTokenizerVisitor : public InDepthQueryTreeVisitorWithContext<TextSearchTokenizerVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<TextSearchTokenizerVisitor>;
    using Base::Base;

    void enterImpl(QueryTreeNodePtr & node)
    {
        auto * function_node = node->as<FunctionNode>();
        if (!function_node || !function_node->isOrdinaryFunction()
            || !textSearchFunctionAcceptsTokenizer(function_node->getFunctionName()))
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

    /// The tokenizer of the text index defined on `expression`, empty when there is none.
    String findTextIndexTokenizer(const QueryTreeNodePtr & expression)
    {
        size_t substitutions = 0;
        auto [resolved, storage] = resolveToTableColumns(expression, substitutions);
        if (!resolved)
            return {};

        const auto metadata = getIndexMetadata(storage, getContext());
        const auto & indices = metadata->getSecondaryIndices();
        if (indices.empty())
            return {};

        /// `IndexDescription::column_names` are the AST column names of the index expression, so the same
        /// serialization matches an expression index (`lower(s)`, `mapValues(m)`) as well as a plain column.
        auto key = std::make_pair(storage.get(), indexExpressionName(*resolved));

        auto [it, inserted] = tokenizer_cache.try_emplace(key);
        if (!inserted)
            return it->second;

        /// Otherwise the row scan would tokenize a carrier differently from the index describing it.
        const Names carriers = carrierNames(*resolved, key.second);

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

            /// Several text indexes on one expression are ambiguous; take the first one, in definition order.
            it->second = getTextIndexTokenizerDescription(index);
            if (!it->second.empty())
                break;
        }

        return it->second;
    }
};

}

void TextSearchTokenizerPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    TextSearchTokenizerVisitor visitor(context);
    visitor.visit(query_tree_node);
}

}
