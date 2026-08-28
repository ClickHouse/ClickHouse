#include <Analyzer/Passes/TextSearchTokenizerPass.h>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/Utils.h>
#include <DataTypes/DataTypeString.h>
#include <Storages/MergeTree/MergeTreeIndexConditionText.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/StorageSnapshot.h>

#include <map>

namespace DB
{

namespace
{

/// A recursive CTE projects columns of its own query, so bound the descent instead of following it.
constexpr size_t max_subquery_depth = 64;

/// The projection a subquery exposes under `name`, when it passes a column through unchanged.
const ColumnNode * findPassThroughProjection(const QueryNode & query_node, const String & name)
{
    const auto & projection = query_node.getProjection().getNodes();
    const auto & projection_columns = query_node.getProjectionColumns();

    for (size_t i = 0; i < projection.size() && i < projection_columns.size(); ++i)
    {
        if (projection_columns[i].name == name)
            return projection[i]->as<ColumnNode>();
    }

    return nullptr;
}

/// The table column `column_node` reads: `x` in `SELECT ... FROM (SELECT s AS x FROM t)` resolves to `s`
/// of `t`. Null when the chain leaves a single table, which includes a projection that computes a new
/// value under the indexed column's name.
const ColumnNode * resolveToTableColumn(const ColumnNode & column_node)
{
    const ColumnNode * current = &column_node;

    for (size_t depth = 0; depth < max_subquery_depth; ++depth)
    {
        const auto source = current->getColumnSourceOrNull();
        if (!source)
            return nullptr;

        if (source->as<TableNode>())
            return current;

        const auto * query_node = source->as<QueryNode>();
        if (!query_node)
            return nullptr;

        current = findPassThroughProjection(*query_node, current->getColumnName());
        if (!current)
            return nullptr;
    }

    return nullptr;
}

/// A copy of `expression` with every column replaced by the table column it reads, so that its AST column
/// name can be compared with an index definition, and the table those columns come from. Null when the
/// expression does not read from exactly one table, because then no index describes it.
std::pair<QueryTreeNodePtr, const TableNode *> resolveToTableColumns(const QueryTreeNodePtr & expression)
{
    auto resolved = expression->clone();
    const TableNode * table = nullptr;
    bool failed = false;

    auto visit = [&](QueryTreeNodePtr & current, auto & self) -> void
    {
        if (failed)
            return;

        if (const auto * column_node = current->as<ColumnNode>())
        {
            const auto * table_column = resolveToTableColumn(*column_node);
            const auto * column_table = table_column ? table_column->getColumnSourceOrNull()->as<TableNode>() : nullptr;

            if (!column_table || (table && table != column_table))
            {
                failed = true;
                return;
            }

            table = column_table;
            current = table_column->clone();
            return;
        }

        for (auto & child : current->getChildren())
        {
            if (child)
                self(child, self);
        }
    };
    visit(resolved, visit);

    if (failed || !table)
        return {};

    return {resolved, table};
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
    std::map<std::pair<const TableNode *, String>, String> tokenizer_cache;

    /// The tokenizer of the text index defined on `expression`, empty when there is none.
    String findTextIndexTokenizer(const QueryTreeNodePtr & expression)
    {
        auto [resolved, table_node] = resolveToTableColumns(expression);
        if (!resolved)
            return {};

        const auto & indices = table_node->getStorageSnapshot()->metadata->getSecondaryIndices();
        if (indices.empty())
            return {};

        /// `IndexDescription::column_names` are the AST column names of the index expression, so the same
        /// serialization matches an expression index (`lower(s)`, `mapValues(m)`) as well as a plain column.
        ConvertToASTOptions ast_options;
        ast_options.add_cast_for_constants = false;
        ast_options.fully_qualified_identifiers = false;
        auto key = std::make_pair(table_node, resolved->toAST(ast_options)->getColumnName());

        auto [it, inserted] = tokenizer_cache.try_emplace(key);
        if (!inserted)
            return it->second;

        for (const auto & index : indices)
        {
            if (index.column_names.size() != 1 || index.column_names.front() != key.second)
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
