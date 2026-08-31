#include <Analyzer/Passes/TextSearchTokenizerPass.h>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/Utils.h>
#include <DataTypes/DataTypeMapHelpers.h>
#include <DataTypes/DataTypeString.h>
#include <Storages/MergeTree/MergeTreeIndexConditionText.h>
#include <Storages/MergeTree/MergeTreeIndexJSONSubcolumnHelper.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
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

/// The expression a subquery exposes under `name`. Not only a passed-through column: `x` may be any
/// expression, and `SELECT lower(s) AS x` is still the indexed `lower(s)` seen from outside.
const IQueryTreeNode * findProjection(const QueryNode & query_node, const String & name)
{
    const auto & projection = query_node.getProjection().getNodes();
    const auto & projection_columns = query_node.getProjectionColumns();

    for (size_t i = 0; i < projection.size() && i < projection_columns.size(); ++i)
    {
        if (projection_columns[i].name == name)
            return projection[i].get();
    }

    return nullptr;
}

/// A copy of `expression` with every column substituted by what it reads, down through subquery
/// projections, so that its AST column name can be compared with an index definition, and the table those
/// columns come from. Null when the expression does not read from exactly one table, because then no index
/// describes it: `concat(s, ' zzz') AS s` resolves to that expression, which no index is defined on.
std::pair<QueryTreeNodePtr, const TableNode *> resolveToTableColumns(const QueryTreeNodePtr & expression)
{
    auto resolved = expression->clone();
    const TableNode * table = nullptr;
    size_t substitutions = 0;
    bool failed = false;

    auto visit = [&](QueryTreeNodePtr & current, auto & self) -> void
    {
        if (failed)
            return;

        if (const auto * column_node = current->as<ColumnNode>())
        {
            const auto source = column_node->getColumnSourceOrNull();
            const auto * column_table = source ? source->as<TableNode>() : nullptr;

            if (column_table)
            {
                if (table && table != column_table)
                    failed = true;
                else
                    table = column_table;
                return;
            }

            const auto * query_node = source ? source->as<QueryNode>() : nullptr;
            const auto * projection = query_node ? findProjection(*query_node, column_node->getColumnName()) : nullptr;

            if (!projection || ++substitutions > max_substitutions)
            {
                failed = true;
                return;
            }

            current = projection->clone();
            self(current, self);
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

/// The names one indexed expression can be read through, the carriers MergeTreeIndexConditionText also
/// accepts: `m['k']` and the `m.key_*` subcolumn for a `mapValues(m)` index, and a CAST around a JSON
/// subcolumn (`j.k::String`).
Names carrierNames(const IQueryTreeNode & resolved, const String & resolved_name, const ConvertToASTOptions & ast_options)
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
    const String argument_name = arguments.front()->toAST(ast_options)->getColumnName();

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

        /// Otherwise the row scan would tokenize a carrier differently from the index describing it.
        const Names carriers = carrierNames(*resolved, key.second, ast_options);

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
