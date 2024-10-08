#include <Analyzer/UnionNode.h>

#include <Common/SipHash.h>
#include <Common/FieldVisitorToString.h>

#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTWithElement.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTFunction.h>

#include <Core/ColumnWithTypeAndName.h>
#include <Core/NamesAndTypes.h>

#include <DataTypes/getLeastSupertype.h>

#include <Storages/IStorage.h>

#include <Interpreters/Context.h>

#include <Analyzer/QueryNode.h>
#include <Analyzer/Utils.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TYPE_MISMATCH;
    extern const int BAD_ARGUMENTS;
}

UnionNode::UnionNode(ContextMutablePtr context_, SelectUnionMode union_mode_)
    : IQueryTreeNode(children_size)
    , context(std::move(context_))
    , union_mode(union_mode_)
{
    if (union_mode == SelectUnionMode::UNION_DEFAULT ||
        union_mode == SelectUnionMode::EXCEPT_DEFAULT ||
        union_mode == SelectUnionMode::INTERSECT_DEFAULT)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "UNION mode {} must be normalized", toString(union_mode));

    children[queries_child_index] = std::make_shared<ListNode>();
}

NamesAndTypes UnionNode::computeProjectionColumns() const
{
    if (recursive_cte_table)
        return recursive_cte_table->columns;

    std::vector<NamesAndTypes> projections;

    NamesAndTypes query_node_projection;

    const auto & query_nodes = getQueries().getNodes();
    projections.reserve(query_nodes.size());

    for (const auto & query_node : query_nodes)
    {
        if (auto * query_node_typed = query_node->as<QueryNode>())
            query_node_projection = query_node_typed->getProjectionColumns();
        else if (auto * union_node_typed = query_node->as<UnionNode>())
            query_node_projection = union_node_typed->computeProjectionColumns();

        projections.push_back(query_node_projection);

        if (query_node_projection.size() != projections.front().size())
            throw Exception(ErrorCodes::TYPE_MISMATCH, "UNION different number of columns in queries");
    }

    NamesAndTypes result_columns;

    size_t projections_size = projections.size();
    DataTypes projection_column_types;
    projection_column_types.resize(projections_size);

    size_t columns_size = query_node_projection.size();
    for (size_t column_index = 0; column_index < columns_size; ++column_index)
    {
        for (size_t projection_index = 0; projection_index < projections_size; ++projection_index)
            projection_column_types[projection_index] = projections[projection_index][column_index].type;

        auto result_type = getLeastSupertype(projection_column_types);
        result_columns.emplace_back(projections.front()[column_index].name, std::move(result_type));
    }

    return result_columns;
}

void UnionNode::removeUnusedProjectionColumns(const std::unordered_set<std::string> & used_projection_columns)
{
    if (recursive_cte_table)
        return;

    auto projection_columns = computeProjectionColumns();
    size_t projection_columns_size = projection_columns.size();
    std::unordered_set<size_t> used_projection_column_indexes;

    for (size_t i = 0; i < projection_columns_size; ++i)
    {
        const auto & projection_column = projection_columns[i];
        if (used_projection_columns.contains(projection_column.name))
            used_projection_column_indexes.insert(i);
    }

    auto & query_nodes = getQueries().getNodes();
    for (auto & query_node : query_nodes)
    {
        if (auto * query_node_typed = query_node->as<QueryNode>())
            query_node_typed->removeUnusedProjectionColumns(used_projection_column_indexes);
        else if (auto * union_node_typed = query_node->as<UnionNode>())
            union_node_typed->removeUnusedProjectionColumns(used_projection_column_indexes);
    }
}

void UnionNode::removeUnusedProjectionColumns(const std::unordered_set<size_t> & used_projection_columns_indexes)
{
    if (recursive_cte_table)
        return;

    auto & query_nodes = getQueries().getNodes();
    for (auto & query_node : query_nodes)
    {
        if (auto * query_node_typed = query_node->as<QueryNode>())
            query_node_typed->removeUnusedProjectionColumns(used_projection_columns_indexes);
        else if (auto * union_node_typed = query_node->as<UnionNode>())
            union_node_typed->removeUnusedProjectionColumns(used_projection_columns_indexes);
    }
}

void UnionNode::dumpTreeImpl(WriteBuffer & buffer, FormatState & format_state, size_t indent) const
{
    buffer << std::string(indent, ' ') << "UNION id: " << format_state.getNodeId(this);

    if (hasAlias())
        buffer << ", alias: " << getAlias();

    if (is_subquery)
        buffer << ", is_subquery: " << is_subquery;

    if (is_cte)
        buffer << ", is_cte: " << is_cte;

    if (is_recursive_cte)
        buffer << ", is_recursive_cte: " << is_recursive_cte;

    if (recursive_cte_table)
        buffer << ", recursive_cte_table: " << recursive_cte_table->storage->getStorageID().getNameForLogs();

    if (!cte_name.empty())
        buffer << ", cte_name: " << cte_name;

    buffer << ", union_mode: " << toString(union_mode);

    buffer << '\n' << std::string(indent + 2, ' ') << "QUERIES\n";
    getQueriesNode()->dumpTreeImpl(buffer, format_state, indent + 4);
}

bool UnionNode::isEqualImpl(const IQueryTreeNode & rhs, CompareOptions) const
{
    const auto & rhs_typed = assert_cast<const UnionNode &>(rhs);

    if (recursive_cte_table && rhs_typed.recursive_cte_table &&
        recursive_cte_table->getStorageID() != rhs_typed.recursive_cte_table->getStorageID())
        return false;
    if ((recursive_cte_table && !rhs_typed.recursive_cte_table) || (!recursive_cte_table && rhs_typed.recursive_cte_table))
        return false;

    return is_subquery == rhs_typed.is_subquery && is_cte == rhs_typed.is_cte && is_recursive_cte == rhs_typed.is_recursive_cte
        && cte_name == rhs_typed.cte_name && union_mode == rhs_typed.union_mode;
}

void UnionNode::updateTreeHashImpl(HashState & state, CompareOptions) const
{
    state.update(is_subquery);
    state.update(is_cte);
    state.update(is_recursive_cte);

    if (recursive_cte_table)
    {
        auto full_name = recursive_cte_table->getStorageID().getFullNameNotQuoted();
        state.update(full_name.size());
        state.update(full_name);
    }

    state.update(cte_name.size());
    state.update(cte_name);

    state.update(static_cast<size_t>(union_mode));
}

QueryTreeNodePtr UnionNode::cloneImpl() const
{
    auto result_union_node = std::make_shared<UnionNode>(context, union_mode);

    result_union_node->is_subquery = is_subquery;
    result_union_node->is_cte = is_cte;
    result_union_node->is_recursive_cte = is_recursive_cte;
    result_union_node->recursive_cte_table = recursive_cte_table;
    result_union_node->cte_name = cte_name;

    return result_union_node;
}

ASTPtr UnionNode::toASTImpl(const ConvertToASTOptions & options) const
{
    auto select_with_union_query = std::make_shared<ASTSelectWithUnionQuery>();
    select_with_union_query->union_mode = union_mode;
    select_with_union_query->is_normalized = true;
    select_with_union_query->children.push_back(getQueriesNode()->toAST(options));
    select_with_union_query->list_of_selects = select_with_union_query->children.back();

    ASTPtr result_query = std::move(select_with_union_query);
    bool set_subquery_cte_name = true;

    if (recursive_cte_table)
    {
        auto recursive_select_query = std::make_shared<ASTSelectQuery>();
        recursive_select_query->recursive_with = true;

        auto with_element_ast = std::make_shared<ASTWithElement>();
        with_element_ast->name = cte_name;
        with_element_ast->subquery = std::make_shared<ASTSubquery>(std::move(result_query));
        with_element_ast->children.push_back(with_element_ast->subquery);

        auto with_expression_list_ast = std::make_shared<ASTExpressionList>();
        with_expression_list_ast->children.push_back(std::move(with_element_ast));

        recursive_select_query->setExpression(ASTSelectQuery::Expression::WITH, std::move(with_expression_list_ast));

        auto select_expression_list_ast = std::make_shared<ASTExpressionList>();
        select_expression_list_ast->children.reserve(recursive_cte_table->columns.size());
        for (const auto & recursive_cte_table_column : recursive_cte_table->columns)
            select_expression_list_ast->children.push_back(std::make_shared<ASTIdentifier>(recursive_cte_table_column.name));

        recursive_select_query->setExpression(ASTSelectQuery::Expression::SELECT, std::move(select_expression_list_ast));

        auto table_expression_ast = std::make_shared<ASTTableExpression>();
        table_expression_ast->children.push_back(std::make_shared<ASTTableIdentifier>(cte_name));
        table_expression_ast->database_and_table_name = table_expression_ast->children.back();

        auto tables_in_select_query_element_ast = std::make_shared<ASTTablesInSelectQueryElement>();
        tables_in_select_query_element_ast->children.push_back(std::move(table_expression_ast));
        tables_in_select_query_element_ast->table_expression = tables_in_select_query_element_ast->children.back();

        ASTPtr tables_in_select_query_ast = std::make_shared<ASTTablesInSelectQuery>();
        tables_in_select_query_ast->children.push_back(std::move(tables_in_select_query_element_ast));

        recursive_select_query->setExpression(ASTSelectQuery::Expression::TABLES, std::move(tables_in_select_query_ast));

        auto recursive_select_with_union_query = std::make_shared<ASTSelectWithUnionQuery>();
        auto recursive_select_with_union_query_list_of_selects = std::make_shared<ASTExpressionList>();
        recursive_select_with_union_query_list_of_selects->children.push_back(std::move(recursive_select_query));
        recursive_select_with_union_query->children.push_back(std::move(recursive_select_with_union_query_list_of_selects));
        recursive_select_with_union_query->list_of_selects = recursive_select_with_union_query->children.back();

        result_query = std::move(recursive_select_with_union_query);
        set_subquery_cte_name = false;
    }

    if (is_subquery)
    {
        auto subquery = std::make_shared<ASTSubquery>(std::move(result_query));
        if (set_subquery_cte_name)
            subquery->cte_name = cte_name;

        result_query = std::move(subquery);
    }

    return result_query;
}

}
