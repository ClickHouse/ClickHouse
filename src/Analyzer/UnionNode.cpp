#include <Analyzer/UnionNode.h>

#include <Common/SipHash.h>

#include <Core/NamesAndTypes.h>

#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTFunction.h>

#include <Core/ColumnWithTypeAndName.h>

#include <DataTypes/getLeastSupertype.h>

#include <Analyzer/QueryNode.h>
#include <Analyzer/Utils.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TYPE_MISMATCH;
}

UnionNode::UnionNode()
{
    children.resize(children_size);
    children[queries_child_index] = std::make_shared<ListNode>();
}

NamesAndTypesList UnionNode::computeProjectionColumns() const
{
    std::vector<NamesAndTypes> projections;
    const auto & query_nodes = getQueries().getNodes();

    NamesAndTypes query_node_projection;

    for (const auto & query_node : query_nodes)
    {
        if (auto * query_node_typed = query_node->as<QueryNode>())
        {
            auto projection_columns = query_node_typed->computeProjectionColumns();
            query_node_projection = NamesAndTypes(projection_columns.begin(), projection_columns.end());
        }
        else if (auto * union_node_typed = query_node->as<UnionNode>())
        {
            auto projection_columns = union_node_typed->computeProjectionColumns();
            query_node_projection = NamesAndTypes(projection_columns.begin(), projection_columns.end());
        }

        projections.push_back(query_node_projection);

        if (query_node_projection.size() != projections.front().size())
            throw Exception(ErrorCodes::TYPE_MISMATCH, "UNION different number of columns in queries");
    }

    NamesAndTypesList result_columns;

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

String UnionNode::getName() const
{
    WriteBufferFromOwnString buffer;

    auto query_nodes = getQueries().getNodes();
    size_t query_nodes_size = query_nodes.size();

    for (size_t i = 0; i < query_nodes_size; ++i)
    {
        const auto & query_node = query_nodes[i];
        buffer << query_node->getName();

        if (i == 0)
            continue;

        auto query_union_mode = union_modes.at(i - 1);
        if (query_union_mode == SelectUnionMode::ALL || query_union_mode == SelectUnionMode::DISTINCT)
            buffer << " UNION " << toString(query_union_mode);
        else
            buffer << toString(query_union_mode);
    }

    return buffer.str();
}

void UnionNode::dumpTreeImpl(WriteBuffer & buffer, FormatState & format_state, size_t indent) const
{
    buffer << std::string(indent, ' ') << "UNION id: " << format_state.getNodeId(this);

    if (hasAlias())
        buffer << ", alias: " << getAlias();

    buffer << ", is_subquery: " << is_subquery;
    buffer << ", is_cte: " << is_cte;

    if (!cte_name.empty())
        buffer << ", cte_name: " << cte_name;

    buffer << ", union_mode: ";

    if (union_mode == SelectUnionMode::ALL || union_mode == SelectUnionMode::DISTINCT)
        buffer << " UNION " << toString(union_mode);
    else
        buffer << toString(union_mode);

    size_t union_modes_size = union_modes.size();
    buffer << '\n' << std::string(indent + 2, ' ') << "UNION MODES " << union_modes_size << '\n';

    for (size_t i = 0; i < union_modes_size; ++i)
    {
        buffer << std::string(indent + 4, ' ');

        auto query_union_mode = union_modes[i];
        if (query_union_mode == SelectUnionMode::ALL || query_union_mode == SelectUnionMode::DISTINCT)
            buffer << " UNION " << toString(query_union_mode);
        else
            buffer << toString(query_union_mode);

        if (i + 1 != union_modes_size)
            buffer << '\n';
    }

    buffer << '\n' << std::string(indent + 2, ' ') << "QUERIES\n";
    getQueriesNode()->dumpTreeImpl(buffer, format_state, indent + 4);
}

bool UnionNode::isEqualImpl(const IQueryTreeNode & rhs) const
{
    const auto & rhs_typed = assert_cast<const UnionNode &>(rhs);
    return is_subquery == rhs_typed.is_subquery && is_cte == rhs_typed.is_cte && cte_name == rhs_typed.cte_name;
}

void UnionNode::updateTreeHashImpl(HashState & state) const
{
    state.update(is_subquery);
    state.update(is_cte);

    state.update(cte_name.size());
    state.update(cte_name);
}

ASTPtr UnionNode::toASTImpl() const
{
    auto select_with_union_query = std::make_shared<ASTSelectWithUnionQuery>();
    select_with_union_query->union_mode = union_mode;
    select_with_union_query->list_of_modes = union_modes;
    select_with_union_query->set_of_modes = union_modes_set;
    select_with_union_query->children.push_back(getQueriesNode()->toAST());
    select_with_union_query->list_of_selects = select_with_union_query->children.back();

    return select_with_union_query;
}

QueryTreeNodePtr UnionNode::cloneImpl() const
{
    auto result_query_node = std::make_shared<UnionNode>();

    result_query_node->is_subquery = is_subquery;
    result_query_node->is_cte = is_cte;
    result_query_node->cte_name = cte_name;

    result_query_node->union_mode = union_mode;
    result_query_node->union_modes = union_modes;
    result_query_node->union_modes_set = union_modes_set;

    return result_query_node;
}

}
