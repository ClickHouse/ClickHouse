#include <Analyzer/UnionNode.h>

#include <Common/SipHash.h>
#include <Common/FieldVisitorToString.h>

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
#include <Core/NamesAndTypes.h>

#include <DataTypes/getLeastSupertype.h>

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

void UnionNode::dumpTreeImpl(WriteBuffer & buffer, FormatState & format_state, size_t indent) const
{
    buffer << std::string(indent, ' ') << "UNION id: " << format_state.getNodeId(this);

    if (hasAlias())
        buffer << ", alias: " << getAlias();

    if (is_subquery)
        buffer << ", is_subquery: " << is_subquery;

    if (is_cte)
        buffer << ", is_cte: " << is_cte;

    if (!cte_name.empty())
        buffer << ", cte_name: " << cte_name;

    buffer << ", union_mode: " << toString(union_mode);

    buffer << '\n' << std::string(indent + 2, ' ') << "QUERIES\n";
    getQueriesNode()->dumpTreeImpl(buffer, format_state, indent + 4);
}

bool UnionNode::isEqualImpl(const IQueryTreeNode & rhs) const
{
    const auto & rhs_typed = assert_cast<const UnionNode &>(rhs);

    return is_subquery == rhs_typed.is_subquery && is_cte == rhs_typed.is_cte && cte_name == rhs_typed.cte_name &&
        union_mode == rhs_typed.union_mode;
}

void UnionNode::updateTreeHashImpl(HashState & state) const
{
    state.update(is_subquery);
    state.update(is_cte);

    state.update(cte_name.size());
    state.update(cte_name);

    state.update(static_cast<size_t>(union_mode));
}

QueryTreeNodePtr UnionNode::cloneImpl() const
{
    auto result_union_node = std::make_shared<UnionNode>(context, union_mode);

    result_union_node->is_subquery = is_subquery;
    result_union_node->is_cte = is_cte;
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

    if (is_subquery)
    {
        auto subquery = std::make_shared<ASTSubquery>();

        subquery->cte_name = cte_name;
        subquery->children.push_back(std::move(select_with_union_query));

        return subquery;
    }

    return select_with_union_query;
}

}
