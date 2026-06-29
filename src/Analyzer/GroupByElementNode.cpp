#include <Analyzer/GroupByElementNode.h>

#include <Common/assert_cast.h>
#include <Common/SipHash.h>

#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>

#include <Parsers/ASTGroupByElement.h>
#include <Parsers/ASTLiteral.h>

namespace DB
{

GroupByElementNode::GroupByElementNode(QueryTreeNodePtr expression_, bool with_cluster_, Float64 cluster_distance_)
    : IQueryTreeNode(children_size)
    , with_cluster(with_cluster_)
    , cluster_distance(cluster_distance_)
{
    children[expression_child_index] = std::move(expression_);
}

void GroupByElementNode::dumpTreeImpl(WriteBuffer & buffer, FormatState & format_state, size_t indent) const
{
    buffer << std::string(indent, ' ') << "GROUP_BY_ELEMENT id: " << format_state.getNodeId(this);

    if (with_cluster)
        buffer << ", with_cluster: true, cluster_distance: " << cluster_distance;

    buffer << '\n' << std::string(indent + 2, ' ') << "EXPRESSION\n";
    getExpression()->dumpTreeImpl(buffer, format_state, indent + 4);
}

bool GroupByElementNode::isEqualImpl(const IQueryTreeNode & rhs, CompareOptions) const
{
    const auto & rhs_typed = assert_cast<const GroupByElementNode &>(rhs);
    return with_cluster == rhs_typed.with_cluster && cluster_distance == rhs_typed.cluster_distance;
}

void GroupByElementNode::updateTreeHashImpl(HashState & hash_state, CompareOptions) const
{
    hash_state.update(with_cluster);
    hash_state.update(cluster_distance);
}

QueryTreeNodePtr GroupByElementNode::cloneImpl() const
{
    return std::make_shared<GroupByElementNode>(nullptr /*expression*/, with_cluster, cluster_distance);
}

ASTPtr GroupByElementNode::toASTImpl(const ConvertToASTOptions & options) const
{
    auto result = make_intrusive<ASTGroupByElement>();
    result->children.push_back(getExpression()->toAST(options));
    result->with_cluster = with_cluster;

    if (with_cluster)
        result->setClusterDistance(make_intrusive<ASTLiteral>(Field(cluster_distance)));

    return result;
}

}
