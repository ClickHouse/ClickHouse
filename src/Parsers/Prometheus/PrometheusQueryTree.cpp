#include <Parsers/Prometheus/PrometheusQueryTree.h>

#include <Common/Exception.h>
#include <Common/quoteString.h>
#include <IO/WriteHelpers.h>
#include <Parsers/Prometheus/PrometheusQueryParsingUtil.h>
#include <fmt/ranges.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_PROMQL_QUERY;
    extern const int LOGICAL_ERROR;
}

namespace
{
    using Node = PrometheusQueryTree::Node;

    template <typename NodeType>
    NodeType * cloneNodeImpl(const NodeType * node, std::vector<std::unique_ptr<Node>> & node_list)
    {
        auto new_node = std::make_unique<NodeType>(*node);
        auto * ptr = new_node.get();
        for (const auto * & child : new_node->children)
        {
            auto * new_child = child->clone(node_list);
            new_child->parent = new_node.get();
            child = new_child;
        }
        new_node->parent = nullptr;
        node_list.emplace_back(std::move(new_node));
        return ptr;
    }
}

Node * PrometheusQueryTree::Scalar::clone(std::vector<std::unique_ptr<Node>> & node_list_) const
{
    return cloneNodeImpl(this, node_list_);
}

Node * PrometheusQueryTree::StringLiteral::clone(std::vector<std::unique_ptr<Node>> & node_list_) const
{
    return cloneNodeImpl(this, node_list_);
}

Node * PrometheusQueryTree::InstantSelector::clone(std::vector<std::unique_ptr<Node>> & node_list_) const
{
    return cloneNodeImpl(this, node_list_);
}

Node * PrometheusQueryTree::RangeSelector::clone(std::vector<std::unique_ptr<Node>> & node_list_) const
{
    return cloneNodeImpl(this, node_list_);
}

Node * PrometheusQueryTree::Subquery::clone(std::vector<std::unique_ptr<Node>> & node_list_) const
{
    return cloneNodeImpl(this, node_list_);
}

Node * PrometheusQueryTree::Offset::clone(std::vector<std::unique_ptr<Node>> & node_list_) const
{
    return cloneNodeImpl(this, node_list_);
}

Node * PrometheusQueryTree::Function::clone(std::vector<std::unique_ptr<Node>> & node_list_) const
{
    return cloneNodeImpl(this, node_list_);
}

Node * PrometheusQueryTree::UnaryOperator::clone(std::vector<std::unique_ptr<Node>> & node_list_) const
{
    return cloneNodeImpl(this, node_list_);
}

Node * PrometheusQueryTree::BinaryOperator::clone(std::vector<std::unique_ptr<Node>> & node_list_) const
{
    return cloneNodeImpl(this, node_list_);
}

Node * PrometheusQueryTree::AggregationOperator::clone(std::vector<std::unique_ptr<Node>> & node_list_) const
{
    return cloneNodeImpl(this, node_list_);
}

PrometheusQueryTree & PrometheusQueryTree::operator=(const PrometheusQueryTree & src)
{
    if (this != &src)
    {
        const Node * new_root = nullptr;
        std::vector<std::unique_ptr<Node>> new_node_list;
        new_node_list.reserve(src.node_list.size());

        if (src.root)
            new_root = src.root->clone(new_node_list);

        *this = PrometheusQueryTree{src.promql_query, src.timestamp_scale, new_root, std::move(new_node_list)};
    }
    return *this;
}

PrometheusQueryTree::PrometheusQueryTree(String promql_query_, UInt32 timestamp_scale_, const Node * root_, std::vector<std::unique_ptr<Node>> node_list_)
    : promql_query(std::move(promql_query_))
    , timestamp_scale(timestamp_scale_)
    , root(root_)
    , node_list(std::move(node_list_))
{
}

PrometheusQueryTree & PrometheusQueryTree::operator=(PrometheusQueryTree && src) noexcept
{
    promql_query = std::exchange(src.promql_query, {});
    timestamp_scale = std::exchange(src.timestamp_scale, 0);
    root = std::exchange(src.root, nullptr);
    node_list = std::exchange(src.node_list, {});
    return *this;
}

PrometheusQueryResultType PrometheusQueryTree::getResultType() const
{
    if (!root)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Prometheus query tree shouldn't be empty");
    return root->result_type;
}

namespace
{
    constexpr const size_t NUM_SPACES_PER_INDENT = 4;

    String makeIndent(size_t indent) { return String(indent * NUM_SPACES_PER_INDENT, ' '); }
}

String PrometheusQueryTree::dumpTree() const
{
    if (root)
        return fmt::format("\nPrometheusQueryTree({}):\n{}\n", root->result_type, root->dumpNode(*this, 1));
    else
        return "\nPrometheusQueryTree(EMPTY)\n";
}

String PrometheusQueryTree::Scalar::dumpNode(const PrometheusQueryTree & /* tree */, size_t indent) const
{
    return fmt::format("{}Scalar({})", makeIndent(indent), ::DB::toString(scalar));
}

String PrometheusQueryTree::StringLiteral::dumpNode(const PrometheusQueryTree & /* tree */, size_t indent) const
{
    return fmt::format("{}StringLiteral({})", makeIndent(indent), quoteString(string));
}

String PrometheusQueryTree::InstantSelector::dumpNode(const PrometheusQueryTree & /* tree */, size_t indent) const
{
    String str = fmt::format("{}InstantSelector:", makeIndent(indent));
    for (const auto & matcher : matchers)
        str += fmt::format("\n{}{} {} {}", makeIndent(indent + 1), matcher.label_name, matcher.matcher_type, quoteString(matcher.label_value));
    return str;
}

String PrometheusQueryTree::RangeSelector::dumpNode(const PrometheusQueryTree & tree, size_t indent) const
{
    String str = fmt::format("{}RangeSelector:", makeIndent(indent));
    str += fmt::format("\n{}range: {}", makeIndent(indent + 1), ::DB::toString(range, tree.timestamp_scale));
    str += fmt::format("\n{}", getInstantSelector()->dumpNode(tree, indent + 1));
    return str;
}

String PrometheusQueryTree::Subquery::dumpNode(const PrometheusQueryTree & tree, size_t indent) const
{
    String str = fmt::format("{}Subquery:", makeIndent(indent));
    str += fmt::format("\n{}range: {}", makeIndent(indent + 1), ::DB::toString(range, tree.timestamp_scale));
    if (step)
        str += fmt::format("\n{}step: {}", makeIndent(indent + 1), ::DB::toString(*step, tree.timestamp_scale));
    str += fmt::format("\n{}", getExpression()->dumpNode(tree, indent + 1));
    return str;
}

String PrometheusQueryTree::Offset::dumpNode(const PrometheusQueryTree & tree, size_t indent) const
{
    String str = fmt::format("{}Offset:", makeIndent(indent));
    if (at_timestamp)
        str += fmt::format("\n{}at: {}", makeIndent(indent + 1), ::DB::toString(*at_timestamp, tree.timestamp_scale));
    if (offset_value)
        str += fmt::format("\n{}offset: {}", makeIndent(indent + 1), ::DB::toString(*offset_value, tree.timestamp_scale));
    str += fmt::format("\n{}", getExpression()->dumpNode(tree, indent + 1));
    return str;
}

String PrometheusQueryTree::Function::dumpNode(const PrometheusQueryTree & tree, size_t indent) const
{
    const auto & arguments = getArguments();
    std::string_view maybe_colon = arguments.empty() ? "" : ":";
    String str = fmt::format("{}Function({}){}", makeIndent(indent), function_name, maybe_colon);
    for (const auto * argument : arguments)
        str += fmt::format("\n{}", argument->dumpNode(tree, indent + 1));
    return str;
}

String PrometheusQueryTree::UnaryOperator::dumpNode(const PrometheusQueryTree & tree, size_t indent) const
{
    String str = fmt::format("{}UnaryOperator({})", makeIndent(indent), operator_name);
    str += fmt::format("\n{}", getArgument()->dumpNode(tree, indent + 1));
    return str;
}

String PrometheusQueryTree::BinaryOperator::dumpNode(const PrometheusQueryTree & tree, size_t indent) const
{
    String str = fmt::format("{}BinaryOperator({})", makeIndent(indent), operator_name);
    if (bool_modifier)
        str += fmt::format("\n{}bool", makeIndent(indent + 1));
    if (on || ignoring)
    {
        std::string_view on_or_ignoring = on ? "on" : "ignoring";
        String joined_labels;
        if (!labels.empty())
            joined_labels += fmt::format(" {}", fmt::join(labels, ", "));
        str += fmt::format("\n{}{}{}", makeIndent(indent + 1), on_or_ignoring, joined_labels);
    }
    if (group_left || group_right)
    {
        std::string_view group_left_or_right = group_left ? "group_left" : "group_right";
        String joined_extra_labels;
        if (!extra_labels.empty())
            joined_extra_labels += fmt::format(" {}", fmt::join(extra_labels, ", "));
        str += fmt::format("\n{}{}{}", makeIndent(indent + 1), group_left_or_right, joined_extra_labels);
    }
    str += fmt::format("\n{}", getLeftArgument()->dumpNode(tree, indent + 1));
    str += fmt::format("\n{}", getRightArgument()->dumpNode(tree, indent + 1));
    return str;
}

String PrometheusQueryTree::AggregationOperator::dumpNode(const PrometheusQueryTree & tree, size_t indent) const
{
    String str = fmt::format("{}AggregationOperator({})", makeIndent(indent), operator_name);
    if (by || without)
    {
        std::string_view by_or_without = by ? "by" : "without";
        String joined_labels;
        if (!labels.empty())
            joined_labels += fmt::format(" {}", fmt::join(labels, ", "));
        str += fmt::format("\n{}{}{}", makeIndent(indent + 1), by_or_without, joined_labels);
    }
    for (const auto * argument : getArguments())
        str += fmt::format("\n{}", argument->dumpNode(tree, indent + 1));
    return str;
}

void PrometheusQueryTree::parse(std::string_view promql_query_, UInt32 timestamp_scale_)
{
    String error_message;
    size_t error_pos;
    if (PrometheusQueryParsingUtil::tryParseQuery(promql_query_, timestamp_scale_, *this, &error_message, &error_pos))
        return;

    throw Exception(ErrorCodes::CANNOT_PARSE_PROMQL_QUERY, "{} at position {} while parsing PromQL query: {}",
                    error_message, error_pos, promql_query_);
}

bool PrometheusQueryTree::tryParse(std::string_view promql_query_, UInt32 timestamp_scale_, String * error_message_, size_t * error_pos_)
{
    return PrometheusQueryParsingUtil::tryParseQuery(promql_query_, timestamp_scale_, *this, error_message_, error_pos_);
}

}
