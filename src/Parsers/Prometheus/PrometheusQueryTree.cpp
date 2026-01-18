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

Node * PrometheusQueryTree::StringLiteral::clone(std::vector<std::unique_ptr<Node>> & node_list_) const
{
    return cloneNodeImpl(this, node_list_);
}

Node * PrometheusQueryTree::ScalarLiteral::clone(std::vector<std::unique_ptr<Node>> & node_list_) const
{
    return cloneNodeImpl(this, node_list_);
}

Node * PrometheusQueryTree::IntervalLiteral::clone(std::vector<std::unique_ptr<Node>> & node_list_) const
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

Node * PrometheusQueryTree::At::clone(std::vector<std::unique_ptr<Node>> & node_list_) const
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

        *this = PrometheusQueryTree{src.promql_query, new_root, std::move(new_node_list)};
    }
    return *this;
}

PrometheusQueryTree::PrometheusQueryTree(String promql_query_, const Node * root_, std::vector<std::unique_ptr<Node>> node_list_)
    : promql_query(std::move(promql_query_))
    , root(root_)
    , node_list(std::move(node_list_))
{
}

PrometheusQueryTree & PrometheusQueryTree::operator=(PrometheusQueryTree && src) noexcept
{
    promql_query = std::exchange(src.promql_query, {});
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
    constexpr const size_t NUM_SPACES_PER_INDENT = 2;

    String makeIndent(size_t indent) { return String(indent * NUM_SPACES_PER_INDENT, ' '); }
}

String PrometheusQueryTree::dumpTree(size_t indent) const
{
    if (root)
        return fmt::format("{}PrometheusQueryTree (result is {}):\n{}", makeIndent(indent), root->result_type, root->dumpTree(indent + 1));
    else
        return fmt::format("{}PrometheusQueryTree: empty", makeIndent(indent));
}

String PrometheusQueryTree::ScalarLiteral::dumpTree(size_t indent) const
{
    return fmt::format("{}ScalarLiteral({})", makeIndent(indent), ::DB::toString(scalar));
}

String PrometheusQueryTree::IntervalLiteral::dumpTree(size_t indent) const
{
    return fmt::format("{}IntervalLiteral({})", makeIndent(indent), ::DB::toString(interval));
}

String PrometheusQueryTree::StringLiteral::dumpTree(size_t indent) const
{
    return fmt::format("{}StringLiteral({})", makeIndent(indent), quoteString(string));
}

String PrometheusQueryTree::InstantSelector::dumpTree(size_t indent) const
{
    std::string_view matchers_word = (matchers.size() > 1) ? "matchers:" : ((matchers.size() == 1) ? "matcher:" : "matchers");
    String str = fmt::format("{}InstantSelector(), {} {}", makeIndent(indent), matchers.size(), matchers_word);
    for (const auto & matcher : matchers)
        str += fmt::format("\n{}{} {} {})", makeIndent(indent + 1), matcher.label_name, matcher.matcher_type, quoteString(matcher.label_value));
    return str;
}

String PrometheusQueryTree::RangeSelector::dumpTree(size_t indent) const
{
    return fmt::format("{}RangeSelector():\n{}instant_selector:\n{}\n{}range:\n{}",
                       makeIndent(indent), makeIndent(indent + 1), getInstantSelector()->dumpTree(indent + 2),
                       makeIndent(indent + 1), getRange()->dumpTree(indent + 2));
}

String PrometheusQueryTree::Subquery::dumpTree(size_t indent) const
{
    String str = fmt::format("{}Subquery():\n{}expression:\n{}\n{}range:\n{}",
                             makeIndent(indent), makeIndent(indent + 1), getExpression()->dumpTree(indent + 2),
                             makeIndent(indent + 1), getRange()->dumpTree(indent + 2));
    if (const auto * resolution = getResolution())
        str += fmt::format("\n{}resolution:\n{}", makeIndent(indent + 1), resolution->dumpTree(indent + 2));
    return str;
}

String PrometheusQueryTree::At::dumpTree(size_t indent) const
{
    String str = fmt::format("{}At():\n{}expression:\n{}", makeIndent(indent), makeIndent(indent + 1), getExpression()->dumpTree(indent + 2));
    if (const auto * at = getAt())
        str += fmt::format("\n{}at:\n{}", makeIndent(indent + 1), at->dumpTree(indent + 2));
    if (const auto * offset = getOffset())
        str += fmt::format("\n{}offset:\n{}", makeIndent(indent + 1), offset->dumpTree(indent + 2));
    return str;
}

String PrometheusQueryTree::Function::dumpTree(size_t indent) const
{
    const auto & arguments = getArguments();
    std::string_view arguments_word = (arguments.size() > 1) ? "arguments:" : ((arguments.size() == 1) ? "argument:" : "arguments");
    String str = fmt::format("{}Function(name \"{}\"): {} {}", makeIndent(indent), function_name, arguments.size(), arguments_word);
    for (const auto * argument : arguments)
        str += fmt::format("\n{}", argument->dumpTree(indent + 1));
    return str;
}

String PrometheusQueryTree::UnaryOperator::dumpTree(size_t indent) const
{
    return fmt::format("{}UnaryOperator(name \"{}\"), 1 argument:\n{}",
                       makeIndent(indent), operator_name, getArgument()->dumpTree(indent + 1));
}

String PrometheusQueryTree::BinaryOperator::dumpTree(size_t indent) const
{
    String str = fmt::format("{}BinaryOperator(name \"{}\"", makeIndent(indent), operator_name);
    if (bool_modifier)
        str += ", bool";
    if (on)
        str += ", on";
    else if (ignoring)
        str += ", ignoring";
    if ((on || ignoring) && !labels.empty())
        str += fmt::format(" [\"{}\"]", fmt::join(labels, "\", \""));
    if (group_left)
        str += ", group_left";
    else if (group_right)
        str += ", group_right";
    if ((group_left || group_right) && !extra_labels.empty())
        str += fmt::format(" [\"{}\"]", fmt::join(extra_labels, "\", \""));
    str += fmt::format("), 2 arguments:\n{}\n{}", getLeftArgument()->dumpTree(indent + 1), getRightArgument()->dumpTree(indent + 1));
    return str;
}

String PrometheusQueryTree::AggregationOperator::dumpTree(size_t indent) const
{
    String str = fmt::format("{}AggregationOperator(name \"{}\"", makeIndent(indent), operator_name);
    if (by)
        str += ", by";
    else if (without)
        str += ", without";
    if ((by || without) && !labels.empty())
        str += fmt::format(" [\"{}\"]", fmt::join(labels, "\", \""));
    const auto & arguments = getArguments();
    std::string_view arguments_word = (arguments.size() > 1) ? "arguments:" : ((arguments.size() == 1) ? "argument:" : "arguments");
    str += fmt::format("), {} {}", arguments.size(), arguments_word);
    for (const auto * argument : arguments)
        str += fmt::format("\n{}", argument->dumpTree(indent + 1));
    return str;
}

void PrometheusQueryTree::parse(std::string_view promql_query_)
{
    String error_message;
    size_t error_pos;
    if (PrometheusQueryParsingUtil::parseQuery(promql_query_, *this, error_message, error_pos))
        return;

    throw Exception(ErrorCodes::CANNOT_PARSE_PROMQL_QUERY, "{} at position {} while parsing PromQL query: {}",
                    error_message, error_pos, promql_query_);
}

bool PrometheusQueryTree::tryParse(std::string_view promql_query_, String * error_message_, size_t * error_pos_)
{
    String error_message;
    size_t error_pos;
    if (PrometheusQueryParsingUtil::parseQuery(promql_query_, *this, error_message, error_pos))
        return true;

    if (error_message_)
        *error_message_ = std::move(error_message);
    if (error_pos_)
        *error_pos_ = error_pos;
    return false;
}

}
