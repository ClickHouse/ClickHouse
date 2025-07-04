#include <Parsers/Prometheus/PrometheusQueryTree.h>

#include <Common/Exception.h>
#include <Common/quoteString.h>
#include <IO/WriteHelpers.h>
#include <Parsers/Prometheus/PrometheusQueryParsingUtil.h>
#include <boost/algorithm/string/join.hpp>


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

Node * PrometheusQueryTree::OffsetLiteral::clone(std::vector<std::unique_ptr<Node>> & node_list_) const
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

PrometheusQueryTree & PrometheusQueryTree::operator=(PrometheusQueryTree && src)
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
        return fmt::format("{}PrometheusQueryTree (result is {})\n{}", makeIndent(indent), root->result_type, root->dumpTree(indent + 1));
    else
        return fmt::format("{}PrometheusQueryTree: empty\n", makeIndent(indent));
}

String PrometheusQueryTree::StringLiteral::dumpTree(size_t indent) const
{
    return fmt::format("{}StringLiteral({})\n", makeIndent(indent), quoteString(string));
}

String PrometheusQueryTree::ScalarLiteral::dumpTree(size_t indent) const
{
    return fmt::format("{}ScalarLiteral({})\n", makeIndent(indent), ::DB::toString(scalar));
}

String PrometheusQueryTree::OffsetLiteral::dumpTree(size_t indent) const
{
    return fmt::format("{}OffsetLiteral({})\n", makeIndent(indent), ::DB::toString(offset));
}

String PrometheusQueryTree::InstantSelector::dumpTree(size_t indent) const
{
    String str = fmt::format("{}InstantSelector(", makeIndent(indent));
    std::string_view matchers_word = (matchers.size() > 1) ? "matchers:" : ((matchers.size() == 1) ? "matcher:" : "matchers");
    str += fmt::format("), {} {}\n", matchers.size(), matchers_word);
    for (const auto & matcher : matchers)
        str += fmt::format("{}{} {} {})\n", makeIndent(indent + 1), matcher.label_name, matcher.matcher_type, quoteString(matcher.label_value));
    return str;
}

String PrometheusQueryTree::RangeSelector::dumpTree(size_t indent) const
{
    String str = fmt::format("{}RangeSelector(range {}s), instant_selector:\n", makeIndent(indent), ::DB::toString(range));
    str += getInstantSelector()->dumpTree(indent + 1);    
    return str;
}

String PrometheusQueryTree::Subquery::dumpTree(size_t indent) const
{
    String str = fmt::format("{}Subquery(range {}s", makeIndent(indent), ::DB::toString(range));
    if (resolution)
        str += fmt::format(", resolution {}s", ::DB::toString(*resolution));
    str += "), expression:\n";
    str += getExpression()->dumpTree(indent + 1);    
    return str;
}

String PrometheusQueryTree::At::dumpTree(size_t indent) const
{
    String str = fmt::format("{}At(", makeIndent(indent));
    if (at)
        str += quoteString(::DB::toString(*at));
    if (at && static_cast<bool>(offset))
        str += " ";
    if (static_cast<bool>(offset))
        str += fmt::format("offset {}s", ::DB::toString(offset));
    str += "), expression:\n";
    str += getExpression()->dumpTree(indent + 1);    
    return str;
}

String PrometheusQueryTree::Function::dumpTree(size_t indent) const
{
    const auto & arguments = getArguments();
    std::string_view arguments_word = (arguments.size() > 1) ? "arguments:" : ((arguments.size() == 1) ? "argument:" : "arguments");
    String str = fmt::format("{}Function(name \"{}\"): {} {}\n", makeIndent(indent), function_name, arguments.size(), arguments_word);
    for (const auto * argument : arguments)
        str += argument->dumpTree(indent + 1);
    return str;
}

String PrometheusQueryTree::UnaryOperator::dumpTree(size_t indent) const
{
    String str = makeIndent(indent) + "UnaryOperator(";
    str += fmt::format("name \"{}\"", operator_name);
    str += "), 1 argument:\n";
    str += getArgument()->dumpTree(indent + 1);
    return str;
}

String PrometheusQueryTree::BinaryOperator::dumpTree(size_t indent) const
{
    String str = makeIndent(indent) + "BinaryOperator(";
    str += fmt::format("name \"{}\"", operator_name);
    if (bool_modifier)
        str += ", bool";
    if (on)
        str += ", on";
    else if (ignoring)
        str += ", ignoring";
    if ((on || ignoring) && !labels.empty())
        str += " [\"" + boost::algorithm::join(labels, "\", \"") + "\"]";
    if (group_left)
        str += ", group_left";
    else if (group_right)
        str += ", group_right";
    if ((group_left || group_right) && !extra_labels.empty())
        str += " [\"" + boost::algorithm::join(extra_labels, "\", \"") + "\"]";
    str += "), 2 arguments:\n";
    str += getLeftArgument()->dumpTree(indent + 1);
    str += getRightArgument()->dumpTree(indent + 1);
    return str;
}

String PrometheusQueryTree::AggregationOperator::dumpTree(size_t indent) const
{
    String str = makeIndent(indent) + "AggregationOperator(";
    str += fmt::format("name \"{}\"", operator_name);
    if (by)
        str += ", by";
    else if (without)
        str += ", without";
    if ((by || without) && !labels.empty())
        str += " [\"" + boost::algorithm::join(labels, "\", \"") + "\"]";
    const auto & arguments = getArguments();
    std::string_view arguments_word = (arguments.size() > 1) ? "arguments:" : ((arguments.size() == 1) ? "argument:" : "arguments");
    str += fmt::format("), {} {}\n", arguments.size(), arguments_word);
    for (const auto * argument : arguments)
        str += argument->dumpTree(indent + 1);
    return str;
}

void PrometheusQueryTree::parse(std::string_view promql_query_, UInt32 timestamp_scale_)
{
    String error_message;
    size_t error_pos;
    if (PrometheusQueryParsingUtil::parseQuery(promql_query_, timestamp_scale_, *this, error_message, error_pos))
        return;

    throw Exception(ErrorCodes::CANNOT_PARSE_PROMQL_QUERY, "{} at position {} while parsing PromQL query: {}",
                    error_message, error_pos, promql_query_);
}

bool PrometheusQueryTree::tryParse(std::string_view promql_query_, UInt32 timestamp_scale_, String * error_message_, size_t * error_pos_)
{
    String error_message;
    size_t error_pos;
    if (PrometheusQueryParsingUtil::parseQuery(promql_query_, timestamp_scale_, *this, error_message, error_pos))
        return true;

    if (error_message_)
        *error_message_ = std::move(error_message);
    if (error_pos_)
        *error_pos_ = error_pos;
    return false;
}

}
