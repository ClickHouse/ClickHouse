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
    extern const int NOT_IMPLEMENTED;
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

        *this = PrometheusQueryTree{std::move(new_node_list), new_root, src.timestamp_scale};
    }
    return *this;
}

PrometheusQueryTree::PrometheusQueryTree(std::vector<std::unique_ptr<Node>> node_list_, const Node * root_, UInt32 timestamp_scale_)
    : node_list(std::move(node_list_))
    , root(root_)
    , timestamp_scale(timestamp_scale_)
{
}

PrometheusQueryTree::PrometheusQueryTree(std::unique_ptr<Node> single_node_, UInt32 timestamp_scale_)
    : timestamp_scale(timestamp_scale_)
{
    node_list.emplace_back(std::move(single_node_));
    root = node_list.back().get();
}

PrometheusQueryTree & PrometheusQueryTree::operator=(PrometheusQueryTree && src) noexcept
{
    node_list = std::exchange(src.node_list, {});
    root = std::exchange(src.root, nullptr);
    timestamp_scale = std::exchange(src.timestamp_scale, 0);
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


namespace
{
    String quotePromQLString(std::string_view str)
    {
        return doubleQuoteString(str);
    }
}

String PrometheusQueryTree::Scalar::toString(const PrometheusQueryTree &) const
{
    if (std::isfinite(scalar))
    {
        return ::DB::toString(scalar);
    }
    else if (std::isinf(scalar))
    {
        String str;
        if (scalar < 0)
            str += "-";
        str += "Inf";
        return str;
    }
    else
    {
        return "NaN";
    }
}

String PrometheusQueryTree::StringLiteral::toString(const PrometheusQueryTree &) const
{
    return quotePromQLString(string);
}

String PrometheusQueryTree::InstantSelector::toString(const PrometheusQueryTree &) const
{
    bool has_metric_name = false;
    size_t metric_name_pos = static_cast<size_t>(-1);
    for (size_t i = 0; i != matchers.size(); ++i)
    {
        const auto & matcher = matchers[i];
        if ((matcher.label_name == "__name__") && (matcher.matcher_type == MatcherType::EQ))
        {
            has_metric_name = true;
            metric_name_pos = i;
            break;
        }
    }

    String str;
    if (has_metric_name)
    {
        str += matchers[metric_name_pos].label_value;
    }

    if (!has_metric_name || (matchers.size() - has_metric_name > 0))
    {
        str += "{";
        bool need_comma = false;
        for (size_t i = 0; i != matchers.size(); ++i)
        {
            if (i == metric_name_pos)
                continue;
            const auto & matcher = matchers[i];
            if (need_comma)
                str += ",";
            str += matcher.label_name;
            std::string_view matcher_type_str;
            switch (matcher.matcher_type)
            {
                case MatcherType::EQ:  matcher_type_str = "=";  break;
                case MatcherType::NE:  matcher_type_str = "!="; break;
                case MatcherType::RE:  matcher_type_str = "=~"; break;
                case MatcherType::NRE: matcher_type_str = "!~"; break;
            }
            chassert(!matcher_type_str.empty());
            str += matcher_type_str;
            str += quotePromQLString(matcher.label_value);
            need_comma = true;
        }
        str += "}";
    }

    return str;
}

String PrometheusQueryTree::RangeSelector::toString(const PrometheusQueryTree & tree) const
{
    String str = getInstantSelector()->toString(tree);
    str += "[";
    str += DB::toString(range, tree.timestamp_scale);
    str += "]";
    return str;
}

String PrometheusQueryTree::Subquery::toString(const PrometheusQueryTree & tree) const
{
    bool need_parentheses = (getPrecedence() <= getExpression()->getPrecedence());

    String str;
    if (need_parentheses)
        str += "(";
    str += getExpression()->toString(tree);
    if (need_parentheses)
        str += ")";

    str += "[";
    str += DB::toString(range, tree.timestamp_scale);
    str += ":";
    if (step)
        str += DB::toString(*step, tree.timestamp_scale);
    str += "]";

    return str;
}

String PrometheusQueryTree::Offset::toString(const PrometheusQueryTree & tree) const
{
    String str = getExpression()->toString(tree);
    if (at_timestamp)
    {
        str += " @ ";
        str += DB::toString(Decimal64{*at_timestamp}, tree.timestamp_scale);
    }
    if (offset_value)
    {
        str += " offset ";
        str += DB::toString(Decimal64{*offset_value}, tree.timestamp_scale);
    }
    return str;
}

String PrometheusQueryTree::Function::toString(const PrometheusQueryTree & tree) const
{
    String str = function_name;
    str += "(";
    bool need_comma = false;
    for (const auto * arg : getArguments())
    {
        if (need_comma)
            str += ", ";
        str += arg->toString(tree);
        need_comma = true;
    }
    str += ")";
    return str;
}

String PrometheusQueryTree::UnaryOperator::toString(const PrometheusQueryTree & tree) const
{
    bool need_parentheses = (getPrecedence() < getArgument()->getPrecedence());
    String str = operator_name;
    if (need_parentheses)
        str += "(";
    str += getArgument()->toString(tree);
    if (need_parentheses)
        str += ")";
    return str;
}

String PrometheusQueryTree::BinaryOperator::toString(const PrometheusQueryTree & tree) const
{
    auto precedence = getPrecedence();
    auto left_arg_precedence = getLeftArgument()->getPrecedence();
    auto right_arg_precedence = getRightArgument()->getPrecedence();
    bool need_left_parentheses = (precedence < left_arg_precedence) || (precedence == left_arg_precedence && isRightAssociative());
    bool need_right_parentheses = (precedence <= right_arg_precedence);

    String str;
    if (need_left_parentheses)
        str += "(";
    str += getLeftArgument()->toString(tree);
    if (need_left_parentheses)
        str += ")";

    str += " ";
    str += operator_name;
    str += " ";

    if (bool_modifier)
        str += "bool ";

    if (on)
        str += "on(";
    else if (ignoring)
        str += "ignoring(";

    if (on || ignoring)
    {
        bool need_comma = false;
        for (const auto & label : labels)
        {
            if (need_comma)
                str += ", ";
            str += label;
            need_comma = true;
        }
        str += ") ";
    }

    if (group_left)
        str += "group_left";
    else if (group_right)
        str += "group_right";

    if (group_left || group_right)
    {
        if (!extra_labels.empty())
        {
            str += "(";
            bool need_comma = false;
            for (const auto & label : extra_labels)
            {
                if (need_comma)
                    str += ", ";
                str += label;
                need_comma = true;
            }
            str += ")";
        }
        str += " ";
    }

    if (need_right_parentheses)
        str += "(";
    str += getRightArgument()->toString(tree);
    if (need_right_parentheses)
        str += ")";

    return str;
}

int PrometheusQueryTree::Scalar::getPrecedence() const
{
    if ((std::isfinite(scalar) || std::isinf(scalar)) && scalar < 0)
        return 3; /// same as unary operator '-'
    else
        return 0;
}

int PrometheusQueryTree::Subquery::getPrecedence() const
{
    return 1; /// before anything what have precedence (we need parentheses around `expr` in "(expr)[1d:1h]" if expr is any operator)
}

int PrometheusQueryTree::UnaryOperator::getPrecedence() const
{
    return 3; /// same as binary operator '*'
}

int PrometheusQueryTree::BinaryOperator::getPrecedence() const
{
    if (operator_name == "^")
        return 2;
    if ((operator_name == "*") || (operator_name == "/") || (operator_name == "%") || (operator_name == "atan2"))
        return 3;
    if ((operator_name == "+") || (operator_name == "-"))
        return 4;
    if ((operator_name == "==") || (operator_name == "!=") || (operator_name == "<") || (operator_name == ">") || (operator_name == "<=") || (operator_name == ">="))
        return 5;
    if ((operator_name == "and") || (operator_name == "unless"))
        return 6;
    if (operator_name == "or")
        return 7;
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Binary operator {} is not implemented", operator_name);
}

bool PrometheusQueryTree::BinaryOperator::isRightAssociative() const
{
    return (operator_name == "^"); /// 2 ^ 3 ^ 2 is equivalent to 2 ^ (3 ^ 2)
}

String PrometheusQueryTree::AggregationOperator::toString(const PrometheusQueryTree & tree) const
{
    String str = operator_name;

    if (by)
        str += " by (";
    else if (without)
        str += " without (";

    if (by || without)
    {
        bool need_comma = false;
        for (const auto & label : labels)
        {
            if (need_comma)
                str += ", ";
            str += label;
            need_comma = true;
        }
        str += ") ";
    }

    str += "(";
    bool need_comma = false;
    for (const auto * arg : getArguments())
    {
        if (need_comma)
            str += ", ";
        str += arg->toString(tree);
        need_comma = true;
    }
    str += ")";

    return str;
}

String PrometheusQueryTree::toString() const
{
    if (empty())
        return "";
    return getRoot()->toString(*this);
}

}
