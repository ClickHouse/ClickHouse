#pragma once

#include <Common/typeid_cast.h>
#include <Core/Field.h>
#include <Parsers/Prometheus/PrometheusQueryResultType.h>
#include <fmt/format.h>


namespace DB
{

/// A tree representing a parsed prometheus query.
class PrometheusQueryTree
{
public:
    /// The scalar type is used to represent floating-point values, for example -539.8 or 1736046605.
    using ScalarType = Float64;

    /// The interval type is used to represent time intervals, for example 1d30m.
    using IntervalType = DecimalField<Decimal64>;

    enum class MatcherType { EQ /* = */, NE /* != */, RE /* =~ */, NRE /* !~ */};

    /// A matcher for a label or for the metric name. Matchers are used in instant selectors and range selectors.
    /// Examples: __name__="http_requests"
    ///           job="prometheus"
    ///           release=~"canary|testing"
    struct Matcher
    {
    public:
        String label_name;
        String label_value;
        MatcherType matcher_type;
    };

    using MatcherList = std::vector<Matcher>;

    enum class NodeType
    {
        StringLiteral,
        ScalarLiteral,
        IntervalLiteral,
        InstantSelector,
        RangeSelector,
        Subquery,
        At,
        Function,
        UnaryOperator,
        BinaryOperator,
        AggregationOperator,
    };

    using ResultType = PrometheusQueryResultType;

    class Node
    {
    public:
        NodeType node_type;
        size_t start_pos = String::npos; /// Start position of the promql query's part which this node represents with its children.
        size_t length = 0;               /// Length of the promql query's part which this node represents with its children.
        ResultType result_type;          /// The data type this node with its children evaluates to.
        std::vector<const Node *> children;  /// E.g. arguments for a function, matchers for selectors.
        const Node * parent = nullptr;
        Node() = default;
        Node(const Node &) = default;
        virtual ~Node() = default;
        virtual Node * clone(std::vector<std::unique_ptr<Node>> & node_list_) const = 0;
        virtual String dumpTree(size_t indent) const = 0;
    };

    /// A scalar literal, i.e. a floating-point or an integer number.
    /// Examples: -2.43, 2h30m
    class ScalarLiteral : public Node
    {
    public:
        ScalarType scalar;
        ScalarLiteral() { node_type = NodeType::ScalarLiteral; result_type = ResultType::SCALAR; }
        Node * clone(std::vector<std::unique_ptr<Node>> & node_list_) const override;
        String dumpTree(size_t indent) const override;
    };

    /// An offset written in the format 2h30m or -1d.
    class IntervalLiteral : public Node
    {
    public:
        IntervalType interval;
        IntervalLiteral() { node_type = NodeType::IntervalLiteral; result_type = ResultType::SCALAR; }
        Node * clone(std::vector<std::unique_ptr<Node>> & node_list_) const override;
        String dumpTree(size_t indent) const override;
    };

    /// A string literal.
    /// Example: "abc"
    class StringLiteral : public Node
    {
    public:
        String string;
        StringLiteral() { node_type = NodeType::StringLiteral; result_type = ResultType::STRING; }
        Node * clone(std::vector<std::unique_ptr<Node>> & node_list_) const override;
        String dumpTree(size_t indent) const override;
    };

    /// An instant selector.
    /// Example: http_requests{job="prometheus"}
    class InstantSelector : public Node
    {
    public:
        MatcherList matchers;
        InstantSelector() { node_type = NodeType::InstantSelector; result_type = ResultType::INSTANT_VECTOR; }
        Node * clone(std::vector<std::unique_ptr<Node>> & node_list_) const override;
        String dumpTree(size_t indent) const override;
    };

    /// A range selector.
    /// Example: http_requests{job="prometheus"}[20m]
    class RangeSelector : public Node
    {
    public:
        const InstantSelector * getInstantSelector() const { return &typeid_cast<const InstantSelector &>(*children.at(0)); }
        const Node * getRange() const { return children.at(1); } /// [20m]
        RangeSelector() { node_type = NodeType::RangeSelector; result_type = ResultType::RANGE_VECTOR; }
        Node * clone(std::vector<std::unique_ptr<Node>> & node_list_) const override;
        String dumpTree(size_t indent) const override;
    };

    /// Represents a subquery, i.e. <expression>[<range>:<resolution>]. Here resolution can be omitted, but the colon always presents.
    /// Examples: <expression>[1h:5m]
    ///           <expression>[1h:]
    class Subquery : public Node
    {
    public:
        const Node * getExpression() const { return children.at(0); }
        const Node * getRange() const { return children.at(1); } /// [1h: ...]
        const Node * getResolution() const { return (children.size() >= 3) ? children[2] : nullptr; } /// [... :5m]
        Subquery() { node_type = NodeType::Subquery; result_type = ResultType::RANGE_VECTOR; }
        Node * clone(std::vector<std::unique_ptr<Node>> & node_list_) const override;
        String dumpTree(size_t indent) const override;
    };

    /// Represents a change of the evaluation time applied to an instant selector or a range selector or a subquery.
    /// Examples: <expression> offset 1d
    ///           <expression> @ 1609746000
    class At : public Node
    {
    public:
        size_t at_index = static_cast<size_t>(-1);
        size_t offset_index = static_cast<size_t>(-1);
        const Node * getExpression() const { return children.at(0); }
        const Node * getAt() const { return (at_index < children.size()) ? children.at(at_index) : nullptr; } /// @ timestamp
        const Node * getOffset() const { return (offset_index < children.size()) ? children.at(offset_index) : nullptr; } /// offset <offset>
        At() { node_type = NodeType::At; }
        Node * clone(std::vector<std::unique_ptr<Node>> & node_list_) const override;
        String dumpTree(size_t indent) const override;
    };

    /// A function with parameters in parentheses.
    /// Examples: abs(<argument>)
    ///           rate(<argument>)
    ///           pi()
    class Function : public Node
    {
    public:
        String function_name;
        const std::vector<const Node *> & getArguments() const { return children; }
        Function() { node_type = NodeType::Function; }
        Node * clone(std::vector<std::unique_ptr<Node>> & node_list_) const override;
        String dumpTree(size_t indent) const override;
    };

    /// An unary operator: either +<argument> or -<argument>.
    class UnaryOperator : public Node
    {
    public:
        String operator_name;
        const Node * getArgument() const { return children.at(0); }
        UnaryOperator() { node_type = NodeType::UnaryOperator; }
        Node * clone(std::vector<std::unique_ptr<Node>> & node_list_) const override;
        String dumpTree(size_t indent) const override;
    };

    /// A binary operator: <left-argument> <operation-name> on(<on-labels>) group_left(<extra-labels>) <right-argument>
    /// Examples: foo + on(color) bar
    ///           foo + on(color) group_left bar
    class BinaryOperator : public Node
    {
    public:
        String operator_name;
        bool on = false;
        bool ignoring = false;
        Strings labels;
        bool group_left = false;
        bool group_right = false;
        Strings extra_labels;
        bool bool_modifier = false;
        const Node * getLeftArgument() const { return children.at(0); }
        const Node * getRightArgument() const { return children.at(1); }
        BinaryOperator() { node_type = NodeType::BinaryOperator; }
        Node * clone(std::vector<std::unique_ptr<Node>> & node_list_) const override;
        String dumpTree(size_t indent) const override;
    };

    /// An aggregation operator: <operator-name> [by (<by-labels>) | without (<without-labels>)] (<arguments>)
    /// Examples: sum without (instance) (http_requests_total)
    ///           sum by (application, group) (http_requests_total)
    ///           sum(http_requests_total)
    class AggregationOperator : public Node
    {
    public:
        String operator_name;
        bool by = false;
        bool without = false;
        Strings labels;
        const std::vector<const Node *> & getArguments() const { return children; }
        AggregationOperator() { node_type = NodeType::AggregationOperator; }
        Node * clone(std::vector<std::unique_ptr<Node>> & node_list_) const override;
        String dumpTree(size_t indent) const override;
    };

    PrometheusQueryTree() = default;
    PrometheusQueryTree(const PrometheusQueryTree & src) { *this = src; }
    PrometheusQueryTree(PrometheusQueryTree && src) noexcept { *this = std::move(src); }
    PrometheusQueryTree & operator=(const PrometheusQueryTree & src);
    PrometheusQueryTree & operator=(PrometheusQueryTree && src) noexcept;

    /// Constructs a PrometheusQueryTree from a prepared list of nodes.
    PrometheusQueryTree(String promql_query_, const Node * root_, std::vector<std::unique_ptr<Node>> node_list_);

    /// Parses a promql query.
    explicit PrometheusQueryTree(std::string_view promql_query_) { parse(promql_query_); }

    /// Parses a promql query.
    /// This function throws an exception if something is wrong with the syntax.
    void parse(std::string_view promql_query_);

    /// Tries to parse a promql query. Returns true if successful.
    /// If it isn't successful the function sets `error_pos` and `error_message` and returns false.
    bool tryParse(std::string_view promql_query_, String * error_message_ = nullptr, size_t * error_pos_ = nullptr);

    bool empty() const { return node_list.empty(); }
    size_t size() const { return node_list.size(); }

    /// Returns the root node.
    const Node * getRoot() const { return root; }

    /// Returns the promql query which was parsed to build this tree.
    const String & getQuery() const { return promql_query; }
    const String & toString() const { return getQuery(); }

    /// Returns a part of the promql query corresponding to a specific node of this tree.
    std::string_view getQuery(const Node * node) const { return std::string_view{getQuery()}.substr(node->start_pos, node->length); }

    /// Returns the type of the query's returning value.
    ResultType getResultType() const;

    /// Dumps the tree to string as a tree for debugging purposes.
    String dumpTree(size_t indent = 0) const;

private:
    String promql_query;
    const Node * root = nullptr;
    std::vector<std::unique_ptr<Node>> node_list;
};

}

template <>
struct fmt::formatter<DB::PrometheusQueryTree>
{
    template<typename ParseContext>
    constexpr auto parse(ParseContext & context)
    {
        return context.begin();
    }

    template <typename FormatContext>
    auto format(const DB::PrometheusQueryTree & promql, FormatContext & ctx) const
    {
        return fmt::format_to(ctx.out(), "{}", promql.toString());
    }
};
