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
    /// The scalar type is used to store a floating-point value, for example -539.8 or 1736046605.
    using ScalarType = Float64;

    /// The timestamp type is used to store a value after '@'.
    using TimestampType = DateTime64;

    /// The duration type is used to store durations in range selectors and subqueries, and also a value after the 'offset' keyword.
    using DurationType = Decimal64;

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
        Scalar,
        StringLiteral,
        InstantSelector,
        RangeSelector,
        Subquery,
        Offset,
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
        ResultType result_type;          /// The data type this node with its children evaluates to.
        std::vector<const Node *> children;  /// E.g. arguments for a function, matchers for selectors.
        const Node * parent = nullptr;
        Node() = default;
        Node(const Node &) = default;
        virtual ~Node() = default;
        virtual Node * clone(std::vector<std::unique_ptr<Node>> & node_list_) const = 0;
        virtual String dumpNode(const PrometheusQueryTree & tree, size_t indent) const = 0;
        virtual String toString(const PrometheusQueryTree & tree) const = 0;
        virtual int getPrecedence() const { return 0; }
    };

    /// A scalar literal, i.e. a floating-point or an integer number.
    /// Examples: -2.43, 2h30m
    class Scalar : public Node
    {
    public:
        ScalarType scalar;
        Scalar() { node_type = NodeType::Scalar; result_type = ResultType::SCALAR; }
        Node * clone(std::vector<std::unique_ptr<Node>> & node_list_) const override;
        String dumpNode(const PrometheusQueryTree & tree, size_t indent) const override;
        String toString(const PrometheusQueryTree & tree) const override;
        int getPrecedence() const override;
    };

    /// A string literal.
    /// Example: "abc"
    class StringLiteral : public Node
    {
    public:
        String string;
        StringLiteral() { node_type = NodeType::StringLiteral; result_type = ResultType::STRING; }
        Node * clone(std::vector<std::unique_ptr<Node>> & node_list_) const override;
        String dumpNode(const PrometheusQueryTree & tree, size_t indent) const override;
        String toString(const PrometheusQueryTree & tree) const override;
    };

    /// An instant selector.
    /// Example: http_requests{job="prometheus"}
    class InstantSelector : public Node
    {
    public:
        MatcherList matchers;
        InstantSelector() { node_type = NodeType::InstantSelector; result_type = ResultType::INSTANT_VECTOR; }
        Node * clone(std::vector<std::unique_ptr<Node>> & node_list_) const override;
        String dumpNode(const PrometheusQueryTree & tree, size_t indent) const override;
        String toString(const PrometheusQueryTree & tree) const override;
    };

    /// A range selector.
    /// Example: http_requests{job="prometheus"}[20m]
    class RangeSelector : public Node
    {
    public:
        DurationType range;
        const InstantSelector * getInstantSelector() const { return &typeid_cast<const InstantSelector &>(*children.at(0)); }
        RangeSelector() { node_type = NodeType::RangeSelector; result_type = ResultType::RANGE_VECTOR; }
        Node * clone(std::vector<std::unique_ptr<Node>> & node_list_) const override;
        String dumpNode(const PrometheusQueryTree & tree, size_t indent) const override;
        String toString(const PrometheusQueryTree & tree) const override;
    };

    /// Represents a subquery, i.e. <expression>[<range>:<step>]. Here step can be omitted, but the colon always presents.
    /// Examples: <expression>[1h:5m]
    ///           <expression>[1h:]
    class Subquery : public Node
    {
    public:
        DurationType range;
        std::optional<DurationType> step;
        const Node * getExpression() const { return children.at(0); }
        Subquery() { node_type = NodeType::Subquery; result_type = ResultType::RANGE_VECTOR; }
        Node * clone(std::vector<std::unique_ptr<Node>> & node_list_) const override;
        String dumpNode(const PrometheusQueryTree & tree, size_t indent) const override;
        String toString(const PrometheusQueryTree & tree) const override;
        int getPrecedence() const override;
    };

    /// Represents a change of the evaluation time applied to an instant selector or a range selector or a subquery.
    /// Examples: <expression> offset 1d
    ///           <expression> @ 1609746000
    ///           <expression> @ 1609746000 offset -1d
    class Offset : public Node
    {
    public:
        std::optional<TimestampType> at_timestamp;
        std::optional<DurationType> offset_value;
        const Node * getExpression() const { return children.at(0); }
        Offset() { node_type = NodeType::Offset; }
        Node * clone(std::vector<std::unique_ptr<Node>> & node_list_) const override;
        String dumpNode(const PrometheusQueryTree & tree, size_t indent) const override;
        String toString(const PrometheusQueryTree & tree) const override;
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
        String dumpNode(const PrometheusQueryTree & tree, size_t indent) const override;
        String toString(const PrometheusQueryTree & tree) const override;
    };

    /// An unary operator: either +<argument> or -<argument>.
    class UnaryOperator : public Node
    {
    public:
        String operator_name;
        const Node * getArgument() const { return children.at(0); }
        UnaryOperator() { node_type = NodeType::UnaryOperator; }
        Node * clone(std::vector<std::unique_ptr<Node>> & node_list_) const override;
        String dumpNode(const PrometheusQueryTree & tree, size_t indent) const override;
        String toString(const PrometheusQueryTree & tree) const override;
        int getPrecedence() const override;
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
        String dumpNode(const PrometheusQueryTree & tree, size_t indent) const override;
        String toString(const PrometheusQueryTree & tree) const override;
        int getPrecedence() const override;
        bool isRightAssociative() const;
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
        String dumpNode(const PrometheusQueryTree & tree, size_t indent) const override;
        String toString(const PrometheusQueryTree & tree) const override;
    };

    PrometheusQueryTree() = default;
    PrometheusQueryTree(const PrometheusQueryTree & src) { *this = src; }
    PrometheusQueryTree(PrometheusQueryTree && src) noexcept { *this = std::move(src); }
    PrometheusQueryTree & operator=(const PrometheusQueryTree & src);
    PrometheusQueryTree & operator=(PrometheusQueryTree && src) noexcept;

    /// Constructs a PrometheusQueryTree from a prepared list of nodes.
    PrometheusQueryTree(std::vector<std::unique_ptr<Node>> node_list_, const Node * root_, UInt32 timestamp_scale_ = 3);
    explicit PrometheusQueryTree(std::unique_ptr<Node> single_node_, UInt32 timestamp_scale_ = 3);

    /// Parses a promql query.
    explicit PrometheusQueryTree(std::string_view promql_query_, UInt32 timestamp_scale_ = 3) { parse(promql_query_, timestamp_scale_); }

    /// Parses a promql query.
    /// This function throws an exception if something is wrong with the syntax.
    void parse(std::string_view promql_query_, UInt32 timestamp_scale_ = 3);

    /// Tries to parse a promql query. Returns true if successful.
    /// If it isn't successful the function sets `error_pos` and `error_message` and returns false.
    bool tryParse(std::string_view promql_query_, UInt32 timestamp_scale_ = 3, String * error_message_ = nullptr, size_t * error_pos_ = nullptr);

    bool empty() const { return node_list.empty(); }
    size_t size() const { return node_list.size(); }

    /// Returns the root node.
    const Node * getRoot() const { return root; }

    /// Returns the promql query which was parsed to build this tree.
    String toString() const;

    /// Returns the type of the query's returning value.
    ResultType getResultType() const;

    /// Returns the scale used for timestamps and durations.
    UInt32 getTimestampScale() const { return timestamp_scale; }

    /// Dumps the tree to string as a tree for debugging purposes.
    String dumpTree() const;

private:
    std::vector<std::unique_ptr<Node>> node_list;
    const Node * root = nullptr;
    UInt32 timestamp_scale = 0;
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
