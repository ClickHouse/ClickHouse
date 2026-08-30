#include <Parsers/Prometheus/prometheusQueryTreeToJSON.h>

#include <Common/Exception.h>
#include <Core/DecimalFunctions.h>
#include <Formats/FormatSettings.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Parsers/Prometheus/PrometheusQueryTree.h>

#include <charconv>
#include <unordered_map>


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
    using NodeType = PrometheusQueryTree::NodeType;
    using ResultType = PrometheusQueryTree::ResultType;
    using Matcher = PrometheusQueryTree::Matcher;
    using MatcherType = PrometheusQueryTree::MatcherType;
    using AtModifier = PrometheusQueryTree::Offset::AtModifier;

    /// The name of a PromQL type in the JSON output: it's also what Prometheus calls a "value type".
    std::string_view resultTypeToValueTypeName(ResultType type)
    {
        switch (type)
        {
            case ResultType::SCALAR: return "scalar";
            case ResultType::STRING: return "string";
            case ResultType::INSTANT_VECTOR: return "vector";
            case ResultType::RANGE_VECTOR: return "matrix";
        }
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown PromQL result type");
    }

    /// The name of a PromQL type in error messages, matching the wording of the Prometheus parser.
    std::string_view resultTypeToDocumentedName(ResultType type)
    {
        switch (type)
        {
            case ResultType::SCALAR: return "scalar";
            case ResultType::STRING: return "string";
            case ResultType::INSTANT_VECTOR: return "instant vector";
            case ResultType::RANGE_VECTOR: return "range vector";
        }
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown PromQL result type");
    }

    /// The signature of a PromQL function as Prometheus defines it (see promql/parser/functions.go).
    struct FunctionSignature
    {
        std::vector<ResultType> arg_types;

        /// 0 means the function takes exactly `arg_types.size()` arguments;
        /// k > 0 means the last k arguments are optional;
        /// -1 means the last argument can be repeated any number of times.
        Int64 variadic = 0;

        ResultType return_type = ResultType::INSTANT_VECTOR;
    };

    const FunctionSignature & getFunctionSignature(const String & function_name)
    {
        static const ResultType scalar = ResultType::SCALAR;
        static const ResultType string = ResultType::STRING;
        static const ResultType vector = ResultType::INSTANT_VECTOR;
        static const ResultType matrix = ResultType::RANGE_VECTOR;

        static const std::unordered_map<std::string_view, FunctionSignature> signatures =
        {
            {"abs", {{vector}, 0, vector}},
            {"absent", {{vector}, 0, vector}},
            {"absent_over_time", {{matrix}, 0, vector}},
            {"acos", {{vector}, 0, vector}},
            {"acosh", {{vector}, 0, vector}},
            {"asin", {{vector}, 0, vector}},
            {"asinh", {{vector}, 0, vector}},
            {"atan", {{vector}, 0, vector}},
            {"atanh", {{vector}, 0, vector}},
            {"avg_over_time", {{matrix}, 0, vector}},
            {"ceil", {{vector}, 0, vector}},
            {"changes", {{matrix}, 0, vector}},
            {"clamp", {{vector, scalar, scalar}, 0, vector}},
            {"clamp_max", {{vector, scalar}, 0, vector}},
            {"clamp_min", {{vector, scalar}, 0, vector}},
            {"cos", {{vector}, 0, vector}},
            {"cosh", {{vector}, 0, vector}},
            {"count_over_time", {{matrix}, 0, vector}},
            {"day_of_month", {{vector}, 1, vector}},
            {"day_of_week", {{vector}, 1, vector}},
            {"day_of_year", {{vector}, 1, vector}},
            {"days_in_month", {{vector}, 1, vector}},
            {"deg", {{vector}, 0, vector}},
            {"delta", {{matrix}, 0, vector}},
            {"deriv", {{matrix}, 0, vector}},
            {"exp", {{vector}, 0, vector}},
            {"floor", {{vector}, 0, vector}},
            {"histogram_count", {{vector}, 0, vector}},
            {"histogram_fraction", {{scalar, scalar, vector}, 0, vector}},
            {"histogram_quantile", {{scalar, vector}, 0, vector}},
            {"histogram_sum", {{vector}, 0, vector}},
            {"holt_winters", {{matrix, scalar, scalar}, 0, vector}},
            {"hour", {{vector}, 1, vector}},
            {"idelta", {{matrix}, 0, vector}},
            {"increase", {{matrix}, 0, vector}},
            {"irate", {{matrix}, 0, vector}},
            {"label_join", {{vector, string, string, string}, -1, vector}},
            {"label_replace", {{vector, string, string, string, string}, 0, vector}},
            {"last_over_time", {{matrix}, 0, vector}},
            {"ln", {{vector}, 0, vector}},
            {"log10", {{vector}, 0, vector}},
            {"log2", {{vector}, 0, vector}},
            {"max_over_time", {{matrix}, 0, vector}},
            {"min_over_time", {{matrix}, 0, vector}},
            {"minute", {{vector}, 1, vector}},
            {"month", {{vector}, 1, vector}},
            {"pi", {{}, 0, scalar}},
            {"predict_linear", {{matrix, scalar}, 0, vector}},
            {"present_over_time", {{matrix}, 0, vector}},
            {"quantile_over_time", {{scalar, matrix}, 0, vector}},
            {"rad", {{vector}, 0, vector}},
            {"rate", {{matrix}, 0, vector}},
            {"resets", {{matrix}, 0, vector}},
            {"round", {{vector, scalar}, 1, vector}},
            {"scalar", {{vector}, 0, scalar}},
            {"sgn", {{vector}, 0, vector}},
            {"sin", {{vector}, 0, vector}},
            {"sinh", {{vector}, 0, vector}},
            {"sort", {{vector}, 0, vector}},
            {"sort_desc", {{vector}, 0, vector}},
            {"sqrt", {{vector}, 0, vector}},
            {"stddev_over_time", {{matrix}, 0, vector}},
            {"stdvar_over_time", {{matrix}, 0, vector}},
            {"sum_over_time", {{matrix}, 0, vector}},
            {"tan", {{vector}, 0, vector}},
            {"tanh", {{vector}, 0, vector}},
            {"time", {{}, 0, scalar}},
            {"timestamp", {{vector}, 0, vector}},
            {"vector", {{scalar}, 0, vector}},
            {"year", {{vector}, 1, vector}},
        };

        auto it = signatures.find(function_name);
        if (it == signatures.end())
            throw Exception(ErrorCodes::CANNOT_PARSE_PROMQL_QUERY, "Unknown function '{}'", function_name);
        return it->second;
    }

    /// Checks the number and the types of the arguments of a function call the same way as the Prometheus parser.
    void checkFunctionCall(const PrometheusQueryTree::Function & function, const FunctionSignature & signature)
    {
        const auto & arguments = function.getArguments();
        const size_t num_arg_types = signature.arg_types.size();

        if (signature.variadic == 0)
        {
            if (arguments.size() != num_arg_types)
                throw Exception(ErrorCodes::CANNOT_PARSE_PROMQL_QUERY,
                                "Expected {} argument(s) in call to '{}', got {}",
                                num_arg_types, function.function_name, arguments.size());
        }
        else
        {
            const size_t min_args = num_arg_types - 1;
            if (arguments.size() < min_args)
                throw Exception(ErrorCodes::CANNOT_PARSE_PROMQL_QUERY,
                                "Expected at least {} argument(s) in call to '{}', got {}",
                                min_args, function.function_name, arguments.size());
            if (signature.variadic > 0 && arguments.size() > min_args + signature.variadic)
                throw Exception(ErrorCodes::CANNOT_PARSE_PROMQL_QUERY,
                                "Expected at most {} argument(s) in call to '{}', got {}",
                                min_args + signature.variadic, function.function_name, arguments.size());
        }

        for (size_t i = 0; i != arguments.size(); ++i)
        {
            /// A repeated last argument must have the type of the last declared argument.
            const auto expected_type = signature.arg_types[std::min(i, num_arg_types - 1)];
            if (arguments[i]->result_type != expected_type)
                throw Exception(ErrorCodes::CANNOT_PARSE_PROMQL_QUERY,
                                "Expected type {} in call to function '{}', got {}",
                                resultTypeToDocumentedName(expected_type), function.function_name,
                                resultTypeToDocumentedName(arguments[i]->result_type));
        }
    }

    /// Returns the type of the parameter an aggregation operator takes before the aggregated vector,
    /// or std::nullopt if it takes no parameter.
    std::optional<ResultType> getAggregationParamType(const String & operator_name)
    {
        if (operator_name == "bottomk" || operator_name == "limitk" || operator_name == "quantile" || operator_name == "topk")
            return ResultType::SCALAR;
        if (operator_name == "count_values")
            return ResultType::STRING;
        return std::nullopt;
    }

    /// Checks the number and the types of the arguments of an aggregation the same way as the Prometheus parser.
    void checkAggregation(const PrometheusQueryTree::AggregationOperator & aggregation)
    {
        const auto & arguments = aggregation.getArguments();
        const auto param_type = getAggregationParamType(aggregation.operator_name);
        const size_t expected_num_arguments = param_type ? 2 : 1;

        if (arguments.size() != expected_num_arguments)
            throw Exception(ErrorCodes::CANNOT_PARSE_PROMQL_QUERY,
                            "Wrong number of arguments for aggregate expression '{}' provided, expected {}, got {}",
                            aggregation.operator_name, expected_num_arguments, arguments.size());

        if (param_type && arguments.front()->result_type != *param_type)
            throw Exception(ErrorCodes::CANNOT_PARSE_PROMQL_QUERY,
                            "Expected type {} in aggregation parameter, got {}",
                            resultTypeToDocumentedName(*param_type), resultTypeToDocumentedName(arguments.front()->result_type));

        if (arguments.back()->result_type != ResultType::INSTANT_VECTOR)
            throw Exception(ErrorCodes::CANNOT_PARSE_PROMQL_QUERY,
                            "Expected type instant vector in aggregation expression, got {}",
                            resultTypeToDocumentedName(arguments.back()->result_type));
    }

    /// Converts a Decimal64 duration or timestamp to whole milliseconds, truncating like Go's Duration.Milliseconds().
    Int64 decimalToMilliseconds(Decimal64 value, UInt32 scale)
    {
        if (scale >= 3)
            return value.value / DecimalUtils::scaleMultiplier<Decimal64::NativeType>(scale - 3);
        return value.value * DecimalUtils::scaleMultiplier<Decimal64::NativeType>(3 - scale);
    }

    /// Converts a Decimal64 timestamp to milliseconds rounding half away from zero,
    /// like `timestamp.FromFloatSeconds` which the Prometheus parser uses for the `@` modifier.
    Int64 decimalToRoundedMilliseconds(Decimal64 value, UInt32 scale)
    {
        Int64 milliseconds = decimalToMilliseconds(value, scale);
        if (scale <= 3)
            return milliseconds;

        const Int64 divisor = DecimalUtils::scaleMultiplier<Decimal64::NativeType>(scale - 3);
        const Int64 remainder = value.value % divisor;
        if (remainder * 2 >= divisor)
            ++milliseconds;
        else if (remainder * 2 <= -divisor)
            --milliseconds;
        return milliseconds;
    }

    /// Formats a number the way Go's strconv.FormatFloat(value, 'f', -1, 64) does, which Prometheus
    /// uses in the "val" field: the shortest decimal representation without an exponent.
    String formatPrometheusNumber(Float64 value)
    {
        if (std::isnan(value))
            return "NaN";
        if (std::isinf(value))
            return value < 0 ? "-Inf" : "+Inf";

        /// Large enough for any Float64 in fixed notation, including denormals.
        char buffer[1100];
        const auto [end, error] = std::to_chars(buffer, buffer + sizeof(buffer), value, std::chars_format::fixed);
        if (error != std::errc{})
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot format number {}", value);
        return {buffer, end};
    }

    /// Serializes the query tree in the format of the `translateAST` function in Prometheus.
    /// The `@` and `offset` modifiers are stored as separate `Offset` nodes in the query tree,
    /// while Prometheus reports them as fields of the modified selector or subquery,
    /// so they are collected into `SelectorModifiers` and passed down to the modified node.
    class JSONWriter
    {
    public:
        JSONWriter(WriteBuffer & out_, UInt32 timestamp_scale_) : out(out_), timestamp_scale(timestamp_scale_) {}

        void writeNode(const Node & node) { writeNode(node, {}); }

    private:
        struct SelectorModifiers
        {
            AtModifier at_modifier = AtModifier::None;
            Int64 at_timestamp = 0;
            Int64 offset = 0;
        };

        WriteBuffer & out;
        UInt32 timestamp_scale;
        const FormatSettings format_settings{};

        void writeFieldName(bool & first, std::string_view name)
        {
            if (!first)
                writeChar(',', out);
            first = false;
            writeJSONString(name, out, format_settings);
            writeChar(':', out);
        }

        void writeStringField(bool & first, std::string_view name, std::string_view value)
        {
            writeFieldName(first, name);
            writeJSONString(value, out, format_settings);
        }

        void writeIntField(bool & first, std::string_view name, Int64 value)
        {
            writeFieldName(first, name);
            writeIntText(value, out);
        }

        void writeBoolField(bool & first, std::string_view name, bool value)
        {
            writeFieldName(first, name);
            writeString(value ? "true" : "false", out);
        }

        void writeNullField(bool & first, std::string_view name)
        {
            writeFieldName(first, name);
            writeString("null", out);
        }

        void writeStringArrayField(bool & first, std::string_view name, const Strings & values)
        {
            writeFieldName(first, name);
            writeChar('[', out);
            for (size_t i = 0; i != values.size(); ++i)
            {
                if (i != 0)
                    writeChar(',', out);
                writeJSONString(values[i], out, format_settings);
            }
            writeChar(']', out);
        }

        static std::string_view matcherTypeToString(MatcherType type)
        {
            switch (type)
            {
                case MatcherType::EQ: return "=";
                case MatcherType::NE: return "!=";
                case MatcherType::RE: return "=~";
                case MatcherType::NRE: return "!~";
            }
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown PromQL matcher type");
        }

        void writeMatcher(const Matcher & matcher)
        {
            writeChar('{', out);
            bool first = true;
            writeStringField(first, "name", matcher.label_name);
            writeStringField(first, "value", matcher.label_value);
            writeStringField(first, "type", matcherTypeToString(matcher.matcher_type));
            writeChar('}', out);
        }

        void writeMatchersField(bool & first, const PrometheusQueryTree::InstantSelector & selector)
        {
            writeFieldName(first, "matchers");
            writeChar('[', out);

            /// When the metric name is written before the braces, the parser stores its matcher first,
            /// while Prometheus appends it after the explicit matchers, so it is moved to the end here.
            const auto & matchers = selector.matchers;
            const size_t begin = selector.metric_name.empty() ? 0 : 1;
            chassert(!begin || (!matchers.empty() && matchers.front().label_name == "__name__"));

            bool first_matcher = true;
            auto write_matcher = [&](const Matcher & matcher)
            {
                if (!first_matcher)
                    writeChar(',', out);
                first_matcher = false;
                writeMatcher(matcher);
            };

            for (size_t i = begin; i != matchers.size(); ++i)
                write_matcher(matchers[i]);
            if (begin)
                write_matcher(matchers.front());

            writeChar(']', out);
        }

        void writeModifiersFields(bool & first, const SelectorModifiers & modifiers)
        {
            writeIntField(first, "offset", modifiers.offset);
            writeNullField(first, "offsetExpr");

            if (modifiers.at_modifier == AtModifier::Timestamp)
                writeIntField(first, "timestamp", modifiers.at_timestamp);
            else
                writeNullField(first, "timestamp");

            if (modifiers.at_modifier == AtModifier::Start)
                writeStringField(first, "startOrEnd", "start");
            else if (modifiers.at_modifier == AtModifier::End)
                writeStringField(first, "startOrEnd", "end");
            else
                writeNullField(first, "startOrEnd");
        }

        void writeVectorSelector(const PrometheusQueryTree::InstantSelector & selector, const SelectorModifiers & modifiers)
        {
            writeChar('{', out);
            bool first = true;
            writeStringField(first, "type", "vectorSelector");
            writeStringField(first, "name", selector.metric_name);
            writeMatchersField(first, selector);
            writeModifiersFields(first, modifiers);
            writeBoolField(first, "anchored", false);
            writeBoolField(first, "smoothed", false);
            writeChar('}', out);
        }

        void writeMatrixSelector(const PrometheusQueryTree::RangeSelector & range_selector, const SelectorModifiers & modifiers)
        {
            const auto & selector = *range_selector.getInstantSelector();
            writeChar('{', out);
            bool first = true;
            writeStringField(first, "type", "matrixSelector");
            writeStringField(first, "name", selector.metric_name);
            writeIntField(first, "range", decimalToMilliseconds(range_selector.range, timestamp_scale));
            writeNullField(first, "rangeExpr");
            writeMatchersField(first, selector);
            writeModifiersFields(first, modifiers);
            writeBoolField(first, "anchored", false);
            writeBoolField(first, "smoothed", false);
            writeChar('}', out);
        }

        void writeSubquery(const PrometheusQueryTree::Subquery & subquery, const SelectorModifiers & modifiers)
        {
            writeChar('{', out);
            bool first = true;
            writeStringField(first, "type", "subquery");
            writeFieldName(first, "expr");
            writeNode(*subquery.getExpression(), {});
            writeIntField(first, "range", decimalToMilliseconds(subquery.range, timestamp_scale));
            writeNullField(first, "rangeExpr");
            writeIntField(first, "step", subquery.step ? decimalToMilliseconds(*subquery.step, timestamp_scale) : 0);
            writeNullField(first, "stepExpr");
            writeModifiersFields(first, modifiers);
            writeChar('}', out);
        }

        void writeFunction(const PrometheusQueryTree::Function & function)
        {
            const auto & signature = getFunctionSignature(function.function_name);
            checkFunctionCall(function, signature);

            writeChar('{', out);
            bool first = true;
            writeStringField(first, "type", "call");

            writeFieldName(first, "func");
            writeChar('{', out);
            bool func_first = true;
            writeStringField(func_first, "name", function.function_name);
            writeFieldName(func_first, "argTypes");
            writeChar('[', out);
            for (size_t i = 0; i != signature.arg_types.size(); ++i)
            {
                if (i != 0)
                    writeChar(',', out);
                writeJSONString(resultTypeToValueTypeName(signature.arg_types[i]), out, format_settings);
            }
            writeChar(']', out);
            writeIntField(func_first, "variadic", signature.variadic);
            writeStringField(func_first, "returnType", resultTypeToValueTypeName(signature.return_type));
            writeChar('}', out);

            writeFieldName(first, "args");
            writeChar('[', out);
            const auto & arguments = function.getArguments();
            for (size_t i = 0; i != arguments.size(); ++i)
            {
                if (i != 0)
                    writeChar(',', out);
                writeNode(*arguments[i], {});
            }
            writeChar(']', out);
            writeChar('}', out);
        }

        void writeBinaryOperator(const PrometheusQueryTree::BinaryOperator & binary)
        {
            writeChar('{', out);
            bool first = true;
            writeStringField(first, "type", "binaryExpr");
            writeStringField(first, "op", binary.operator_name);
            writeFieldName(first, "lhs");
            writeNode(*binary.getLeftArgument(), {});
            writeFieldName(first, "rhs");
            writeNode(*binary.getRightArgument(), {});
            writeMatchingField(first, binary);
            writeBoolField(first, "bool", binary.bool_modifier);
            writeChar('}', out);
        }

        void writeMatchingField(bool & first, const PrometheusQueryTree::BinaryOperator & binary)
        {
            writeFieldName(first, "matching");

            /// The Prometheus parser drops the vector matching unless both operands are instant vectors.
            const bool both_vectors = binary.getLeftArgument()->result_type == ResultType::INSTANT_VECTOR
                && binary.getRightArgument()->result_type == ResultType::INSTANT_VECTOR;
            if (!both_vectors)
            {
                writeString("null", out);
                return;
            }

            const bool is_set_operator
                = binary.operator_name == "and" || binary.operator_name == "or" || binary.operator_name == "unless";

            std::string_view cardinality = "one-to-one";
            if (binary.group_left)
                cardinality = "many-to-one";
            else if (binary.group_right)
                cardinality = "one-to-many";
            else if (is_set_operator)
                cardinality = "many-to-many";

            writeChar('{', out);
            bool matching_first = true;
            writeStringField(matching_first, "card", cardinality);
            writeStringArrayField(matching_first, "labels", binary.labels);
            writeBoolField(matching_first, "on", binary.on);
            writeStringArrayField(matching_first, "include", binary.extra_labels);

            writeFieldName(matching_first, "fillValues");
            writeChar('{', out);
            bool fill_first = true;
            writeNullField(fill_first, "lhs");
            writeNullField(fill_first, "rhs");
            writeChar('}', out);

            writeChar('}', out);
        }

        void writeAggregation(const PrometheusQueryTree::AggregationOperator & aggregation)
        {
            checkAggregation(aggregation);

            /// After checkAggregation the arguments are either (vector) or (param, vector).
            const auto & arguments = aggregation.getArguments();

            writeChar('{', out);
            bool first = true;
            writeStringField(first, "type", "aggregation");
            writeStringField(first, "op", aggregation.operator_name);
            writeFieldName(first, "expr");
            writeNode(*arguments.back(), {});
            writeFieldName(first, "param");
            if (arguments.size() == 2)
                writeNode(*arguments.front(), {});
            else
                writeString("null", out);
            writeStringArrayField(first, "grouping", aggregation.labels);
            writeBoolField(first, "without", aggregation.without);
            writeChar('}', out);
        }

        void writeNode(const Node & node, const SelectorModifiers & modifiers)
        {
            switch (node.node_type)
            {
                case NodeType::Scalar:
                {
                    const auto & scalar = typeid_cast<const PrometheusQueryTree::Scalar &>(node);
                    writeChar('{', out);
                    bool first = true;
                    writeStringField(first, "type", "numberLiteral");
                    writeStringField(first, "val", formatPrometheusNumber(scalar.scalar));
                    writeChar('}', out);
                    return;
                }
                case NodeType::StringLiteral:
                {
                    const auto & string_literal = typeid_cast<const PrometheusQueryTree::StringLiteral &>(node);
                    writeChar('{', out);
                    bool first = true;
                    writeStringField(first, "type", "stringLiteral");
                    writeStringField(first, "val", string_literal.string);
                    writeChar('}', out);
                    return;
                }
                case NodeType::InstantSelector:
                {
                    writeVectorSelector(typeid_cast<const PrometheusQueryTree::InstantSelector &>(node), modifiers);
                    return;
                }
                case NodeType::RangeSelector:
                {
                    writeMatrixSelector(typeid_cast<const PrometheusQueryTree::RangeSelector &>(node), modifiers);
                    return;
                }
                case NodeType::Subquery:
                {
                    writeSubquery(typeid_cast<const PrometheusQueryTree::Subquery &>(node), modifiers);
                    return;
                }
                case NodeType::Offset:
                {
                    const auto & offset = typeid_cast<const PrometheusQueryTree::Offset &>(node);
                    SelectorModifiers new_modifiers = modifiers;
                    new_modifiers.at_modifier = offset.at_modifier;
                    if (offset.at_timestamp)
                        new_modifiers.at_timestamp = decimalToRoundedMilliseconds(Decimal64{*offset.at_timestamp}, timestamp_scale);
                    if (offset.offset_value)
                        new_modifiers.offset = decimalToMilliseconds(*offset.offset_value, timestamp_scale);
                    writeNode(*offset.getExpression(), new_modifiers);
                    return;
                }
                case NodeType::Function:
                {
                    writeFunction(typeid_cast<const PrometheusQueryTree::Function &>(node));
                    return;
                }
                case NodeType::UnaryOperator:
                {
                    const auto & unary = typeid_cast<const PrometheusQueryTree::UnaryOperator &>(node);

                    /// The Prometheus parser folds an unary +/- applied to a number literal into the literal itself.
                    if (const auto * scalar = typeid_cast<const PrometheusQueryTree::Scalar *>(unary.getArgument()))
                    {
                        const Float64 value = (unary.operator_name == "-") ? -scalar->scalar : scalar->scalar;
                        writeChar('{', out);
                        bool first = true;
                        writeStringField(first, "type", "numberLiteral");
                        writeStringField(first, "val", formatPrometheusNumber(value));
                        writeChar('}', out);
                        return;
                    }

                    writeChar('{', out);
                    bool first = true;
                    writeStringField(first, "type", "unaryExpr");
                    writeStringField(first, "op", unary.operator_name);
                    writeFieldName(first, "expr");
                    writeNode(*unary.getArgument(), {});
                    writeChar('}', out);
                    return;
                }
                case NodeType::BinaryOperator:
                {
                    writeBinaryOperator(typeid_cast<const PrometheusQueryTree::BinaryOperator &>(node));
                    return;
                }
                case NodeType::AggregationOperator:
                {
                    writeAggregation(typeid_cast<const PrometheusQueryTree::AggregationOperator &>(node));
                    return;
                }
            }
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown PromQL query node type");
        }
    };
}


String prometheusQueryTreeToJSON(const PrometheusQueryTree & promql_query)
{
    WriteBufferFromOwnString out;
    if (const auto * root = promql_query.getRoot())
    {
        JSONWriter writer{out, promql_query.getTimestampScale()};
        writer.writeNode(*root);
    }
    else
    {
        writeString("null", out);
    }
    out.finalize();
    return out.str();
}

}
