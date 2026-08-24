#include <Parsers/Prometheus/PrometheusQueryParsingUtil.h>

#include <Common/Exception.h>

#include "config.h"

#if USE_ANTLR4_GRAMMARS
#include <Parsers/Prometheus/PrometheusQueryTree.h>

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdocumentation"
#pragma clang diagnostic ignored "-Wdocumentation-deprecated-sync"
#pragma clang diagnostic ignored "-Wdocumentation-html"
#pragma clang diagnostic ignored "-Wextra-semi"
#pragma clang diagnostic ignored "-Winconsistent-missing-destructor-override"
#pragma clang diagnostic ignored "-Wshadow-field"
#pragma clang diagnostic ignored "-Wshadow-field-in-constructor"
#pragma clang diagnostic ignored "-Wsuggest-destructor-override"
#include <antlr4_grammars/PromQLLexer.h>
#include <antlr4_grammars/PromQLParser.h>
#include <antlr4_grammars/PromQLParserBaseVisitor.h>
#pragma clang diagnostic pop
#endif


namespace DB
{

namespace ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
    extern const int LOGICAL_ERROR;
}

#if USE_ANTLR4_GRAMMARS

namespace
{
    using ScalarType = PrometheusQueryTree::ScalarType;
    using TimestampType = PrometheusQueryTree::TimestampType;
    using DurationType = PrometheusQueryTree::DurationType;
    using ResultType = PrometheusQueryResultType;
    using Node = PrometheusQueryTree::Node;

    /// Handles errors while a promql query is parsed.
    class ErrorListener : public antlr4::BaseErrorListener
    {
    public:
        explicit ErrorListener(std::string_view promql_query_) : promql_query(promql_query_) {}

        void setError(const String & error_message_, size_t error_pos_)
        {
            /// Only the first error is interesting.
            if (!hasError() && !error_message_.empty())
            {
                error_pos = error_pos_;
                error_message = error_message_;
            }
        }

        bool hasError() const { return !error_message.empty(); }
        size_t getErrorPos() const { return error_pos; }
        const String & getErrorMessage() const { return error_message; }

    protected:
        void syntaxError(antlr4::Recognizer * /* recognizer */, antlr4::Token * offending_symbol,
            size_t line, size_t position_in_line, const std::string & msg, std::exception_ptr /* exception */) override
        {
            chassert(!msg.empty());

            size_t pos;
            if (offending_symbol)
                pos = offending_symbol->getStartIndex();
            else  /// `offending_symbol` can be null if `recognizer` is a lexer.
                pos = convertLineAndPositionInLine(line, position_in_line);

            setError(msg, pos);
        }

        /// ANTLR4's lexer returns the position of an error as a line number and a position in that line;
        /// we need to convert them to a char index.
        size_t convertLineAndPositionInLine(size_t line, size_t position_in_line) const
        {
            size_t char_index = 0;
            if (line != 1)
            {
                size_t cur_line = 1;
                while (char_index != promql_query.length())
                {
                    char c = promql_query[char_index++];
                    /// ANTLR4 considers only '\n' as end-of-line (see LexerATNSimulator::consume()).
                    if (c == '\n')
                    {
                        if (++cur_line == line)
                            break;
                    }
                }
            }
            return std::max(char_index + position_in_line, promql_query.length());
        }

    private:
        std::string_view promql_query;
        size_t error_pos = String::npos;
        String error_message;
    };

    [[noreturn]] void throwInconsistentSchema(std::string_view context_name, std::string_view token)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Schema '{}' is inconsistent with {}", context_name, token);
    }

    /// Helps to build a PrometheusQueryTree.
    class PrometheusQueryTreeBuilder : public antlr4_grammars::PromQLParserBaseVisitor
    {
    public:
        explicit PrometheusQueryTreeBuilder(std::string_view promql_query_, UInt32 timestamp_scale_, ErrorListener & error_listener_)
            : promql_query(promql_query_), timestamp_scale(timestamp_scale_), error_listener(error_listener_) {}

        Node * makeNode(antlr4::ParserRuleContext * expression)
        {
            std::any any = visit(expression);
            if (Node * node = anyToNodePtr(any))
                return node;

            chassert(error_listener.hasError());
            return nullptr;
        }

        std::vector<std::unique_ptr<Node>> extractNodes() { return std::exchange(nodes, {}); }

    private:
        std::string_view promql_query;
        UInt32 timestamp_scale;
        ErrorListener & error_listener;
        std::vector<std::unique_ptr<Node>> nodes;

        Node * addNode(std::unique_ptr<Node> new_node)
        {
            return nodes.emplace_back(std::move(new_node)).get();
        }

        void addChild(Node * parent, Node * new_child)
        {
            chassert(!new_child->parent);
            new_child->parent = parent;
            parent->children.push_back(new_child);
        }

        static String getText(const antlr4::tree::TerminalNode * ctx) { return ctx->getSymbol()->getText(); }

        static size_t getStartPos(const antlr4::tree::TerminalNode * ctx) { return ctx->getSymbol()->getStartIndex(); }

        bool parseStringLiteral(const antlr4::tree::TerminalNode * ctx, String & result)
        {
            String error_message;
            size_t error_pos;
            if (!PrometheusQueryParsingUtil::tryParseStringLiteral(getText(ctx), result, &error_message, &error_pos))
            {
                error_listener.setError(error_message, error_pos + getStartPos(ctx));
                return false;
            }
            return true;
        }

        bool parseScalar(const antlr4::tree::TerminalNode * ctx, ScalarType & result)
        {
            String error_message;
            size_t error_pos;
            if (!PrometheusQueryParsingUtil::tryParseScalar(getText(ctx), result, &error_message, &error_pos))
            {
                error_listener.setError(error_message, error_pos + getStartPos(ctx));
                return false;
            }
            return true;
        }

        bool parseTimestamp(const antlr4::tree::TerminalNode * ctx, TimestampType & result)
        {
            String error_message;
            size_t error_pos;
            if (!PrometheusQueryParsingUtil::tryParseTimestamp(getText(ctx), timestamp_scale, result, &error_message, &error_pos))
            {
                error_listener.setError(error_message, error_pos + getStartPos(ctx));
                return false;
            }
            return true;
        }

        bool parseDuration(const antlr4::tree::TerminalNode * ctx, DurationType & result)
        {
            String error_message;
            size_t error_pos;
            if (!PrometheusQueryParsingUtil::tryParseDuration(getText(ctx), timestamp_scale, result, &error_message, &error_pos))
            {
                error_listener.setError(error_message, error_pos + getStartPos(ctx));
                return false;
            }
            return true;
        }

        bool parseSelectorRange(const antlr4::tree::TerminalNode * ctx, DurationType & res_range)
        {
            String error_message;
            size_t error_pos;
            if (!PrometheusQueryParsingUtil::tryParseSelectorRange(getText(ctx), timestamp_scale, res_range, &error_message, &error_pos))
            {
                error_listener.setError(error_message, error_pos + getStartPos(ctx));
                return false;
            }
            return true;
        }

        bool parseSubqueryRange(const antlr4::tree::TerminalNode * ctx, DurationType & res_range, std::optional<DurationType> & res_step)
        {
            String error_message;
            size_t error_pos;
            if (!PrometheusQueryParsingUtil::tryParseSubqueryRange(getText(ctx), timestamp_scale, res_range, res_step, &error_message, &error_pos))
            {
                error_listener.setError(error_message, error_pos + getStartPos(ctx));
                return false;
            }
            return true;
        }

        using Matcher = PrometheusQueryTree::Matcher;
        using MatcherType = PrometheusQueryTree::MatcherType;
        using MatcherList = PrometheusQueryTree::MatcherList;
        using Scalar = PrometheusQueryTree::Scalar;
        using StringLiteral = PrometheusQueryTree::StringLiteral;
        using InstantSelector = PrometheusQueryTree::InstantSelector;
        using RangeSelector = PrometheusQueryTree::RangeSelector;
        using Subquery = PrometheusQueryTree::Subquery;
        using Offset = PrometheusQueryTree::Offset;
        using Function = PrometheusQueryTree::Function;
        using UnaryOperator = PrometheusQueryTree::UnaryOperator;
        using BinaryOperator = PrometheusQueryTree::BinaryOperator;
        using AggregationOperator = PrometheusQueryTree::AggregationOperator;

        /// Makes a node for a string literal after unquoting and unescaping it.
        Node * makeStringLiteral(antlr4::tree::TerminalNode * ctx)
        {
            auto new_node = std::make_unique<StringLiteral>();
            if (!parseStringLiteral(ctx, new_node->string))
            {
                chassert(error_listener.hasError());
                return nullptr;
            }
            return addNode(std::move(new_node));
        }

        /// Makes a node for a scalar or an interval literal after parsing it.
        Node * makeScalar(antlr4::tree::TerminalNode * ctx)
        {
            ScalarType scalar;
            if (!parseScalar(ctx, scalar))
            {
                chassert(error_listener.hasError());
                return nullptr;
            }
            auto new_node = std::make_unique<Scalar>();
            new_node->scalar = scalar;
            return addNode(std::move(new_node));
        }

        /// Extracts a metric name.
        String getMetricName(antlr4_grammars::PromQLParser::MetricNameContext * ctx) const { return ctx->getText(); }

        /// Extracts a label name.
        String getLabelName(antlr4_grammars::PromQLParser::LabelNameContext * ctx) const { return ctx->getText(); }

        /// Extracts multiple label names separated by comma.
        Strings getLabelNameList(antlr4_grammars::PromQLParser::LabelNameListContext * ctx) const
        {
            Strings label_name_list;

            antlr4_grammars::PromQLParser::LabelNameContext * label_name_ctx = nullptr;
            for (size_t i = 0; (label_name_ctx = ctx->labelName(i)) != nullptr; ++i)
                label_name_list.push_back(getLabelName(label_name_ctx));

            return label_name_list;
        }

        /// Extracts a matcher.
        bool getMatcher(antlr4_grammars::PromQLParser::LabelMatcherContext * ctx, Matcher & res_matcher)
        {
            auto * label_name_ctx = ctx->labelName();
            auto * label_value_ctx = ctx->STRING();
            auto * op_ctx = ctx->labelMatcherOperator();
            if (!label_name_ctx || !label_value_ctx || !op_ctx)
                throwInconsistentSchema("LabelMatcher", ctx->getText());

            res_matcher.label_name = getLabelName(label_name_ctx);

            MatcherType matcher_type;
            if (op_ctx->EQ())
                matcher_type = MatcherType::EQ;
            else if (op_ctx->NE())
                matcher_type = MatcherType::NE;
            else if (op_ctx->RE())
                matcher_type = MatcherType::RE;
            else if (op_ctx->NRE())
                matcher_type = MatcherType::NRE;
            else
                throwInconsistentSchema("LabelMatcher", ctx->getText());

            res_matcher.matcher_type = matcher_type;

            if (!parseStringLiteral(label_value_ctx, res_matcher.label_value))
            {
                chassert(error_listener.hasError());
                return false;
            }

            return true;
        }

        Matcher getMatcherForMetricName(antlr4_grammars::PromQLParser::MetricNameContext * ctx)
        {
            Matcher matcher;
            matcher.label_name = "__name__";
            matcher.label_value = getMetricName(ctx);
            matcher.matcher_type = MatcherType::EQ;
            return matcher;
        }

        /// Makes a node for an instant selector.
        Node * makeInstantSelector(antlr4_grammars::PromQLParser::InstantSelectorContext * ctx)
        {
            auto new_node = std::make_unique<InstantSelector>();

            MatcherList matchers;
            if (auto * metric_name_ctx = ctx->metricName())
                matchers.push_back(getMatcherForMetricName(metric_name_ctx));

            if (auto * label_matcher_list_ctx = ctx->labelMatcherList())
            {
                antlr4_grammars::PromQLParser::LabelMatcherContext * label_matcher_ctx = nullptr;
                for (size_t i = 0; (label_matcher_ctx = label_matcher_list_ctx->labelMatcher(i)) != nullptr; ++i)
                {
                    Matcher matcher;
                    if (!getMatcher(label_matcher_ctx, matcher))
                    {
                        chassert(error_listener.hasError());
                        return nullptr;
                    }
                    matchers.push_back(std::move(matcher));
                }
            }

            new_node->matchers = std::move(matchers);
            return addNode(std::move(new_node));
        }

        /// Makes a node for a range selector.
        Node * makeRangeSelector(antlr4_grammars::PromQLParser::RangeSelectorContext * ctx)
        {
            auto new_node = std::make_unique<RangeSelector>();
            auto * instant_selector_ctx = ctx->instantSelector();
            auto * selector_range_ctx = ctx->SELECTOR_RANGE();
            if (!instant_selector_ctx || !selector_range_ctx)
                throwInconsistentSchema("RangeSelector", ctx->getText());

            auto * instant_selector = makeInstantSelector(instant_selector_ctx);

            if (!instant_selector || !parseSelectorRange(selector_range_ctx, new_node->range))
            {
                chassert(error_listener.hasError());
                return nullptr;
            }

            addChild(new_node.get(), instant_selector);
            return addNode(std::move(new_node));
        }

        /// Makes a node for a subquery operator.
        Node * makeSubquery(antlr4_grammars::PromQLParser::SubqueryOpContext * ctx, Node * expression)
        {
            auto new_node = std::make_unique<Subquery>();
            auto * subquery_range_ctx = ctx->SUBQUERY_RANGE();
            if (!subquery_range_ctx)
                throwInconsistentSchema("SubqueryOp", ctx->getText());

            if (!parseSubqueryRange(subquery_range_ctx, new_node->range, new_node->step))
            {
                chassert(error_listener.hasError());
                return nullptr;
            }

            addChild(new_node.get(), expression);

            auto * res_node = addNode(std::move(new_node));

            if (auto * offset_op_ctx = ctx->offsetOp())
                res_node = makeOffset(offset_op_ctx, res_node);

            return res_node;
        }

        /// Makes a node for [@ timestamp][offset <offset>],
        Node * makeOffset(antlr4_grammars::PromQLParser::OffsetOpContext * ctx, Node * expression)
        {
            auto new_node = std::make_unique<Offset>();
            new_node->result_type = expression->result_type;

            bool ok = true;

            if (auto * timestamp_ctx = ctx->timestamp())
            {
                auto * number_ctx = timestamp_ctx->NUMBER();
                if (!number_ctx)
                    throwInconsistentSchema("OffsetOp", ctx->getText());
                auto & timestamp = new_node->at_timestamp.emplace();
                ok &= parseTimestamp(number_ctx, timestamp);
            }

            if (auto * offset_value_ctx = ctx->offsetValue())
            {
                auto * number_ctx = offset_value_ctx->NUMBER();
                if (!number_ctx)
                    throwInconsistentSchema("OffsetOp", ctx->getText());
                auto & offset_value = new_node->offset_value.emplace();
                ok &= parseDuration(number_ctx, offset_value);
                if (ok && offset_value_ctx->SUB())
                    offset_value = -offset_value;
            }

            if (!ok)
            {
                chassert(error_listener.hasError());
                return nullptr;
            }

            addChild(new_node.get(), expression);
            return addNode(std::move(new_node));
        }

        /// Makes a node for an unary operation.
        Node * makeUnaryOperator(std::string_view operator_name, Node * argument)
        {
            auto new_node = std::make_unique<UnaryOperator>();
            new_node->result_type = argument->result_type;
            new_node->operator_name = operator_name;
            addChild(new_node.get(), argument);
            return addNode(std::move(new_node));
        }

        Node * makeUnaryOperator(antlr4_grammars::PromQLParser::UnaryOpContext * ctx, Node * argument)
        {
            std::string_view operator_name;
            if (ctx->ADD())
                operator_name = "+";
            else if (ctx->SUB())
                operator_name = "-";
            else
                throwInconsistentSchema("UnaryOp", ctx->getText());

            return makeUnaryOperator(operator_name, argument);
        }

        /// Makes a node for a binary operation.
        Node * makeBinaryOperator(std::string_view operator_name, Node * left_argument, Node * right_argument,
                                  antlr4_grammars::PromQLParser::GroupingContext * grouping, bool bool_modifier)
        {
            auto new_node = std::make_unique<BinaryOperator>();
            new_node->operator_name = operator_name;
            new_node->result_type = getBinaryOperatorResultType(left_argument->result_type, right_argument->result_type);

            new_node->children.reserve(2);
            addChild(new_node.get(), left_argument);
            addChild(new_node.get(), right_argument);

            if (grouping)
            {
                if (auto * on_ctx = grouping->on_())
                {
                    auto * labels_ctx = on_ctx->labelNameList();
                    if (!labels_ctx)
                        throwInconsistentSchema("Grouping", grouping->getText());
                    new_node->on = true;
                    new_node->labels = getLabelNameList(labels_ctx);
                }
                else if (auto * ignoring_ctx = grouping->ignoring())
                {
                    auto * labels_ctx = ignoring_ctx->labelNameList();
                    if (!labels_ctx)
                        throwInconsistentSchema("Grouping", grouping->getText());
                    new_node->ignoring = true;
                    new_node->labels = getLabelNameList(labels_ctx);
                }
                if (auto * group_left_ctx = grouping->groupLeft())
                {
                    new_node->group_left = true;
                    if (auto * extra_labels_ctx = group_left_ctx->labelNameList())
                        new_node->extra_labels = getLabelNameList(extra_labels_ctx);
                }
                else if (auto * group_right_ctx = grouping->groupRight())
                {
                    new_node->group_right = true;
                    if (auto * extra_labels_ctx = group_right_ctx->labelNameList())
                        new_node->extra_labels = getLabelNameList(extra_labels_ctx);
                }
            }
            new_node->bool_modifier = bool_modifier;

            return addNode(std::move(new_node));
        }

        Node * makeBinaryOperator(antlr4_grammars::PromQLParser::PowOpContext * ctx, Node * left_argument, Node * right_argument)
        {
            return makeBinaryOperator("^", left_argument, right_argument, ctx->grouping(), /* bool_modifier = */ false);
        }

        Node * makeBinaryOperator(antlr4_grammars::PromQLParser::MultOpContext * ctx, Node * left_argument, Node * right_argument)
        {
            std::string_view operator_name;
            if (ctx->MULT())
                operator_name = "*";
            else if (ctx->DIV())
                operator_name = "/";
            else if (ctx->MOD())
                operator_name = "%";
            else
                throwInconsistentSchema("MultOp", ctx->getText());

            return makeBinaryOperator(operator_name, left_argument, right_argument, ctx->grouping(), /* bool_modifier = */ false);
        }

        Node * makeBinaryOperator(antlr4_grammars::PromQLParser::AddOpContext * ctx, Node * left_argument, Node * right_argument)
        {
            std::string_view operator_name;
            if (ctx->ADD())
                operator_name = "+";
            else if (ctx->SUB())
                operator_name = "-";
            else
                throwInconsistentSchema("AddOp", ctx->getText());

            return makeBinaryOperator(operator_name, left_argument, right_argument, ctx->grouping(), /* bool_modifier = */ false);
        }

        Node * makeBinaryOperator(antlr4_grammars::PromQLParser::CompareOpContext * ctx, Node * left_argument, Node * right_argument)
        {
            std::string_view operator_name;
            if (ctx->DEQ())
                operator_name = "==";
            else if (ctx->NE())
                operator_name = "!=";
            else if (ctx->GT())
                operator_name = ">";
            else if (ctx->LT())
                operator_name = "<";
            else if (ctx->GE())
                operator_name = ">=";
            else if (ctx->LE())
                operator_name = "<=";
            else
                throwInconsistentSchema("CompareOp", ctx->getText());

            bool bool_modifier = (ctx->BOOL() != nullptr);
            return makeBinaryOperator(operator_name, left_argument, right_argument, ctx->grouping(), bool_modifier);
        }

        Node * makeBinaryOperator(antlr4_grammars::PromQLParser::OrOpContext * ctx, Node * left_argument, Node * right_argument)
        {
            return makeBinaryOperator("or", left_argument, right_argument, ctx->grouping(), /* bool_modifier = */ false);
        }

        Node * makeBinaryOperator(antlr4_grammars::PromQLParser::AndUnlessOpContext * ctx, Node * left_argument, Node * right_argument)
        {
            std::string_view operator_name;
            if (ctx->AND())
                operator_name = "and";
            else if (ctx->UNLESS())
                operator_name = "unless";
            else
                throwInconsistentSchema("AndUnlessOp", ctx->getText());

            return makeBinaryOperator(operator_name, left_argument, right_argument, ctx->grouping(), /* bool_modifier = */ false);
        }

        /// Returns the result type of a binary operator.
        ResultType getBinaryOperatorResultType(ResultType left_argument_type, ResultType right_argument_type)
        {
            if ((left_argument_type == ResultType::SCALAR) && (right_argument_type == ResultType::SCALAR))
                return ResultType::SCALAR;
            else
                return ResultType::INSTANT_VECTOR;
        }

        /// Makes a node to call a function.
        Node * makeFunction(std::string_view function_name, const std::vector<Node *> & arguments)
        {
            auto new_node = std::make_unique<Function>();
            new_node->function_name = function_name;
            new_node->result_type = getFunctionResultType(function_name);

            new_node->children.reserve(arguments.size());
            for (auto * argument : arguments)
                addChild(new_node.get(), argument);

            return addNode(std::move(new_node));
        }

        Node * makeFunction(antlr4_grammars::PromQLParser::Function_Context * ctx, const std::vector<Node *> & arguments)
        {
            auto * function_name_ctx = ctx->FUNCTION();
            if (!function_name_ctx)
                throwInconsistentSchema("Function", ctx->getText());

            auto function_name = getText(function_name_ctx);
            return makeFunction(function_name, arguments);
        }

        /// Returns the result type of a function.
        ResultType getFunctionResultType(std::string_view function_name)
        {
            if (function_name == "scalar" || function_name == "time" || function_name == "pi")
                return ResultType::SCALAR;
            else
                return ResultType::INSTANT_VECTOR;
        }

        /// Makes a node for an aggregation operator.
        Node * makeAggregationOperator(std::string_view operator_name, const std::vector<Node *> & arguments,
                                       antlr4_grammars::PromQLParser::ByContext * by,
                                       antlr4_grammars::PromQLParser::WithoutContext * without)
        {
            auto new_node = std::make_unique<AggregationOperator>();
            new_node->operator_name = operator_name;
            new_node->result_type = ResultType::INSTANT_VECTOR;
            if (by)
            {
                new_node->by = true;
                auto * labels_ctx = by->labelNameList();
                if (!labels_ctx)
                    throwInconsistentSchema("By", by->getText());
                new_node->labels = getLabelNameList(labels_ctx);
            }
            else if (without)
            {
                new_node->without = true;
                auto * labels_ctx = without->labelNameList();
                if (!labels_ctx)
                    throwInconsistentSchema("Without", without->getText());
                new_node->labels = getLabelNameList(labels_ctx);
            }

            new_node->children.reserve(arguments.size());
            for (auto * argument : arguments)
                addChild(new_node.get(), argument);

            return addNode(std::move(new_node));
        }

        Node * makeAggregationOperator(antlr4_grammars::PromQLParser::AggregationContext * ctx, const std::vector<Node *> & arguments)
        {
            auto * operator_name_ctx = ctx->AGGREGATION_OPERATOR();
            if (!operator_name_ctx)
                throwInconsistentSchema("Aggregation", ctx->getText());

            auto operator_name = getText(operator_name_ctx);
            return makeAggregationOperator(operator_name, arguments, ctx->by(), ctx->without());
        }

        /// ANTLR visitors:
        std::any visitLiteral(antlr4_grammars::PromQLParser::LiteralContext * ctx) override
        {
            if (auto * number_ctx = ctx->NUMBER())
                return makeScalar(number_ctx);
            else if (auto * string_ctx = ctx->STRING())
                return makeStringLiteral(string_ctx);
            else
                throwInconsistentSchema("Literal", ctx->getText());
        }

        std::any visitInstantSelector(antlr4_grammars::PromQLParser::InstantSelectorContext * ctx) override
        {
            return makeInstantSelector(ctx);
        }

        std::any visitRangeSelector(antlr4_grammars::PromQLParser::RangeSelectorContext * ctx) override
        {
            return makeRangeSelector(ctx);
        }

        std::any visitSelectorWithOffset(antlr4_grammars::PromQLParser::SelectorWithOffsetContext * ctx) override
        {
            Node * res_node = nullptr;
            if (auto * instant_selector_ctx = ctx->instantSelector())
                res_node = makeInstantSelector(instant_selector_ctx);
            else if (auto * range_selector_ctx = ctx->rangeSelector())
                res_node = makeRangeSelector(range_selector_ctx);
            else
                throwInconsistentSchema("Offset", ctx->getText());

            if (!res_node)
            {
                chassert(error_listener.hasError());
                return nullptr;
            }

            auto * offset_op_ctx = ctx->offsetOp();
            if (!offset_op_ctx)
                throwInconsistentSchema("Offset", ctx->getText());

            res_node = makeOffset(offset_op_ctx, res_node);
            return res_node;
        }

        std::any visitVectorOperation(antlr4_grammars::PromQLParser::VectorOperationContext * ctx) override
        {
            if (auto * unary_ctx = ctx->unaryOp())
            {
                auto * argument = makeNode(ctx->vectorOperation(0));
                if (!argument)
                {
                    chassert(error_listener.hasError());
                    return {};
                }
                return makeUnaryOperator(unary_ctx, argument);
            }
            else if (auto * pow_ctx = ctx->powOp())
            {
                auto * left_argument = makeNode(ctx->vectorOperation(0));
                auto * right_argument = makeNode(ctx->vectorOperation(1));
                if (!left_argument || !right_argument)
                {
                    chassert(error_listener.hasError());
                    return {};
                }
                return makeBinaryOperator(pow_ctx, left_argument, right_argument);
            }
            else if (auto * mult_ctx = ctx->multOp())
            {
                auto * left_argument = makeNode(ctx->vectorOperation(0));
                auto * right_argument = makeNode(ctx->vectorOperation(1));
                if (!left_argument || !right_argument)
                {
                    chassert(error_listener.hasError());
                    return {};
                }
                return makeBinaryOperator(mult_ctx, left_argument, right_argument);
            }
            else if (auto * add_ctx = ctx->addOp())
            {
                auto * left_argument = makeNode(ctx->vectorOperation(0));
                auto * right_argument = makeNode(ctx->vectorOperation(1));
                if (!left_argument || !right_argument)
                {
                    chassert(error_listener.hasError());
                    return {};
                }
                return makeBinaryOperator(add_ctx, left_argument, right_argument);
            }
            else if (auto * compare_ctx = ctx->compareOp())
            {
                auto * left_argument = makeNode(ctx->vectorOperation(0));
                auto * right_argument = makeNode(ctx->vectorOperation(1));
                if (!left_argument || !right_argument)
                {
                    chassert(error_listener.hasError());
                    return {};
                }
                return makeBinaryOperator(compare_ctx, left_argument, right_argument);
            }
            else if (auto * or_ctx = ctx->orOp())
            {
                auto * left_argument = makeNode(ctx->vectorOperation(0));
                auto * right_argument = makeNode(ctx->vectorOperation(1));
                if (!left_argument || !right_argument)
                {
                    chassert(error_listener.hasError());
                    return {};
                }
                return makeBinaryOperator(or_ctx, left_argument, right_argument);
            }
            else if (auto * and_unless_ctx = ctx->andUnlessOp())
            {
                auto * left_argument = makeNode(ctx->vectorOperation(0));
                auto * right_argument = makeNode(ctx->vectorOperation(1));
                if (!left_argument || !right_argument)
                {
                    chassert(error_listener.hasError());
                    return {};
                }
                return makeBinaryOperator(and_unless_ctx, left_argument, right_argument);
            }
            else if (auto * subquery_ctx = ctx->subqueryOp())
            {
                auto * expression = makeNode(ctx->vectorOperation(0));
                if (!expression)
                {
                    chassert(error_listener.hasError());
                    return {};
                }
                return makeSubquery(subquery_ctx, expression);
            }
            else
            {
                return visitChildren(ctx);
            }
        }

        std::any visitFunction_(antlr4_grammars::PromQLParser::Function_Context * ctx) override
        {
            std::vector<Node *> arguments;
            antlr4_grammars::PromQLParser::ParameterContext * parameter_ctx = nullptr;
            for (size_t i = 0; (parameter_ctx = ctx->parameter(i)) != nullptr; ++i)
            {
                Node * argument = makeNode(parameter_ctx);
                if (!argument)
                {
                    chassert(error_listener.hasError());
                    return {};
                }
                arguments.push_back(argument);
            }
            return makeFunction(ctx, arguments);
        }

        std::any visitAggregation(antlr4_grammars::PromQLParser::AggregationContext * ctx) override
        {
            auto * parameter_list_ctx = ctx->parameterList();
            if (!parameter_list_ctx)
                throwInconsistentSchema("Aggregation", ctx->getText());

            std::vector<Node *> arguments;
            antlr4_grammars::PromQLParser::ParameterContext * parameter_ctx = nullptr;
            for (size_t i = 0; (parameter_ctx = parameter_list_ctx->parameter(i)) != nullptr; ++i)
            {
                Node * argument = makeNode(parameter_ctx);
                if (!argument)
                {
                    chassert(error_listener.hasError());
                    return {};
                }
                arguments.push_back(argument);
            }
            return makeAggregationOperator(ctx, arguments);
        }

        /// Converts std::any to a pointer to a Node.
        static Node * anyToNodePtr(std::any any)
        {
            if (!any.has_value())
                return nullptr;

            Node ** node_ptr = std::any_cast<Node *>(&any);
            if (!node_ptr)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "PrometheusQueryTreeBuilder: got {} and not Node *", any.type().name());

            return *node_ptr;
        }

        /// If there is no visitor for some specific case we provide no default handling.
        std::any aggregateResult(std::any aggregate, std::any next_result) override
        {
            Node * node = anyToNodePtr(aggregate);
            Node * next_node = anyToNodePtr(next_result);
            if (node && next_node)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't aggregate nodes {} and {}", node->node_type, next_node->node_type);
            if (node)
                return node;
            else
                return next_node;
        }
    };
}

#endif

bool PrometheusQueryParsingUtil::tryParseQuery([[maybe_unused]] std::string_view input, [[maybe_unused]] UInt32 timestamp_scale, [[maybe_unused]] PrometheusQueryTree & res_query, [[maybe_unused]] String * error_message, [[maybe_unused]] size_t * error_pos)
{
#if USE_ANTLR4_GRAMMARS
    ErrorListener error_listener{input};
    antlr4::ANTLRInputStream input_stream{input};

    antlr4_grammars::PromQLLexer promql_lexer{&input_stream};
    promql_lexer.removeErrorListeners();
    promql_lexer.addErrorListener(&error_listener);

    antlr4::CommonTokenStream token_stream{&promql_lexer};

    antlr4_grammars::PromQLParser promql_parser{&token_stream};
    promql_parser.removeErrorListeners();
    promql_parser.addErrorListener(&error_listener);

    antlr4_grammars::PromQLParser::ExpressionContext * expression = nullptr;
    if (!error_listener.hasError())
        expression = promql_parser.expression();

    if (!expression)
        error_listener.setError("Couldn't get an expression after parsing promql query", 0);

    PrometheusQueryTreeBuilder builder{input, timestamp_scale, error_listener};
    std::vector<std::unique_ptr<Node>> parsed_nodes;
    Node * parsed_root = nullptr;
    if (expression && !error_listener.hasError())
    {
        parsed_root = builder.makeNode(expression);
        parsed_nodes = builder.extractNodes();
    }

    if (error_listener.hasError())
    {
        if (error_message)
            *error_message = error_listener.getErrorMessage();
        if (error_pos)
            *error_pos = error_listener.getErrorPos();
        return false;
    }

    if (!parsed_root)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Parsing promql query '{}' failed without setting any error message", input);

    res_query = PrometheusQueryTree{std::move(parsed_nodes), parsed_root, timestamp_scale};
    return true;
#else
    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "ANTLR4 support is disabled");
#endif

}

}
