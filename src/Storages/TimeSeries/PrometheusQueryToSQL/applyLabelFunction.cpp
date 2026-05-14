#include <Storages/TimeSeries/PrometheusQueryToSQL/applyLabelFunction.h>

#include <Common/Exception.h>
#include <Common/isValidUTF8.h>
#include <Common/re2.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/toVectorGrid.h>

#include <cstring>


namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
    constexpr const char * NewValues = "new_values";

    bool isValidPrometheusLabelName(std::string_view label_name)
    {
        return !label_name.empty() && UTF8::isValidUTF8(reinterpret_cast<const UInt8 *>(label_name.data()), label_name.size());
    }

    String makePrometheusRegex(std::string_view regex)
    {
        String result;
        result.reserve(regex.size() + strlen("(?s:)"));
        result += "(?s:";
        result += regex;
        result += ")";
        return result;
    }

    void validatePrometheusRegex(std::string_view regex)
    {
        re2::RE2::Options options;
        options.set_log_errors(false);
        re2::RE2 compiled_regex(makePrometheusRegex(regex), options);
        if (!compiled_regex.ok())
        {
            throw Exception(
                ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                "invalid regular expression in label_replace(): {}",
                regex);
        }
    }

    void validateDestinationLabelName(std::string_view function_name, std::string_view label_name)
    {
        if (isValidPrometheusLabelName(label_name))
            return;

        throw Exception(
            ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
            "invalid destination label name in {}(): {}",
            function_name,
            label_name);
    }

    void validateSourceLabelName(std::string_view function_name, std::string_view label_name)
    {
        if (isValidPrometheusLabelName(label_name))
            return;

        throw Exception(
            ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
            "invalid source label name in {}(): {}",
            function_name,
            label_name);
    }

    const String & getConstStringArgument(
        const PQT::Function * function_node,
        const std::vector<SQLQueryPiece> & arguments,
        size_t argument_index,
        const ConverterContext & context)
    {
        const auto & function_name = function_node->function_name;
        chassert(argument_index < arguments.size());

        const auto & argument = arguments[argument_index];
        if ((argument.type != ResultType::STRING) || (argument.store_method != StoreMethod::CONST_STRING))
        {
            throw Exception(
                ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                "Function '{}' expects argument #{} to be a string literal, but expression {} has type {}",
                function_name,
                argument_index + 1,
                getPromQLText(argument, context),
                argument.type);
        }

        return argument.string_value;
    }

    void checkVectorArgument(
        const PQT::Function * function_node,
        const std::vector<SQLQueryPiece> & arguments,
        const ConverterContext & context)
    {
        const auto & function_name = function_node->function_name;
        const auto & argument = arguments[0];
        if (argument.type != ResultType::INSTANT_VECTOR)
        {
            throw Exception(
                ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                "Function '{}' expects argument #1 to be of type {}, but expression {} has type {}",
                function_name,
                ResultType::INSTANT_VECTOR,
                getPromQLText(argument, context),
                argument.type);
        }
    }

    struct LabelFunctionArguments
    {
        String destination_label;
        String replacement;
        String source_label;
        String regex;
        String separator;
        Strings source_labels;
    };

    LabelFunctionArguments checkArgumentTypes(
        const PQT::Function * function_node,
        const std::vector<SQLQueryPiece> & arguments,
        const ConverterContext & context)
    {
        const auto & function_name = function_node->function_name;
        LabelFunctionArguments result;

        if (function_name == "label_replace")
        {
            if (arguments.size() != 5)
            {
                throw Exception(
                    ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                    "Function '{}' expects {} arguments, but was called with {} arguments",
                    function_name,
                    5,
                    arguments.size());
            }

            checkVectorArgument(function_node, arguments, context);

            result.destination_label = getConstStringArgument(function_node, arguments, 1, context);
            result.replacement = getConstStringArgument(function_node, arguments, 2, context);
            result.source_label = getConstStringArgument(function_node, arguments, 3, context);
            result.regex = getConstStringArgument(function_node, arguments, 4, context);

            validateDestinationLabelName(function_name, result.destination_label);
            validatePrometheusRegex(result.regex);
            result.regex = makePrometheusRegex(result.regex);
        }
        else if (function_name == "label_join")
        {
            if (arguments.size() < 3)
            {
                throw Exception(
                    ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                    "Function '{}' expects at least {} arguments, but was called with {} arguments",
                    function_name,
                    3,
                    arguments.size());
            }

            checkVectorArgument(function_node, arguments, context);

            result.destination_label = getConstStringArgument(function_node, arguments, 1, context);
            result.separator = getConstStringArgument(function_node, arguments, 2, context);
            validateDestinationLabelName(function_name, result.destination_label);

            result.source_labels.reserve(arguments.size() - 3);
            for (size_t i = 3; i != arguments.size(); ++i)
            {
                const auto & source_label = getConstStringArgument(function_node, arguments, i, context);
                validateSourceLabelName(function_name, source_label);
                result.source_labels.push_back(source_label);
            }
        }
        else
        {
            UNREACHABLE();
        }

        return result;
    }

    ASTPtr makeTransformedGroupAST(std::string_view function_name, const LabelFunctionArguments & arguments)
    {
        if (function_name == "label_replace")
        {
            return makeASTFunction(
                "timeSeriesReplaceTag",
                make_intrusive<ASTIdentifier>(ColumnNames::Group),
                make_intrusive<ASTLiteral>(arguments.destination_label),
                make_intrusive<ASTLiteral>(arguments.replacement),
                make_intrusive<ASTLiteral>(arguments.source_label),
                make_intrusive<ASTLiteral>(arguments.regex));
        }

        if (function_name == "label_join")
        {
            return makeASTFunction(
                "timeSeriesJoinTags",
                make_intrusive<ASTIdentifier>(ColumnNames::Group),
                make_intrusive<ASTLiteral>(arguments.destination_label),
                make_intrusive<ASTLiteral>(arguments.separator),
                make_intrusive<ASTLiteral>(Array{arguments.source_labels.begin(), arguments.source_labels.end()}));
        }

        UNREACHABLE();
    }

    ASTPtr makeHasDuplicateSamplesPredicate()
    {
        return makeASTFunction(
            "arrayExists",
            makeASTLambda({"x"}, makeASTFunction("greater", make_intrusive<ASTIdentifier>("x"), make_intrusive<ASTLiteral>(1u))),
            makeASTFunction("countForEach", make_intrusive<ASTIdentifier>(ColumnNames::Values)));
    }
}

SQLQueryPiece applyLabelFunction(const PQT::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context)
{
    const auto & function_name = function_node->function_name;
    chassert(isLabelFunction(function_name));

    auto label_function_arguments = checkArgumentTypes(function_node, arguments, context);

    if (arguments[0].store_method == StoreMethod::EMPTY)
        return SQLQueryPiece{function_node, ResultType::INSTANT_VECTOR, StoreMethod::EMPTY};

    auto vector_argument = toVectorGrid(std::move(arguments[0]), context);

    ASTPtr label_function_query;
    {
        SelectQueryBuilder builder;

        builder.select_list.push_back(makeTransformedGroupAST(function_name, label_function_arguments));
        builder.select_list.back()->setAlias(ColumnNames::NewGroup);

        builder.select_list.push_back(makeASTFunction("anyForEach", make_intrusive<ASTIdentifier>(ColumnNames::Values)));
        builder.select_list.back()->setAlias(NewValues);

        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(vector_argument.select_query), SQLSubqueryType::TABLE});
        builder.from_table = context.subqueries.back().name;

        builder.group_by.push_back(make_intrusive<ASTIdentifier>(ColumnNames::NewGroup));
        builder.having = makeASTFunction(
            "equals",
            makeASTFunction(
                "timeSeriesThrowDuplicateSeriesIf",
                makeHasDuplicateSamplesPredicate(),
                make_intrusive<ASTIdentifier>(ColumnNames::NewGroup)),
            make_intrusive<ASTLiteral>(0u));

        label_function_query = builder.getSelectQuery();
    }

    ASTPtr result_query;
    {
        SelectQueryBuilder builder;

        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::NewGroup));
        builder.select_list.back()->setAlias(ColumnNames::Group);

        builder.select_list.push_back(make_intrusive<ASTIdentifier>(NewValues));
        builder.select_list.back()->setAlias(ColumnNames::Values);

        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(label_function_query), SQLSubqueryType::TABLE});
        builder.from_table = context.subqueries.back().name;

        result_query = builder.getSelectQuery();
    }

    SQLQueryPiece res{function_node, ResultType::INSTANT_VECTOR, StoreMethod::VECTOR_GRID};
    res.select_query = std::move(result_query);
    res.start_time = vector_argument.start_time;
    res.end_time = vector_argument.end_time;
    res.step = vector_argument.step;
    res.metric_name_dropped = (label_function_arguments.destination_label == kMetricName) ? false : vector_argument.metric_name_dropped;

    return res;
}

}
