#include <Storages/TimeSeries/PrometheusQueryToSQL/applyLabelManipulationFunction.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/Prometheus/stepsInTimeSeriesRange.h>
#include <Common/isValidUTF8.h>
#include <Common/quoteString.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Storages/TimeSeries/timeSeriesTypesToAST.h>
#include <base/insertAtEnd.h>

#include <algorithm>
#include <unordered_map>


namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
    /// Checks that a label name argument is a valid label name.
    /// Reads the text from the string literal node, because `SQLQueryPiece::string_value` isn't set
    /// if the evaluation range of the literal is empty (see `fromLiteral`).
    void checkLabelName(const PrometheusQueryTree::Function * function_node, size_t argument_index)
    {
        const auto & function_name = function_node->function_name;
        const auto * argument_node = function_node->getArguments().at(argument_index);
        if (argument_node->node_type != PrometheusQueryTree::NodeType::StringLiteral)
        {
            throw Exception(
                ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                "Function '{}' expects a string literal in argument #{}",
                function_name,
                argument_index + 1);
        }

        const auto & label_name = static_cast<const PrometheusQueryTree::StringLiteral *>(argument_node)->string;
        if (label_name.empty() || !UTF8::isValidUTF8(reinterpret_cast<const UInt8 *>(label_name.data()), label_name.size()))
        {
            throw Exception(
                ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                "Function '{}' received invalid label name {} in argument #{}",
                function_name,
                quoteString(label_name),
                argument_index + 1);
        }
    }

    /// Checks if the types of the specified arguments are valid for a label manipulation function.
    void checkArgumentTypes(
        const PrometheusQueryTree::Function * function_node, const std::vector<SQLQueryPiece> & arguments, const ConverterContext & context)
    {
        const auto & function_name = function_node->function_name;

        if (function_name == "label_replace")
        {
            if (arguments.size() != 5)
                throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                                "Function '{}' expects {} arguments, but was called with {} arguments",
                                function_name, 5, arguments.size());
        }
        else
        {
            chassert(function_name == "label_join");
            if (arguments.size() < 3)
                throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                                "Function '{}' expects {} or more arguments, but was called with {} arguments",
                                function_name, 3, arguments.size());
        }

        chassert(!arguments.empty());

        const auto & first_argument = arguments[0];
        if (first_argument.type != ResultType::INSTANT_VECTOR)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Function '{}' expects the first argument of type {}, but expression {} has type {}",
                            function_name, ResultType::INSTANT_VECTOR, getPromQLText(first_argument, context), first_argument.type);
        }

        for (size_t i = 1; i < arguments.size(); ++i)
        {
            const auto & argument = arguments[i];
            if (argument.type != ResultType::STRING)
            {
                throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                                "Function '{}' expects argument #{} of type {}, but expression {} has type {}",
                                function_name, i + 1, ResultType::STRING, getPromQLText(argument, context), argument.type);
            }
        }

        if (function_name == "label_replace")
        {
            checkLabelName(function_node, 1);
        }
        else
        {
            for (size_t i = 3; i < arguments.size(); ++i)
                checkLabelName(function_node, i);

            checkLabelName(function_node, 1);
        }
    }

    struct ImplInfo
    {
        std::string_view ch_function_name;
        /// If set, PromQL arguments from this index onward are passed as a single Array(String) literal
        /// to the underlying ClickHouse function (used for `label_join`'s variadic src_tags).
        size_t array_argument_index = static_cast<size_t>(-1);
    };

    const ImplInfo * getImplInfo(std::string_view function_name)
    {
        static const std::unordered_map<std::string_view, ImplInfo> impl_map = {
            {"label_replace", {"timeSeriesReplaceTag"}},
            {"label_join",    {"timeSeriesJoinTags", 3}},
        };

        auto it = impl_map.find(function_name);
        if (it == impl_map.end())
            return nullptr;

        return &it->second;
    }

    /// Collects PromQL string arguments [start_index..end_index) into a list of String literals.
    ASTs collectStringArguments(const std::vector<SQLQueryPiece> & arguments, size_t start_index, size_t end_index = static_cast<size_t>(-1))
    {
        end_index = std::min(end_index, arguments.size());
        ASTs result;
        result.reserve(end_index - start_index);
        for (size_t i = start_index; i < end_index; ++i)
            result.push_back(make_intrusive<ASTLiteral>(arguments[i].string_value));
        return result;
    }

    /// Collects PromQL string arguments [start_index..end_index) into a single Array(String) literal.
    ASTPtr collectStringArgumentsAsArray(const std::vector<SQLQueryPiece> & arguments, size_t start_index, size_t end_index = static_cast<size_t>(-1))
    {
        end_index = std::min(end_index, arguments.size());
        Array values;
        values.reserve(end_index - start_index);
        for (size_t i = start_index; i < end_index; ++i)
            values.emplace_back(arguments[i].string_value);
        return make_intrusive<ASTLiteral>(std::move(values));
    }
}


bool isLabelManipulationFunction(std::string_view function_name)
{
    return getImplInfo(function_name) != nullptr;
}


SQLQueryPiece applyLabelManipulationFunction(
    const PrometheusQueryTree::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context)
{
    checkArgumentTypes(function_node, arguments, context);

    const auto & function_name = function_node->function_name;
    const auto * impl_info = getImplInfo(function_name);
    chassert(impl_info);

    /// Prometheus doesn't validate the source label of `label_replace`, and a label with an empty or invalid UTF-8 name
    /// can't exist there, so such a source label always behaves like a missing label. The tags stored in a `TimeSeries` table
    /// can have invalid UTF-8 names, so we replace such a source label with the empty name, which can't be stored.
    if (function_name == "label_replace")
    {
        auto & src_label = arguments[3].string_value;
        if (!UTF8::isValidUTF8(reinterpret_cast<const UInt8 *>(src_label.data()), src_label.size()))
            src_label.clear();
    }

    chassert(arguments.size() >= 2);
    auto & first_argument = arguments[0];
    const String & dest_label = arguments[1].string_value;

    auto res = first_argument;
    res.node = function_node;

    switch (first_argument.store_method)
    {
        case StoreMethod::EMPTY:
        {
            return res;
        }

        case StoreMethod::CONST_SCALAR:
        case StoreMethod::SINGLE_SCALAR:
        case StoreMethod::SCALAR_GRID:
        {
            /// For const scalar:
            /// SELECT f(0, 'arg2', 'arg3', ...) AS group, arrayResize([], <count_of_time_steps>, <scalar_value>) AS values
            ///
            /// For single scalar:
            /// SELECT f(0, 'arg2', 'arg3', ...) AS group, arrayResize([], <count_of_time_steps>, value) AS values FROM <subquery>
            ///
            /// For scalar grid:
            /// SELECT f(0, 'arg2', 'arg3', ...) AS group, values
            /// FROM <scalar_grid>
            SelectQueryBuilder builder;

            ASTs group_function_args;
            group_function_args.push_back(makeASTFunction(
                "CAST", make_intrusive<ASTLiteral>(0u), make_intrusive<ASTLiteral>("UInt64"))); /// Group "0" means no tags

            size_t array_argument_index = impl_info->array_argument_index;
            insertAtEnd(group_function_args, collectStringArguments(arguments, 1, array_argument_index));
            if (array_argument_index != static_cast<size_t>(-1))
                group_function_args.push_back(collectStringArgumentsAsArray(arguments, array_argument_index));

            auto group_function = makeASTFunction(impl_info->ch_function_name);
            group_function->arguments->children = std::move(group_function_args);

            builder.select_list.push_back(std::move(group_function));
            builder.select_list.back()->setAlias(ColumnNames::Group);

            ASTPtr values;
            if (first_argument.store_method == StoreMethod::SCALAR_GRID)
            {
                values = make_intrusive<ASTIdentifier>(ColumnNames::Values);
            }
            else
            {
                ASTPtr value = (first_argument.store_method == StoreMethod::CONST_SCALAR)
                    ? timeSeriesScalarToAST(first_argument.scalar_value)
                    : make_intrusive<ASTIdentifier>(ColumnNames::Value);

                values = makeASTFunction(
                    "arrayResize",
                    make_intrusive<ASTLiteral>(Array{}),
                    make_intrusive<ASTLiteral>(
                        stepsInTimeSeriesRange(first_argument.start_time, first_argument.end_time, first_argument.step)),
                    value);

                values->setAlias(ColumnNames::Values);
            }

            builder.select_list.push_back(std::move(values));

            if (first_argument.select_query)
            {
                context.subqueries.emplace_back(
                    SQLSubquery{context.subqueries.size(), std::move(first_argument.select_query), SQLSubqueryType::TABLE});
                builder.from_table = context.subqueries.back().name;
            }

            res.select_query = builder.getSelectQuery();
            res.store_method = StoreMethod::VECTOR_GRID;
            res.scalar_value = {};
            res.metric_name_dropped = (dest_label != kMetricName);

            return res;
        }

        case StoreMethod::VECTOR_GRID:
        {
            /// Step 1:
            /// SELECT f(group, 'arg2', 'arg3', ...) AS new_group, any(values) AS values
            /// FROM <vector_grid>
            /// GROUP BY new_group
            /// HAVING timeSeriesThrowDuplicateSeriesIf(count() > 1, new_group) = 0
            ///
            /// If the destination label is '__name__' then the metric name is preserved in the result,
            /// so the marker of a dropped metric name (see dropMetricName) is removed from the new group:
            /// SELECT timeSeriesRemoveTag(f(group, '__name__', 'arg3', ...), '__name__.dropped') AS new_group, ...
            ASTPtr label_replacing_query;
            {
                SelectQueryBuilder builder;

                ASTs group_function_args;
                group_function_args.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));

                size_t array_argument_index = impl_info->array_argument_index;
                insertAtEnd(group_function_args, collectStringArguments(arguments, 1, array_argument_index));
                if (array_argument_index != static_cast<size_t>(-1))
                    group_function_args.push_back(collectStringArgumentsAsArray(arguments, array_argument_index));

                auto group_function = makeASTFunction(impl_info->ch_function_name);
                group_function->arguments->children = std::move(group_function_args);

                ASTPtr new_group = std::move(group_function);
                if (dest_label == kMetricName)
                    new_group = makeASTFunction(
                        "timeSeriesRemoveTag", std::move(new_group), make_intrusive<ASTLiteral>(kDroppedMetricNameMarker));

                builder.select_list.push_back(std::move(new_group));
                builder.select_list.back()->setAlias(ColumnNames::NewGroup);

                builder.select_list.push_back(makeASTFunction("any", make_intrusive<ASTIdentifier>(ColumnNames::Values)));
                builder.select_list.back()->setAlias(ColumnNames::Values);

                context.subqueries.emplace_back(
                    SQLSubquery{context.subqueries.size(), std::move(first_argument.select_query), SQLSubqueryType::TABLE});
                builder.from_table = context.subqueries.back().name;

                builder.group_by.push_back(make_intrusive<ASTIdentifier>(ColumnNames::NewGroup));

                builder.having = makeASTFunction(
                    "equals",
                    makeASTFunction(
                        "timeSeriesThrowDuplicateSeriesIf",
                        makeASTFunction("greater", makeASTFunction("count"), make_intrusive<ASTLiteral>(1u)),
                        make_intrusive<ASTIdentifier>(ColumnNames::NewGroup)),
                    make_intrusive<ASTLiteral>(0u));

                label_replacing_query = builder.getSelectQuery();
            }

            /// Step 2:
            /// SELECT new_group AS group, values
            /// FROM step1
            ASTPtr column_renaming_query;
            {
                SelectQueryBuilder builder;

                builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::NewGroup));
                builder.select_list.back()->setAlias(ColumnNames::Group);

                builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Values));

                context.subqueries.emplace_back(
                    SQLSubquery{context.subqueries.size(), std::move(label_replacing_query), SQLSubqueryType::TABLE});
                builder.from_table = context.subqueries.back().name;

                column_renaming_query = builder.getSelectQuery();
            }

            res.select_query = std::move(column_renaming_query);

            if (dest_label == kMetricName)
                res.metric_name_dropped = false;

            return res;
        }

        case StoreMethod::CONST_STRING:
        case StoreMethod::RAW_DATA:
        {
            /// Can't get in here because these store methods are incompatible with the allowed argument types
            /// (see checkArgumentTypes()).
            throwUnexpectedStoreMethod(first_argument, context);
        }
    }

    UNREACHABLE();
}

}
