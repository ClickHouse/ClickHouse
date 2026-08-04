#include <Storages/TimeSeries/PrometheusQueryToSQL/applySortFunction.h>

#include <Common/Exception.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>


namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
    bool isSortByLabelFunction(std::string_view function_name)
    {
        return (function_name == "sort_by_label") || (function_name == "sort_by_label_desc");
    }
}

bool isSortFunction(std::string_view function_name)
{
    return (function_name == "sort") || (function_name == "sort_desc") || isSortByLabelFunction(function_name);
}

SQLQueryPiece applySortFunction(
    const PQT::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context)
{
    const auto & function_name = function_node->function_name;

    if (isSortByLabelFunction(function_name))
    {
        /// sort_by_label(v instant-vector, label string, ...) requires the vector and at least one label.
        if (arguments.size() < 2)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Function '{}' expects at least 2 arguments (an instant vector and at least one label name), "
                            "but was called with {} arguments",
                            function_name, arguments.size());
        }

        auto & argument = arguments[0];

        if (argument.type != ResultType::INSTANT_VECTOR)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Function '{}' expects the first argument of type {}, but expression {} has type {}",
                            function_name, ResultType::INSTANT_VECTOR,
                            getPromQLText(argument, context), argument.type);
        }

        std::vector<String> labels;
        labels.reserve(arguments.size() - 1);
        for (size_t i = 1; i < arguments.size(); ++i)
        {
            if (arguments[i].type != ResultType::STRING)
            {
                throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                                "Function '{}' expects argument #{} of type {}, but expression {} has type {}",
                                function_name, i + 1, ResultType::STRING,
                                getPromQLText(arguments[i], context), arguments[i].type);
            }
            labels.push_back(arguments[i].string_value);
        }

        /// sort_by_label() / sort_by_label_desc() do not change the values, they only affect the output ordering.
        /// We pass the vector through unchanged and set the sort direction and the labels to sort by on the result.
        argument.sort_direction = (function_name == "sort_by_label") ? 1 : -1;
        argument.sort_by_labels = std::move(labels);
        argument.node = function_node;
        return std::move(argument);
    }

    if (arguments.size() != 1)
    {
        throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                        "Function '{}' expects {} argument, but was called with {} arguments",
                        function_name, 1, arguments.size());
    }

    auto & argument = arguments[0];

    if (argument.type != ResultType::INSTANT_VECTOR)
    {
        throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                        "Function '{}' expects an argument of type {}, but expression {} has type {}",
                        function_name, ResultType::INSTANT_VECTOR,
                        getPromQLText(argument, context), argument.type);
    }

    /// sort() / sort_desc() do not change the values, they only affect the output ordering.
    /// We pass the argument through unchanged and set the sort direction on the result.
    /// sort() / sort_desc() replace the ordering mode entirely, so any `sort_by_labels` set by an
    /// earlier sort_by_label() / sort_by_label_desc() in the argument (e.g. sort_desc(sort_by_label(...)))
    /// must be cleared here, otherwise finalizeSQL() would still take the label-sorting path.
    argument.sort_direction = (function_name == "sort") ? 1 : -1;
    argument.sort_by_labels.clear();
    argument.node = function_node;
    return std::move(argument);
}

}
