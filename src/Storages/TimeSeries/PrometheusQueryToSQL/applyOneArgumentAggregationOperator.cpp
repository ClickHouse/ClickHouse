#include <Storages/TimeSeries/PrometheusQueryToSQL/applyOneArgumentAggregationOperator.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/toVectorGrid.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/transformGroupASTForAggregationOperator.h>
#include <Storages/TimeSeries/timeSeriesTypesToAST.h>
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
    /// Checks if the types of the specified arguments are valid for a one-argument aggregation operator.
    void checkArgumentTypes(
        const PrometheusQueryTree::AggregationOperator * operator_node,
        const std::vector<SQLQueryPiece> & arguments,
        const ConverterContext & context)
    {
        const auto & operator_name = operator_node->operator_name;

        if (arguments.size() != 1)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Aggregation operator '{}' expects {} argument, but was called with {} arguments",
                            operator_name, 1, arguments.size());
        }

        const auto & argument = arguments[0];

        if (argument.type != ResultType::INSTANT_VECTOR)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Aggregation operator '{}' expects an argument of type {}, but expression {} has type {}",
                            operator_name, ResultType::INSTANT_VECTOR,
                            getPromQLText(argument, context), argument.type);
        }
    }

    struct ImplInfo
    {
        OneArgumentAggregationTransform transform_ast;
    };

    const ImplInfo * getImplInfo(std::string_view operator_name)
    {
        static const std::unordered_map<std::string_view, ImplInfo> impl_map = {
            {"sum",
             {
                 [](ASTPtr && v) -> ASTPtr
                 { return makeASTFunction("sumForEach", std::move(v)); },
             }},

            {"min",
             {
                [](ASTPtr && v) -> ASTPtr
                { return makeASTFunction("minForEach", std::move(v)); },
            }},

            {"max",
             {
                [](ASTPtr && v) -> ASTPtr
                { return makeASTFunction("maxForEach", std::move(v)); },
            }},

            {"avg",
             {
                [](ASTPtr && v) -> ASTPtr
                {
                    /// Prometheus averages the series in the order of their tags, and the rounding depends on that order.
                    return makeASTFunction(
                        "timeSeriesAvgOverGroup",
                        makeASTFunction("timeSeriesGroupToTags", make_intrusive<ASTIdentifier>(ColumnNames::Group)),
                        std::move(v));
                },
            }},

            {"count",
             {
                [](ASTPtr && v) -> ASTPtr
                {
                    /// countForEach is special: it returns UInt64 and not Nullable: if all the inputs at any specific position are NULLs,
                    /// countForEach produces 0 at this position instead of NULL. So we need to convert it to NULL with arrayMap.
                    /// Cast to Float64: a subquery like `rate(count(m)[5m:1m])` feeds this grid into a
                    /// `timeSeries*ToGrid` aggregate, which accepts only floats.
                    return makeASTFunction(
                        "CAST",
                        makeASTFunction(
                            "arrayMap",
                            makeASTFunction(
                                "lambda",
                                makeASTFunction("tuple", make_intrusive<ASTIdentifier>("x")),
                                makeASTFunction("nullIf", make_intrusive<ASTIdentifier>("x"), make_intrusive<ASTLiteral>(0u))),
                            makeASTFunction("countForEach", std::move(v))),
                        make_intrusive<ASTLiteral>("Array(Nullable(Float64))"));
                },
            }},

            {"stddev",
             {
                [](ASTPtr && v) -> ASTPtr
                { return makeASTFunction("stddevPopStableForEach", std::move(v)); },
            }},

            {"stdvar",
             {
                [](ASTPtr && v) -> ASTPtr
                { return makeASTFunction("varPopStableForEach", std::move(v)); },
            }},

            {"group",
             {
                [](ASTPtr && v) -> ASTPtr
                {
                    /// arrayMap(x -> if(isNotNull(x), 1., NULL), anyForEach(values))
                    return makeASTFunction(
                        "arrayMap",
                        makeASTFunction(
                            "lambda",
                            makeASTFunction("tuple", make_intrusive<ASTIdentifier>("x")),
                            makeASTFunction(
                                "if",
                                makeASTFunction("isNotNull", make_intrusive<ASTIdentifier>("x")),
                                timeSeriesScalarToAST(1),
                                make_intrusive<ASTLiteral>(Field{} /* NULL */))),
                        makeASTFunction("anyForEach", std::move(v)));
                },
            }},
        };

        auto it = impl_map.find(operator_name);
        if (it == impl_map.end())
            return nullptr;

        return &it->second;
    }

    /// Prometheus averages an instant `topk` or `bottomk` result group by group, each group in the order of its values.
    /// Returns the sort key for that order, or nullptr if `operator_node` is not `avg` over such a result.
    ASTPtr tryMakeValueOrderKey(const PrometheusQueryTree::AggregationOperator * operator_node, const SQLQueryPiece & argument)
    {
        if (operator_node->operator_name != "avg" || argument.start_time != argument.end_time)
            return nullptr;

        /// Unary operators keep the order of the series, and `-` reverses the order of their values.
        const auto * child = operator_node->getArguments().at(0);
        bool negated = false;
        while (child->node_type == PrometheusQueryTree::NodeType::UnaryOperator)
        {
            const auto * unary = static_cast<const PrometheusQueryTree::UnaryOperator *>(child);
            if (unary->operator_name == "-")
                negated = !negated;
            child = unary->getArgument();
        }

        if (child->node_type != PrometheusQueryTree::NodeType::AggregationOperator)
            return nullptr;

        const auto * inner = static_cast<const PrometheusQueryTree::AggregationOperator *>(child);
        bool is_topk = inner->operator_name == "topk";
        if (!is_topk && inner->operator_name != "bottomk")
            return nullptr;

        /// The groups of `topk` are ordered by their tags, which is exact when each group of `avg` is inside one of them.
        bool metric_name_dropped = false;
        ASTPtr inner_group = transformGroupASTForAggregationOperator(
            inner, make_intrusive<ASTIdentifier>(ColumnNames::Group), /*drop_metric_name=*/true, metric_name_dropped);

        ASTPtr values = make_intrusive<ASTIdentifier>(ColumnNames::Values);
        if (is_topk != negated)
            values = makeASTFunction(
                "arrayMap",
                makeASTFunction(
                    "lambda",
                    makeASTFunction("tuple", make_intrusive<ASTIdentifier>("x")),
                    makeASTFunction("negate", make_intrusive<ASTIdentifier>("x"))),
                std::move(values));

        return makeASTFunction("tuple", makeASTFunction("timeSeriesGroupToTags", std::move(inner_group)), std::move(values));
    }
}


bool isOneArgumentAggregationOperator(std::string_view operator_name)
{
    return getImplInfo(operator_name) != nullptr;
}


ASTPtr makeOneArgumentAggregationAST(const PrometheusQueryTree::AggregationOperator * operator_node, const SQLQueryPiece & argument)
{
    if (auto sort_key = tryMakeValueOrderKey(operator_node, argument))
        return makeASTFunction("timeSeriesAvgOverGroup", std::move(sort_key), make_intrusive<ASTIdentifier>(ColumnNames::Values));

    const auto * impl_info = getImplInfo(operator_node->operator_name);
    chassert(impl_info);
    return impl_info->transform_ast(make_intrusive<ASTIdentifier>(ColumnNames::Values));
}


SQLQueryPiece applyOneArgumentAggregationOperator(
    const PrometheusQueryTree::AggregationOperator * operator_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context)
{
    checkArgumentTypes(operator_node, arguments, context);

    auto & argument = arguments[0];

    /// If the argument is empty then the result is also empty.
    if (argument.store_method == StoreMethod::EMPTY)
        return SQLQueryPiece{operator_node, operator_node->result_type, StoreMethod::EMPTY};

    argument = toVectorGrid(std::move(argument), context);

    auto res = argument;
    res.node = operator_node;

    /// Step 1: aggregate over series, using `new_group` as an intermediate alias to avoid
    /// ambiguity with the input `group` column when the alias and the source column share the same name.
    ASTPtr aggregation_query;
    {
        SelectQueryBuilder builder;

        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(argument.select_query), SQLSubqueryType::TABLE});
        builder.from_table = context.subqueries.back().name;

        ASTPtr new_group = transformGroupASTForAggregationOperator(
            operator_node, make_intrusive<ASTIdentifier>(ColumnNames::Group), /*drop_metric_name=*/true, res.metric_name_dropped);

        builder.select_list.push_back(std::move(new_group));
        builder.select_list.back()->setAlias(ColumnNames::NewGroup);

        builder.select_list.push_back(makeOneArgumentAggregationAST(operator_node, res));
        builder.select_list.back()->setAlias(ColumnNames::Values);

        if (operator_node->by || operator_node->without)
            builder.group_by.push_back(make_intrusive<ASTIdentifier>(ColumnNames::NewGroup));

        aggregation_query = builder.getSelectQuery();
    }

    /// Step 2: rename `new_group` back to `group` and drop empty grids (a WHERE, so `values` cannot bind to the input column
    /// under prefer_column_name_to_alias).
    {
        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(aggregation_query), SQLSubqueryType::TABLE});

        SelectQueryBuilder builder;
        builder.from_table = context.subqueries.back().name;
        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::NewGroup));
        builder.select_list.back()->setAlias(ColumnNames::Group);
        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Values));
        builder.where = makeASTFunction("notEmpty", make_intrusive<ASTIdentifier>(ColumnNames::Values));

        res.select_query = builder.getSelectQuery();
    }

    return res;
}

}
