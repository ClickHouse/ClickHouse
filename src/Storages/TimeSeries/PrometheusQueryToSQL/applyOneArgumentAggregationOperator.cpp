#include <Storages/TimeSeries/PrometheusQueryToSQL/applyOneArgumentAggregationOperator.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/toVectorGrid.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/transformGroupASTForAggregationOperator.h>
#include <Storages/TimeSeries/timeSeriesTypesToAST.h>
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
    void checkArgumentTypes(const PQT::AggregationOperator * operator_node, const std::vector<SQLQueryPiece> & arguments, const ConverterContext & context)
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

    using TransformASTFunc = ASTPtr (*)(ASTPtr && v, const DataTypePtr & scalar_data_type);

    struct ImplInfo
    {
        TransformASTFunc transform_ast;
    };

    const ImplInfo * getImplInfo(std::string_view operator_name)
    {
        static const std::unordered_map<std::string_view, ImplInfo> impl_map = {
            {"sum",
             {
                 [](ASTPtr && v, const DataTypePtr &) -> ASTPtr
                 { return makeASTFunction("sumForEach", std::move(v)); },
             }},

            {"min",
             {
                [](ASTPtr && v, const DataTypePtr &) -> ASTPtr
                { return makeASTFunction("minForEach", std::move(v)); },
            }},

            {"max",
             {
                [](ASTPtr && v, const DataTypePtr &) -> ASTPtr
                { return makeASTFunction("maxForEach", std::move(v)); },
            }},

            {"avg",
             {
                [](ASTPtr && v, const DataTypePtr &) -> ASTPtr
                { return makeASTFunction("avgForEach", std::move(v)); },
            }},

            {"count",
             {
                [](ASTPtr && v, const DataTypePtr &) -> ASTPtr
                {
                    /// countForEach is special: it returns UInt64 and not Nullable: if all the inputs at any specific position are NULLs,
                    /// countForEach produces 0 at this position instead of NULL. So we need to convert it to NULL with arrayMap.
                    return makeASTFunction(
                        "arrayMap",
                        makeASTFunction(
                            "lambda",
                            makeASTFunction("tuple", make_intrusive<ASTIdentifier>("x")),
                            makeASTFunction("nullIf", make_intrusive<ASTIdentifier>("x"), make_intrusive<ASTLiteral>(0u))),
                        makeASTFunction("countForEach", std::move(v)));
                },
            }},

            {"stddev",
             {
                [](ASTPtr && v, const DataTypePtr &) -> ASTPtr
                { return makeASTFunction("stddevPopForEach", std::move(v)); },
            }},

            {"stdvar",
             {
                [](ASTPtr && v, const DataTypePtr &) -> ASTPtr
                { return makeASTFunction("varPopForEach", std::move(v)); },
            }},

            {"group",
             {
                [](ASTPtr && v, const DataTypePtr & scalar_data_type) -> ASTPtr
                {
                    /// arrayMap(x -> if(isNotNull(x), 1::scalar_data_type, NULL), anyForEach(values))
                    return makeASTFunction(
                        "arrayMap",
                        makeASTFunction(
                            "lambda",
                            makeASTFunction("tuple", make_intrusive<ASTIdentifier>("x")),
                            makeASTFunction(
                                "if",
                                makeASTFunction("isNotNull", make_intrusive<ASTIdentifier>("x")),
                                timeSeriesScalarToAST(1, scalar_data_type),
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
}


bool isOneArgumentAggregationOperator(std::string_view operator_name)
{
    return getImplInfo(operator_name) != nullptr;
}


SQLQueryPiece applyOneArgumentAggregationOperator(
    const PQT::AggregationOperator * operator_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context)
{
    const auto & operator_name = operator_node->operator_name;
    const auto * impl_info = getImplInfo(operator_name);
    chassert(impl_info);

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

        builder.select_list.push_back(impl_info->transform_ast(make_intrusive<ASTIdentifier>(ColumnNames::Values), context.scalar_data_type));
        builder.select_list.back()->setAlias(ColumnNames::Values);

        if (operator_node->by || operator_node->without)
            builder.group_by.push_back(make_intrusive<ASTIdentifier>(ColumnNames::NewGroup));

        /// Drop empty-values rows.
        /// If the input has no rows then countForEach([]) returns [], but the number of values
        /// in array must always match the number of steps in SQLQueryPiece (see StoreMethod::VECTOR_GRID),
        /// so we just drop such rows.
        builder.having = makeASTFunction("notEmpty", make_intrusive<ASTIdentifier>(ColumnNames::Values));

        aggregation_query = builder.getSelectQuery();
    }

    /// Step 2: rename `new_group` back to `group`.
    {
        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(aggregation_query), SQLSubqueryType::TABLE});

        SelectQueryBuilder builder;
        builder.from_table = context.subqueries.back().name;
        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::NewGroup));
        builder.select_list.back()->setAlias(ColumnNames::Group);
        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Values));

        res.select_query = builder.getSelectQuery();
    }

    return res;
}

}
