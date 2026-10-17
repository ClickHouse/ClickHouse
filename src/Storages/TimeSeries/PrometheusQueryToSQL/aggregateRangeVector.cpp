#include <Storages/TimeSeries/PrometheusQueryToSQL/aggregateRangeVector.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>


namespace DB::PrometheusQueryToSQL
{

namespace
{
    constexpr const char * STATE = "state";
}


RangeVectorAggregation aggregateRangeVector(SQLQueryPiece & range_vector, boost::intrusive_ptr<ASTFunction> aggregate_function, ConverterContext & context)
{
    RangeVectorAggregation res;

    switch (range_vector.store_method)
    {
        case StoreMethod::RAW_DATA:
        {
            if (!context.aggregate_range_vectors_in_order)
            {
                /// SELECT timeSeriesIdToGroup(id), <aggregate_function> ... GROUP BY timeSeriesIdToGroup(id)
                res.group_expression = makeASTFunction("timeSeriesIdToGroup", make_intrusive<ASTIdentifier>(ColumnNames::ID));
                res.group_by_key = res.group_expression->clone();
                res.aggregate_function = std::move(aggregate_function);
                break;
            }

            /// SELECT group, <aggregate_function>Merge(<parameters>)(state) ... GROUP BY group
            /// FROM (SELECT timeSeriesIdToGroup(id) AS group, <aggregate_function>State(<parameters>)(<arguments>) AS state
            ///       FROM <raw_data>
            ///       GROUP BY id)  -- in the order of the samples table
            auto merge_function = makeASTFunction(aggregate_function->name + "Merge", make_intrusive<ASTIdentifier>(STATE));
            if (aggregate_function->parameters)
            {
                merge_function->parameters = aggregate_function->parameters->clone();
                merge_function->children.push_back(merge_function->parameters);
            }
            aggregate_function->name += "State";

            SelectQueryBuilder builder;
            builder.select_list.push_back(makeASTFunction("timeSeriesIdToGroup", make_intrusive<ASTIdentifier>(ColumnNames::ID)));
            builder.select_list.back()->setAlias(ColumnNames::Group);
            builder.select_list.push_back(std::move(aggregate_function));
            builder.select_list.back()->setAlias(STATE);
            builder.group_by.push_back(make_intrusive<ASTIdentifier>(ColumnNames::ID));

            auto & subqueries = context.subqueries;
            subqueries.emplace_back(subqueries.size(), std::move(range_vector.select_query), SQLSubqueryType::TABLE);
            builder.from_table = subqueries.back().name;
            range_vector.select_query = builder.getSelectQuery();

            res.group_expression = make_intrusive<ASTIdentifier>(ColumnNames::Group);
            res.group_by_key = make_intrusive<ASTIdentifier>(ColumnNames::Group);
            res.aggregate_function = std::move(merge_function);
            break;
        }

        case StoreMethod::VECTOR_GRID:
        {
            /// SELECT group, <aggregate_function> ... GROUP BY group
            res.group_expression = make_intrusive<ASTIdentifier>(ColumnNames::Group);
            res.group_by_key = make_intrusive<ASTIdentifier>(ColumnNames::Group);
            res.aggregate_function = std::move(aggregate_function);
            break;
        }

        case StoreMethod::EMPTY:
        case StoreMethod::CONST_SCALAR:
        case StoreMethod::CONST_STRING:
        case StoreMethod::SINGLE_SCALAR:
        case StoreMethod::SCALAR_GRID:
        {
            /// These store methods have no series to group by.
            throwUnexpectedStoreMethod(range_vector, context);
        }
    }

    return res;
}

}
