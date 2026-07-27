#include <Storages/TimeSeries/PrometheusQueryToSQL/fromSelector.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/makeASTForLogicalFunction.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/NodeEvaluationRange.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFunctionOverRange.h>
#include <Storages/TimeSeries/timeSeriesTypesToAST.h>


namespace DB::PrometheusQueryToSQL
{

namespace
{
    /// Makes an AST for the table function call `timeSeriesSelector(db, table, selector, min_time, max_time)`.
    ASTPtr makeSelectorTableFunction(std::string_view instant_selector_text,
                                     TimestampType min_time,
                                     TimestampType max_time,
                                     const ConverterContext & context)
    {
        return makeASTFunction(
            "timeSeriesSelector",
            make_intrusive<ASTLiteral>(context.time_series_storage_id.getDatabaseName()),
            make_intrusive<ASTLiteral>(context.time_series_storage_id.getTableName()),
            make_intrusive<ASTLiteral>(String{instant_selector_text}),
            timeSeriesTimestampToAST(min_time, context.timestamp_data_type),
            timeSeriesTimestampToAST(max_time, context.timestamp_data_type));
    }

    /// For a TimeSeries table with chunked samples the selector returns per-series arrays covering whole
    /// chunks. This function builds the query converting them to the scalar RAW_DATA form:
    ///
    /// SELECT timeSeriesIdToGroup(id) AS group, timestamp, value
    /// FROM timeSeriesSelector(...)
    /// ARRAY JOIN timestamps AS timestamp, `values` AS value
    /// WHERE (timestamp >= <min_time>) AND (timestamp <= <max_time>)
    ///
    /// The WHERE clips the chunks to the selector's exact time range.
    ASTPtr makeArrayJoinedSelectorQuery(ASTPtr table_function,
                                        TimestampType min_time,
                                        TimestampType max_time,
                                        const ConverterContext & context)
    {
        auto select_query = make_intrusive<ASTSelectQuery>();

        {
            auto select_list = make_intrusive<ASTExpressionList>();
            select_list->children.push_back(makeASTFunction("timeSeriesIdToGroup", make_intrusive<ASTIdentifier>(ColumnNames::ID)));
            select_list->children.back()->setAlias(ColumnNames::Group);
            select_list->children.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Timestamp));
            select_list->children.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Value));
            select_query->setExpression(ASTSelectQuery::Expression::SELECT, std::move(select_list));
        }

        auto tables = make_intrusive<ASTTablesInSelectQuery>();

        {
            auto table_exp = make_intrusive<ASTTableExpression>();
            table_exp->table_function = std::move(table_function);
            table_exp->children.emplace_back(table_exp->table_function);

            auto table_element = make_intrusive<ASTTablesInSelectQueryElement>();
            table_element->table_expression = table_exp;
            table_element->children.push_back(std::move(table_exp));
            tables->children.push_back(std::move(table_element));
        }

        {
            auto array_join_expressions = make_intrusive<ASTExpressionList>();
            auto timestamps_identifier = make_intrusive<ASTIdentifier>(ColumnNames::Timestamps);
            timestamps_identifier->setAlias(ColumnNames::Timestamp);
            array_join_expressions->children.push_back(std::move(timestamps_identifier));
            auto values_identifier = make_intrusive<ASTIdentifier>(ColumnNames::Values);
            values_identifier->setAlias(ColumnNames::Value);
            array_join_expressions->children.push_back(std::move(values_identifier));

            auto array_join = make_intrusive<ASTArrayJoin>();
            array_join->kind = ASTArrayJoin::Kind::Inner;
            array_join->expression_list = array_join_expressions;
            array_join->children.push_back(std::move(array_join_expressions));

            auto array_join_element = make_intrusive<ASTTablesInSelectQueryElement>();
            array_join_element->array_join = array_join;
            array_join_element->children.push_back(std::move(array_join));
            tables->children.push_back(std::move(array_join_element));
        }

        select_query->setExpression(ASTSelectQuery::Expression::TABLES, std::move(tables));

        {
            ASTs conditions;
            conditions.push_back(makeASTFunction(
                "greaterOrEquals",
                make_intrusive<ASTIdentifier>(ColumnNames::Timestamp),
                timeSeriesTimestampToAST(min_time, context.timestamp_data_type)));
            conditions.push_back(makeASTFunction(
                "lessOrEquals",
                make_intrusive<ASTIdentifier>(ColumnNames::Timestamp),
                timeSeriesTimestampToAST(max_time, context.timestamp_data_type)));
            select_query->setExpression(ASTSelectQuery::Expression::WHERE, makeASTForLogicalAnd(std::move(conditions)));
        }

        auto list_of_selects = make_intrusive<ASTExpressionList>();
        list_of_selects->children.push_back(std::move(select_query));
        auto select_with_union_query = make_intrusive<ASTSelectWithUnionQuery>();
        select_with_union_query->list_of_selects = list_of_selects;
        select_with_union_query->children.push_back(std::move(list_of_selects));
        return select_with_union_query;
    }

    SQLQueryPiece fromRangeSelector(std::string_view instant_selector_text,
                                    const Node * node,
                                    ConverterContext & context)
    {
        auto node_range = context.node_range_getter.get(node);
        if (node_range.empty())
            return SQLQueryPiece{node, ResultType::RANGE_VECTOR, StoreMethod::EMPTY};

        SQLQueryPiece res{node, ResultType::RANGE_VECTOR, StoreMethod::RAW_DATA};

        TimestampType min_time = node_range.start_time - node_range.window + 1;
        TimestampType max_time = node_range.end_time;

        if (context.samples_stored_in_chunks)
        {
            /// The scalar RAW_DATA form (via ARRAY JOIN) works for every consumer;
            /// the array form is picked up instead by applyFunctionOverRange().
            res.select_query = makeArrayJoinedSelectorQuery(
                makeSelectorTableFunction(instant_selector_text, min_time, max_time, context), min_time, max_time, context);

            /// SELECT id, timestamps, `values`
            /// FROM timeSeriesSelector(<selector>, <start_time>, <end_time>)
            SelectQueryBuilder builder;
            builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::ID));
            builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Timestamps));
            builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Values));
            builder.from_table_function = makeSelectorTableFunction(instant_selector_text, min_time, max_time, context);
            res.chunked_select_query = builder.getSelectQuery();
        }
        else
        {
            /// SELECT timeSeriesIdToGroup(id) AS group, timestamp, value
            /// FROM timeSeriesSelector(<selector>, <start_time>, <end_time>)
            SelectQueryBuilder builder;

            builder.select_list.push_back(makeASTFunction("timeSeriesIdToGroup", make_intrusive<ASTIdentifier>(ColumnNames::ID)));
            builder.select_list.back()->setAlias(ColumnNames::Group);

            builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Timestamp));
            builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Value));

            builder.from_table_function = makeSelectorTableFunction(instant_selector_text, min_time, max_time, context);

            res.select_query = builder.getSelectQuery();
        }

        return res;
    }
}


SQLQueryPiece fromSelector(const PQT::InstantSelector * instant_selector_node, ConverterContext & context)
{
    auto instant_selector_text = instant_selector_node->toString(*context.promql_tree);
    auto range_selector = fromRangeSelector(instant_selector_text, instant_selector_node, context);
    return applyFunctionOverRange(instant_selector_node, "last_over_time", {std::move(range_selector)}, context);
}


SQLQueryPiece fromSelector(const PQT::RangeSelector * range_selector_node, ConverterContext & context)
{
    auto instant_selector_text = range_selector_node->getInstantSelector()->toString(*context.promql_tree);
    return fromRangeSelector(instant_selector_text, range_selector_node, context);
}

}
