#include <Storages/TimeSeries/PrometheusQueryToSQL/fromSelector.h>

#include <DataTypes/IDataType.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/NodeEvaluationRange.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFunctionOverRange.h>
#include <Storages/TimeSeries/TimeSeriesNativeHistograms.h>
#include <Storages/TimeSeries/timeSeriesTypesToAST.h>


namespace DB::PrometheusQueryToSQL
{

namespace
{
    /// Makes a SELECT query reading from a table function.
    ASTPtr makeSelectorArm(ASTs select_list, ASTPtr table_function)
    {
        auto select_query = make_intrusive<ASTSelectQuery>();

        {
            auto select_list_exp = make_intrusive<ASTExpressionList>();
            select_list_exp->children = std::move(select_list);
            select_query->setExpression(ASTSelectQuery::Expression::SELECT, std::move(select_list_exp));
        }

        {
            auto table_exp = make_intrusive<ASTTableExpression>();
            table_exp->table_function = std::move(table_function);
            table_exp->children.push_back(table_exp->table_function);

            auto table = make_intrusive<ASTTablesInSelectQueryElement>();
            table->table_expression = table_exp;
            table->children.push_back(std::move(table_exp));

            auto tables = make_intrusive<ASTTablesInSelectQuery>();
            tables->children.push_back(std::move(table));

            select_query->setExpression(ASTSelectQuery::Expression::TABLES, std::move(tables));
        }

        return select_query;
    }

    /// Combines two SELECT queries into one: <float_arm> UNION ALL <histogram_arm>.
    ASTPtr makeUnionAll(ASTPtr float_arm, ASTPtr histogram_arm)
    {
        auto select_with_union_query = make_intrusive<ASTSelectWithUnionQuery>();
        select_with_union_query->union_mode = SelectUnionMode::UNION_ALL;
        select_with_union_query->is_normalized = true;

        auto list_of_selects = make_intrusive<ASTExpressionList>();
        list_of_selects->children.push_back(std::move(float_arm));
        list_of_selects->children.push_back(std::move(histogram_arm));
        select_with_union_query->children.push_back(list_of_selects);
        select_with_union_query->list_of_selects = select_with_union_query->children.back();

        return select_with_union_query;
    }

    SQLQueryPiece fromRangeSelector(std::string_view instant_selector_text,
                                    const Node * node,
                                    ConverterContext & context)
    {
        auto node_range = context.node_range_getter.get(node);
        if (node_range.empty())
            return SQLQueryPiece{node, ResultType::RANGE_VECTOR, StoreMethod::EMPTY};

        TimestampType min_time = node_range.start_time - node_range.window + 1;
        TimestampType max_time = node_range.end_time;

        auto make_table_function = [&](std::string_view function_name)
        {
            return makeASTFunction(
                function_name,
                make_intrusive<ASTLiteral>(context.time_series_storage_id.getDatabaseName()),
                make_intrusive<ASTLiteral>(context.time_series_storage_id.getTableName()),
                make_intrusive<ASTLiteral>(String{instant_selector_text}),
                timeSeriesTimestampToAST(min_time, context.timestamp_data_type),
                timeSeriesTimestampToAST(max_time, context.timestamp_data_type));
        };

        if (!context.storage_has_native_histograms)
        {
            SQLQueryPiece res{node, ResultType::RANGE_VECTOR, StoreMethod::RAW_DATA};

            /// SELECT timeSeriesIdToGroup(id) AS group, timestamp, value
            /// FROM timeSeriesSelector(<storage>, <selector>, <min_time>, <max_time>)
            SelectQueryBuilder builder;

            builder.select_list.push_back(makeASTFunction("timeSeriesIdToGroup", make_intrusive<ASTIdentifier>(ColumnNames::ID)));
            builder.select_list.back()->setAlias(ColumnNames::Group);

            builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Timestamp));
            builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Value));

            builder.from_table_function = make_table_function("timeSeriesSelector");

            res.select_query = builder.getSelectQuery();
            return res;
        }

        SQLQueryPiece res{node, ResultType::RANGE_VECTOR, StoreMethod::HISTOGRAM_RAW_DATA};

        /// A combined selector stream (StoreMethod::HISTOGRAM_RAW_DATA): the float arm UNION ALL the histogram arm.
        /// UNION ALL unifies columns by position, so every fabricated default column below is an explicit cast to the exact column type.
        ASTs float_arm_select_list;
        {
            float_arm_select_list.push_back(makeASTFunction("timeSeriesIdToGroup", make_intrusive<ASTIdentifier>(ColumnNames::ID)));
            float_arm_select_list.back()->setAlias(ColumnNames::Group);

            float_arm_select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Timestamp));
            float_arm_select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Value));

            /// The float arm carries no histograms: the payload columns hold default values.
            for (const auto & [name, type] : getTimeSeriesHistogramPayloadColumns())
            {
                ASTPtr default_value = WhichDataType(type).isArray()
                    ? make_intrusive<ASTLiteral>(Array{})
                    : make_intrusive<ASTLiteral>(UInt64{0});
                float_arm_select_list.push_back(makeASTFunction(
                    "_CAST", std::move(default_value), make_intrusive<ASTLiteral>(type->getName())));
                float_arm_select_list.back()->setAlias(name);
            }

            float_arm_select_list.push_back(makeASTFunction(
                "_CAST", make_intrusive<ASTLiteral>(UInt64{0}), make_intrusive<ASTLiteral>("UInt8")));
            float_arm_select_list.back()->setAlias(ColumnNames::IsHistogram);
        }

        ASTs histogram_arm_select_list;
        {
            histogram_arm_select_list.push_back(makeASTFunction("timeSeriesIdToGroup", make_intrusive<ASTIdentifier>(ColumnNames::ID)));
            histogram_arm_select_list.back()->setAlias(ColumnNames::Group);

            histogram_arm_select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Timestamp));

            /// The histogram arm carries no float values: `value` is a dummy zero of the scalar data type.
            histogram_arm_select_list.push_back(makeASTFunction(
                "_CAST", make_intrusive<ASTLiteral>(UInt64{0}), make_intrusive<ASTLiteral>(context.scalar_data_type->getName())));
            histogram_arm_select_list.back()->setAlias(ColumnNames::Value);

            for (const auto & [name, type] : getTimeSeriesHistogramPayloadColumns())
                histogram_arm_select_list.push_back(make_intrusive<ASTIdentifier>(name));

            histogram_arm_select_list.push_back(makeASTFunction(
                "_CAST", make_intrusive<ASTLiteral>(UInt64{1}), make_intrusive<ASTLiteral>("UInt8")));
            histogram_arm_select_list.back()->setAlias(ColumnNames::IsHistogram);
        }

        res.select_query = makeUnionAll(
            makeSelectorArm(std::move(float_arm_select_list), make_table_function("timeSeriesSelector")),
            makeSelectorArm(std::move(histogram_arm_select_list), make_table_function("timeSeriesHistogramSelector")));
        return res;
    }
}


SQLQueryPiece fromSelector(const PrometheusQueryTree::InstantSelector * instant_selector_node, ConverterContext & context)
{
    auto instant_selector_text = instant_selector_node->toString(*context.promql_tree);
    auto range_selector = fromRangeSelector(instant_selector_text, instant_selector_node, context);
    return applyFunctionOverRange(instant_selector_node, "last_over_time", {std::move(range_selector)}, context);
}


SQLQueryPiece fromSelector(const PrometheusQueryTree::RangeSelector * range_selector_node, ConverterContext & context)
{
    auto instant_selector_text = range_selector_node->getInstantSelector()->toString(*context.promql_tree);
    return fromRangeSelector(instant_selector_text, range_selector_node, context);
}

}
