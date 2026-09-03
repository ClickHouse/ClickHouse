#include <Storages/TimeSeries/PrometheusQueryToSQL/applyBinaryOperatorUnless.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyBinaryOperatorSet.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/toVectorGrid.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/transformGroupASTForBinaryOperator.h>


namespace DB::PrometheusQueryToSQL
{

SQLQueryPiece applyBinaryOperatorUnless(
    const PrometheusQueryTree::BinaryOperator * operator_node,
    SQLQueryPiece && left_argument,
    SQLQueryPiece && right_argument,
    ConverterContext & context)
{
    checkArgumentTypesForSetBinaryOperator(operator_node, left_argument, right_argument, context);

    /// If the left argument is empty then the result is also empty.
    if (left_argument.store_method == StoreMethod::EMPTY)
    {
        SQLQueryPiece res{operator_node, ResultType::INSTANT_VECTOR, StoreMethod::EMPTY};
        res.value_data_type = mergeValueDataType(left_argument.value_data_type, right_argument.value_data_type);
        return res;
    }

    /// If the right argument is empty then we return the left argument.
    if (right_argument.store_method == StoreMethod::EMPTY)
    {
        auto res = std::move(left_argument);
        res.node = operator_node;
        return res;
    }

    left_argument = toVectorGrid(std::move(left_argument), context);
    context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(left_argument.select_query), SQLSubqueryType::TABLE});
    String left = context.subqueries.back().name;

    right_argument = toVectorGrid(std::move(right_argument), context);
    context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(right_argument.select_query), SQLSubqueryType::TABLE});
    String right = context.subqueries.back().name;

    /// Step 1:
    /// SELECT timeSeriesRemoveAllTagsExcept(group, on_tags) AS join_group,
    ///        groupBitOrForEach(arrayMap(x -> isNotNull(x), values)) AS join_presence
    /// GROUP BY join_group
    /// FROM right
    ///
    String step1;
    {
        SelectQueryBuilder builder;

        bool right_metric_name_dropped = right_argument.metric_name_dropped;
        builder.select_list.push_back(transformGroupASTForBinaryOperator(
            operator_node,
            make_intrusive<ASTIdentifier>(ColumnNames::Group),
            /*drop_metric_name=*/ true,
            right_metric_name_dropped));
        builder.select_list.back()->setAlias(ColumnNames::JoinGroup);

        builder.select_list.push_back(makePresenceMask(make_intrusive<ASTIdentifier>(ColumnNames::Values)));
        builder.select_list.back()->setAlias(ColumnNames::JoinPresence);

        builder.group_by.push_back(make_intrusive<ASTIdentifier>(ColumnNames::JoinGroup));

        builder.from_table = right;

        ASTPtr step1_ast = builder.getSelectQuery();
        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(step1_ast), SQLSubqueryType::TABLE});
        step1 = context.subqueries.back().name;
    }

    /// Step 2:
    /// SELECT group,
    ///        if(notEmpty(join_presence), arrayMap(x, y -> if(y = 0, x, NULL), values, join_presence), values) AS values
    /// FROM left LEFT ANY JOIN step1
    /// ON timeSeriesRemoveAllTagsExcept(group, on_tags) == join_group
    ///
    ASTPtr step2;
    {
        SelectQueryBuilder builder;

        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));

        builder.select_list.push_back(makeASTFunction(
            "if",
            makeASTFunction("notEmpty", make_intrusive<ASTIdentifier>(ColumnNames::JoinPresence)),
            makeASTFunction(
                "arrayMap",
                makeASTFunction(
                    "lambda",
                    makeASTFunction("tuple", make_intrusive<ASTIdentifier>("x"), make_intrusive<ASTIdentifier>("y")),
                    makeASTFunction(
                        "if",
                        makeASTFunction("equals", make_intrusive<ASTIdentifier>("y"), make_intrusive<ASTLiteral>(0u)),
                        make_intrusive<ASTIdentifier>("x"),
                        make_intrusive<ASTLiteral>(Field{} /* NULL */))),
                make_intrusive<ASTIdentifier>(ColumnNames::Values),
                make_intrusive<ASTIdentifier>(ColumnNames::JoinPresence)),
            make_intrusive<ASTIdentifier>(ColumnNames::Values)));

        builder.select_list.back()->setAlias(ColumnNames::Values);

        builder.from_table = left;

        builder.join_kind = JoinKind::Left;
        builder.join_strictness = JoinStrictness::Any;
        builder.join_table = step1;

        bool left_metric_name_dropped = left_argument.metric_name_dropped;
        builder.join_on = makeASTFunction(
            "equals",
            transformGroupASTForBinaryOperator(
                operator_node,
                make_intrusive<ASTIdentifier>(ColumnNames::Group),
                /*drop_metric_name=*/ true,
                left_metric_name_dropped),
            make_intrusive<ASTIdentifier>(ColumnNames::JoinGroup));

        step2 = builder.getSelectQuery();
    }

    SQLQueryPiece res{operator_node, ResultType::INSTANT_VECTOR, StoreMethod::VECTOR_GRID};
    res.select_query = std::move(step2);
    res.metric_name_dropped = left_argument.metric_name_dropped;

    res.start_time = left_argument.start_time;
    res.end_time = left_argument.end_time;
    res.step = left_argument.step;
    /// The values of the result come from the left argument only, so its value type wins.
    res.value_data_type = left_argument.value_data_type;

    return res;
}

}
