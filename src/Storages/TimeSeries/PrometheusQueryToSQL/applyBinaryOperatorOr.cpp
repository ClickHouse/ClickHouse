#include <Storages/TimeSeries/PrometheusQueryToSQL/applyBinaryOperatorOr.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyBinaryOperatorAnd.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/toVectorGrid.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/transformGroupASTForBinaryOperator.h>


namespace DB::PrometheusQueryToSQL
{

SQLQueryPiece applyBinaryOperatorOr(
    const PQT::BinaryOperator * operator_node, SQLQueryPiece && left_argument, SQLQueryPiece && right_argument, ConverterContext & context)
{
    checkArgumentTypesForSetBinaryOperator(operator_node, left_argument, right_argument, context);

    /// If one of the arguments is empty then we return the other argument.
    if (left_argument.store_method == StoreMethod::EMPTY)
    {
        auto res = std::move(right_argument);
        res.node = operator_node;
        return res;
    }

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

    /// Step 1: Count the non-NULL values per `join_group` on the left side, per timestamp.
    ///
    /// SELECT timeSeriesRemoveAllTagsExcept(group, on_tags) AS join_group,
    ///        countForEach(values) AS join_count
    /// GROUP BY join_group
    /// FROM left
    ///
    String step1;
    {
        SelectQueryBuilder builder;

        bool left_metric_name_dropped = left_argument.metric_name_dropped;
        builder.select_list.push_back(transformGroupASTForBinaryOperator(
            operator_node,
            make_intrusive<ASTIdentifier>(ColumnNames::Group),
            /*drop_metric_name=*/ true,
            left_metric_name_dropped));
        builder.select_list.back()->setAlias(ColumnNames::JoinGroup);

        builder.select_list.push_back(makeASTFunction("countForEach", make_intrusive<ASTIdentifier>(ColumnNames::Values)));
        builder.select_list.back()->setAlias(ColumnNames::JoinCount);

        builder.group_by.push_back(make_intrusive<ASTIdentifier>(ColumnNames::JoinGroup));

        builder.from_table = left;

        ASTPtr step1_ast = builder.getSelectQuery();
        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(step1_ast), SQLSubqueryType::TABLE});
        step1 = context.subqueries.back().name;
    }

    /// Step 2: For each row from the right side, replace values with NULLs at timestamps
    /// where the left side has any value with the matching `join_group`.
    /// Rows whose `join_group` is absent from the left side pass through unchanged.
    ///
    /// SELECT group,
    ///        if(notEmpty(join_count), arrayMap(x, y -> if(x = 0, y, NULL), join_count, values), values) AS values
    /// FROM step1 RIGHT ANY JOIN right
    /// ON join_group == timeSeriesRemoveAllTagsExcept(group, on_tags)
    ///
    String step2;
    {
        SelectQueryBuilder builder;

        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));

        builder.select_list.push_back(makeASTFunction(
            "if",
            makeASTFunction("notEmpty", make_intrusive<ASTIdentifier>(ColumnNames::JoinCount)),
            makeASTFunction(
                "arrayMap",
                makeASTFunction(
                    "lambda",
                    makeASTFunction("tuple", make_intrusive<ASTIdentifier>("x"), make_intrusive<ASTIdentifier>("y")),
                    makeASTFunction(
                        "if",
                        makeASTFunction("equals", make_intrusive<ASTIdentifier>("x"), make_intrusive<ASTLiteral>(0u)),
                        make_intrusive<ASTIdentifier>("y"),
                        make_intrusive<ASTLiteral>(Field{} /* NULL */))),
                make_intrusive<ASTIdentifier>(ColumnNames::JoinCount),
                make_intrusive<ASTIdentifier>(ColumnNames::Values)),
            make_intrusive<ASTIdentifier>(ColumnNames::Values)));

        builder.select_list.back()->setAlias(ColumnNames::Values);

        builder.from_table = step1;
        builder.join_kind = JoinKind::Right;
        builder.join_strictness = JoinStrictness::Any;
        builder.join_table = right;

        bool right_metric_name_dropped = right_argument.metric_name_dropped;
        builder.join_on = makeASTFunction(
            "equals",
            make_intrusive<ASTIdentifier>(ColumnNames::JoinGroup),
            transformGroupASTForBinaryOperator(
                operator_node,
                make_intrusive<ASTIdentifier>(ColumnNames::Group),
                /*drop_metric_name=*/ true,
                right_metric_name_dropped));

        ASTPtr step2_ast = builder.getSelectQuery();
        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(step2_ast), SQLSubqueryType::TABLE});
        step2 = context.subqueries.back().name;
    }

    /// Step 3: Merge the original left rows with the filtered right rows from step 2,
    /// picking element-wise non-NULL where both sides have a row.
    ///
    /// SELECT
    ///   if(notEmpty(left.values), left.group, step2.group) AS group,
    ///   multiIf(
    ///     notEmpty(left.values) AND notEmpty(step2.values),
    ///        arrayMap((a, b) -> if(isNotNull(a), a, b), left.values, step2.values),
    ///     notEmpty(left.values),
    ///        left.values,
    ///     step2.values) AS values
    /// FROM left FULL ALL JOIN step2
    /// ON left.group == step2.group
    ///
    ASTPtr step3;
    {
        SelectQueryBuilder builder;

        builder.select_list.push_back(makeASTFunction(
            "if",
            makeASTFunction("empty", make_intrusive<ASTIdentifier>(Strings{left, ColumnNames::Values})),
            make_intrusive<ASTIdentifier>(Strings{step2, ColumnNames::Group}),
            make_intrusive<ASTIdentifier>(Strings{left, ColumnNames::Group})));
        builder.select_list.back()->setAlias(ColumnNames::Group);

        builder.select_list.push_back(makeASTFunction(
            "multiIf",
            makeASTFunction(
                "and",
                makeASTFunction("notEmpty", make_intrusive<ASTIdentifier>(Strings{left, ColumnNames::Values})),
                makeASTFunction("notEmpty", make_intrusive<ASTIdentifier>(Strings{step2, ColumnNames::Values}))),
            makeASTFunction(
                "arrayMap",
                makeASTFunction(
                    "lambda",
                    makeASTFunction("tuple", make_intrusive<ASTIdentifier>("a"), make_intrusive<ASTIdentifier>("b")),
                    makeASTFunction(
                        "if",
                        makeASTFunction("isNotNull", make_intrusive<ASTIdentifier>("a")),
                        make_intrusive<ASTIdentifier>("a"),
                        make_intrusive<ASTIdentifier>("b"))),
                make_intrusive<ASTIdentifier>(Strings{left, ColumnNames::Values}),
                make_intrusive<ASTIdentifier>(Strings{step2, ColumnNames::Values})),
            makeASTFunction("notEmpty", make_intrusive<ASTIdentifier>(Strings{left, ColumnNames::Values})),
            make_intrusive<ASTIdentifier>(Strings{left, ColumnNames::Values}),
            make_intrusive<ASTIdentifier>(Strings{step2, ColumnNames::Values})));
        builder.select_list.back()->setAlias(ColumnNames::Values);

        builder.from_table = left;
        builder.join_kind = JoinKind::Full;
        builder.join_strictness = JoinStrictness::All;
        builder.join_table = step2;

        builder.join_on = makeASTFunction(
            "equals",
            make_intrusive<ASTIdentifier>(Strings{left, ColumnNames::Group}),
            make_intrusive<ASTIdentifier>(Strings{step2, ColumnNames::Group}));

        step3 = builder.getSelectQuery();
    }

    SQLQueryPiece res{operator_node, ResultType::INSTANT_VECTOR, StoreMethod::VECTOR_GRID};
    res.select_query = std::move(step3);
    res.metric_name_dropped = left_argument.metric_name_dropped && right_argument.metric_name_dropped;

    res.start_time = left_argument.start_time;
    res.end_time = left_argument.end_time;
    res.step = left_argument.step;

    return res;
}

}
