#include <Storages/TimeSeries/PrometheusQueryToSQL/applyBinaryOperatorOr.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTWindowDefinition.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyBinaryOperatorSet.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/toVectorGrid.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/transformGroupASTForBinaryOperator.h>
#include <base/defines.h>


namespace DB::PrometheusQueryToSQL
{

namespace
{

constexpr const char * sideColumn = "side";
constexpr const char * leftGroupColumn = "left_group";
constexpr const char * rightGroupColumn = "right_group";
constexpr const char * leftValuesColumn = "left_values";
constexpr const char * rightValuesColumn = "right_values";

String storeSubquery(ASTPtr query, ConverterContext & context)
{
    context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(query), SQLSubqueryType::TABLE});
    return context.subqueries.back().name;
}

ASTPtr takeSingleSelect(ASTPtr query)
{
    auto * union_query = query->as<ASTSelectWithUnionQuery>();
    chassert(union_query && union_query->list_of_selects && union_query->list_of_selects->children.size() == 1);
    ASTPtr select = std::move(union_query->list_of_selects->children[0]);
    union_query->list_of_selects->children.clear();
    return select;
}

ASTPtr makeUnionAll(ASTPtr left_select, ASTPtr right_select)
{
    auto list = make_intrusive<ASTExpressionList>();
    list->children.push_back(std::move(left_select));
    list->children.push_back(std::move(right_select));

    auto query = make_intrusive<ASTSelectWithUnionQuery>();
    query->list_of_selects = list;
    query->children.push_back(std::move(list));
    query->union_mode = SelectUnionMode::UNION_ALL;
    query->is_normalized = true;
    return query;
}

ASTPtr equalsSide(UInt64 side)
{
    return makeASTFunction(
        "equals",
        make_intrusive<ASTIdentifier>(sideColumn),
        make_intrusive<ASTLiteral>(side));
}

ASTPtr makeTaggedSide(const String & table, UInt64 side, ASTPtr join_group)
{
    SelectQueryBuilder builder;
    builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));
    builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Values));

    join_group->setAlias(ColumnNames::JoinGroup);
    builder.select_list.push_back(std::move(join_group));

    auto side_literal = make_intrusive<ASTLiteral>(side);
    side_literal->setAlias(sideColumn);
    builder.select_list.push_back(std::move(side_literal));

    builder.from_table = table;
    return takeSingleSelect(builder.getSelectQuery());
}

/// groupBitOrForEach(...) OVER (PARTITION BY join_group)
ASTPtr makePresenceWindow()
{
    auto left_bits = makeASTFunction(
        "arrayMap",
        makeASTFunction(
            "lambda",
            makeASTFunction("tuple", make_intrusive<ASTIdentifier>("x")),
            makeASTFunction("isNotNull", make_intrusive<ASTIdentifier>("x"))),
        make_intrusive<ASTIdentifier>(ColumnNames::Values));

    auto right_bits = makeASTFunction(
        "arrayWithConstant",
        makeASTFunction("length", make_intrusive<ASTIdentifier>(ColumnNames::Values)),
        makeASTFunction("toUInt8", make_intrusive<ASTLiteral>(0u)));

    auto function = makeASTFunction(
        "groupBitOrForEach",
        makeASTFunction("if", equalsSide(0), std::move(left_bits), std::move(right_bits)));
    function->setIsWindowFunction(true);
    function->setKind(ASTFunction::Kind::WINDOW_FUNCTION);

    auto partition_by = make_intrusive<ASTExpressionList>();
    partition_by->children.push_back(make_intrusive<ASTIdentifier>(ColumnNames::JoinGroup));

    auto window = make_intrusive<ASTWindowDefinition>();
    window->partition_by = partition_by;
    window->children.push_back(std::move(partition_by));

    function->window_definition = window;
    function->children.push_back(std::move(window));
    function->setAlias(ColumnNames::JoinPresence);
    return function;
}

/// SELECT group, values, side,
///        groupBitOrForEach(if(side = 0, arrayMap(x -> isNotNull(x), values), arrayWithConstant(length(values), toUInt8(0))))
///          OVER (PARTITION BY join_group) AS join_presence
/// FROM tagged
ASTPtr makePresenceSelect(const String & tagged)
{
    SelectQueryBuilder builder;
    builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));
    builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Values));
    builder.select_list.push_back(make_intrusive<ASTIdentifier>(sideColumn));
    builder.select_list.push_back(makePresenceWindow());
    builder.from_table = tagged;
    return builder.getSelectQuery();
}

ASTPtr makeRightMaskedValues()
{
    return makeASTFunction(
        "if",
        makeASTFunction("notEmpty", make_intrusive<ASTIdentifier>(ColumnNames::JoinPresence)),
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
            make_intrusive<ASTIdentifier>(ColumnNames::JoinPresence),
            make_intrusive<ASTIdentifier>(ColumnNames::Values)),
        make_intrusive<ASTIdentifier>(ColumnNames::Values));
}

/// One output row per series. Left values stay intact. Right values are NULL
/// at timestamps where any left series with the same join_group has a sample.
ASTPtr makePerSeriesSelect(const String & presence)
{
    SelectQueryBuilder builder;

    auto left_group = makeASTFunction("anyIf", make_intrusive<ASTIdentifier>(ColumnNames::Group), equalsSide(0));
    left_group->setAlias(leftGroupColumn);
    builder.select_list.push_back(std::move(left_group));

    auto left_values = makeASTFunction("anyIf", make_intrusive<ASTIdentifier>(ColumnNames::Values), equalsSide(0));
    left_values->setAlias(leftValuesColumn);
    builder.select_list.push_back(std::move(left_values));

    auto right_group = makeASTFunction("anyIf", make_intrusive<ASTIdentifier>(ColumnNames::Group), equalsSide(1));
    right_group->setAlias(rightGroupColumn);
    builder.select_list.push_back(std::move(right_group));

    auto right_values = makeASTFunction("anyIf", makeRightMaskedValues(), equalsSide(1));
    right_values->setAlias(rightValuesColumn);
    builder.select_list.push_back(std::move(right_values));

    builder.group_by.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));
    builder.from_table = presence;
    return builder.getSelectQuery();
}

/// SELECT
///   if(empty(left_values), right_group, left_group) AS group,
///   multiIf(
///     notEmpty(left_values) AND notEmpty(right_values),
///        arrayMap((a, b) -> if(isNotNull(a), a, b), left_values, right_values),
///     notEmpty(left_values),
///        left_values,
///     right_values) AS values
/// FROM per_series
ASTPtr makeMergedSelect(const String & per_series)
{
    SelectQueryBuilder builder;

    builder.select_list.push_back(makeASTFunction(
        "if",
        makeASTFunction("empty", make_intrusive<ASTIdentifier>(leftValuesColumn)),
        make_intrusive<ASTIdentifier>(rightGroupColumn),
        make_intrusive<ASTIdentifier>(leftGroupColumn)));
    builder.select_list.back()->setAlias(ColumnNames::Group);

    builder.select_list.push_back(makeASTFunction(
        "multiIf",
        makeASTFunction(
            "and",
            makeASTFunction("notEmpty", make_intrusive<ASTIdentifier>(leftValuesColumn)),
            makeASTFunction("notEmpty", make_intrusive<ASTIdentifier>(rightValuesColumn))),
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
            make_intrusive<ASTIdentifier>(leftValuesColumn),
            make_intrusive<ASTIdentifier>(rightValuesColumn)),
        makeASTFunction("notEmpty", make_intrusive<ASTIdentifier>(leftValuesColumn)),
        make_intrusive<ASTIdentifier>(leftValuesColumn),
        make_intrusive<ASTIdentifier>(rightValuesColumn)));
    builder.select_list.back()->setAlias(ColumnNames::Values);

    builder.from_table = per_series;
    return builder.getSelectQuery();
}

}


SQLQueryPiece applyBinaryOperatorOr(
    const PrometheusQueryTree::BinaryOperator * operator_node,
    SQLQueryPiece && left_argument,
    SQLQueryPiece && right_argument,
    ConverterContext & context)
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
    String left = storeSubquery(std::move(left_argument.select_query), context);

    right_argument = toVectorGrid(std::move(right_argument), context);
    String right = storeSubquery(std::move(right_argument.select_query), context);

    /// Each grid is a table expression once.
    bool left_metric_name_dropped = left_argument.metric_name_dropped;
    ASTPtr left_join_group = transformGroupASTForBinaryOperator(
        operator_node,
        make_intrusive<ASTIdentifier>(ColumnNames::Group),
        /*drop_metric_name=*/ true,
        left_metric_name_dropped);

    bool right_metric_name_dropped = right_argument.metric_name_dropped;
    ASTPtr right_join_group = transformGroupASTForBinaryOperator(
        operator_node,
        make_intrusive<ASTIdentifier>(ColumnNames::Group),
        /*drop_metric_name=*/ true,
        right_metric_name_dropped);

    ASTPtr tagged = makeUnionAll(
        makeTaggedSide(left, 0, std::move(left_join_group)),
        makeTaggedSide(right, 1, std::move(right_join_group)));
    String tagged_name = storeSubquery(std::move(tagged), context);
    String presence_name = storeSubquery(makePresenceSelect(tagged_name), context);
    String per_series_name = storeSubquery(makePerSeriesSelect(presence_name), context);

    SQLQueryPiece res{operator_node, ResultType::INSTANT_VECTOR, StoreMethod::VECTOR_GRID};
    res.select_query = makeMergedSelect(per_series_name);
    res.metric_name_dropped = left_argument.metric_name_dropped && right_argument.metric_name_dropped;

    res.start_time = left_argument.start_time;
    res.end_time = left_argument.end_time;
    res.step = left_argument.step;

    return res;
}

}
