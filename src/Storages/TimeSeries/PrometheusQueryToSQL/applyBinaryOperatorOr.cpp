#include <Storages/TimeSeries/PrometheusQueryToSQL/applyBinaryOperatorOr.h>

#include <Core/Field.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
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

constexpr const char * leftGroupsColumn = "left_groups";
constexpr const char * leftValuesColumn = "left_values";
constexpr const char * rightGroupsColumn = "right_groups";
constexpr const char * rightValuesColumn = "right_values";
constexpr const char * leftMapColumn = "left_map";
constexpr const char * rightMapColumn = "right_map";
constexpr const char * seriesColumn = "series";

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

ASTPtr wrapSelect(ASTPtr select)
{
    auto list = make_intrusive<ASTExpressionList>();
    list->children.push_back(std::move(select));

    auto query = make_intrusive<ASTSelectWithUnionQuery>();
    query->list_of_selects = list;
    query->children.push_back(std::move(list));
    return query;
}

ASTPtr id(const char * name)
{
    return make_intrusive<ASTIdentifier>(name);
}

ASTPtr col(const String & table, const char * name)
{
    return make_intrusive<ASTIdentifier>(Strings{table, name});
}

ASTPtr lambda1(const char * arg, ASTPtr body)
{
    return makeASTFunction(
        "lambda",
        makeASTFunction("tuple", id(arg)),
        std::move(body));
}

ASTPtr lambda2(const char * arg1, const char * arg2, ASTPtr body)
{
    return makeASTFunction(
        "lambda",
        makeASTFunction("tuple", id(arg1), id(arg2)),
        std::move(body));
}

ASTPtr nullToEmpty(ASTPtr expr, const char * type)
{
    return makeASTFunction(
        "ifNull",
        std::move(expr),
        makeASTFunction("CAST", make_intrusive<ASTLiteral>(Array{}), make_intrusive<ASTLiteral>(type)));
}

/// Right samples stay only at steps where no left series in the match group has a value.
ASTPtr maskValues(const ASTPtr & presence, const ASTPtr & values)
{
    auto masked = makeASTFunction(
        "arrayMap",
        lambda2(
            "x",
            "y",
            makeASTFunction(
                "if",
                makeASTFunction("equals", id("x"), make_intrusive<ASTLiteral>(0u)),
                id("y"),
                make_intrusive<ASTLiteral>(Field{}))),
        presence->clone(),
        values->clone());

    return makeASTFunction(
        "if",
        makeASTFunction("notEmpty", presence->clone()),
        std::move(masked),
        values->clone());
}

ASTPtr preferLeft(ASTPtr left_values, ASTPtr right_values)
{
    return makeASTFunction(
        "arrayMap",
        lambda2(
            "a",
            "b",
            makeASTFunction(
                "if",
                makeASTFunction("isNotNull", id("a")),
                id("a"),
                id("b"))),
        std::move(left_values),
        std::move(right_values));
}

/// One row per join_group. groupArray(group) and groupArray(values) follow the same row order.
ASTPtr makePackedSide(const String & table, ASTPtr join_group, bool is_left)
{
    SelectQueryBuilder builder;

    join_group->setAlias(ColumnNames::JoinGroup);
    builder.select_list.push_back(std::move(join_group));

    if (is_left)
    {
        builder.select_list.push_back(makePresenceMask(make_intrusive<ASTIdentifier>(ColumnNames::Values)));
        builder.select_list.back()->setAlias(ColumnNames::JoinPresence);
    }

    const char * groups_alias = is_left ? leftGroupsColumn : rightGroupsColumn;
    const char * values_alias = is_left ? leftValuesColumn : rightValuesColumn;

    auto groups = makeASTFunction("groupArray", make_intrusive<ASTIdentifier>(ColumnNames::Group));
    groups->setAlias(groups_alias);
    builder.select_list.push_back(std::move(groups));

    auto values = makeASTFunction("groupArray", make_intrusive<ASTIdentifier>(ColumnNames::Values));
    values->setAlias(values_alias);
    builder.select_list.push_back(std::move(values));

    builder.group_by.push_back(make_intrusive<ASTIdentifier>(ColumnNames::JoinGroup));
    builder.from_table = table;
    return builder.getSelectQuery();
}

ASTPtr makeJoinedSelect(const String & left, const String & right)
{
    auto left_groups = nullToEmpty(col(left, leftGroupsColumn), "Array(UInt64)");
    auto left_values = nullToEmpty(col(left, leftValuesColumn), "Array(Array(Nullable(Float64)))");
    auto presence = nullToEmpty(col(left, ColumnNames::JoinPresence), "Array(UInt8)");
    auto right_groups = nullToEmpty(col(right, rightGroupsColumn), "Array(UInt64)");
    auto right_values = nullToEmpty(col(right, rightValuesColumn), "Array(Array(Nullable(Float64)))");

    auto masked_right = makeASTFunction(
        "arrayMap",
        lambda1("rv", maskValues(presence, id("rv"))),
        right_values->clone());

    SelectQueryBuilder builder;

    auto add = [&](ASTPtr expr, const char * alias)
    {
        expr->setAlias(alias);
        builder.select_list.push_back(std::move(expr));
    };

    ASTPtr left_groups_key = left_groups->clone();
    ASTPtr right_groups_key = right_groups->clone();

    add(std::move(left_groups_key), leftGroupsColumn);
    add(std::move(left_values), leftValuesColumn);
    add(std::move(right_groups_key), rightGroupsColumn);
    ASTPtr left_map_key = left_groups->clone();
    add(makeASTFunction("mapFromArrays", std::move(left_map_key), std::move(left_groups)), leftMapColumn);
    add(makeASTFunction("mapFromArrays", std::move(right_groups), std::move(masked_right)), rightMapColumn);

    builder.from_table = left;
    builder.join_kind = JoinKind::Full;
    builder.join_strictness = JoinStrictness::All;
    builder.join_table = right;
    builder.join_on = makeASTFunction(
        "equals",
        col(left, ColumnNames::JoinGroup),
        col(right, ColumnNames::JoinGroup));

    return builder.getSelectQuery();
}

ASTPtr makeLeftSeries()
{
    auto lookup = makeASTFunction("arrayElement", id(rightMapColumn), id("g"));
    auto value = makeASTFunction(
        "if",
        makeASTFunction("mapContains", id(rightMapColumn), id("g")),
        preferLeft(id("lv"), std::move(lookup)),
        id("lv"));

    return makeASTFunction(
        "arrayMap",
        lambda2("g", "lv", makeASTFunction("tuple", id("g"), std::move(value))),
        id(leftGroupsColumn),
        id(leftValuesColumn));
}

ASTPtr makeRightSeries()
{
    return makeASTFunction(
        "arrayMap",
        lambda1("g", makeASTFunction("tuple", id("g"), makeASTFunction("arrayElement", id(rightMapColumn), id("g")))),
        makeASTFunction(
            "arrayFilter",
            lambda1("g", makeASTFunction("not", makeASTFunction("mapContains", id(leftMapColumn), id("g")))),
            id(rightGroupsColumn)));
}

void addArrayJoin(ASTSelectQuery & select, ASTPtr array_expr)
{
    array_expr->setAlias(seriesColumn);

    auto list = make_intrusive<ASTExpressionList>();
    list->children.push_back(std::move(array_expr));

    auto array_join = make_intrusive<ASTArrayJoin>();
    array_join->kind = ASTArrayJoin::Kind::Inner;
    array_join->expression_list = list;
    array_join->children.push_back(list);

    auto element = make_intrusive<ASTTablesInSelectQueryElement>();
    element->array_join = array_join;
    element->children.push_back(array_join);

    select.refTables()->children.push_back(element);
}

/// SELECT tupleElement(series, 1) AS group, tupleElement(series, 2) AS values
/// FROM joined
/// ARRAY JOIN arrayConcat(left_series, right_only_series) AS series
ASTPtr makeEmitSelect(const String & joined)
{
    SelectQueryBuilder builder;

    builder.select_list.push_back(makeASTFunction(
        "tupleElement",
        id(seriesColumn),
        make_intrusive<ASTLiteral>(1u)));
    builder.select_list.back()->setAlias(ColumnNames::Group);

    builder.select_list.push_back(makeASTFunction(
        "tupleElement",
        id(seriesColumn),
        make_intrusive<ASTLiteral>(2u)));
    builder.select_list.back()->setAlias(ColumnNames::Values);

    builder.from_table = joined;

    ASTPtr select = takeSingleSelect(builder.getSelectQuery());
    addArrayJoin(*select->as<ASTSelectQuery>(), makeASTFunction("arrayConcat", makeLeftSeries(), makeRightSeries()));
    return wrapSelect(std::move(select));
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

    String left_packed = storeSubquery(makePackedSide(left, std::move(left_join_group), /*is_left=*/ true), context);
    String right_packed = storeSubquery(makePackedSide(right, std::move(right_join_group), /*is_left=*/ false), context);
    String joined = storeSubquery(makeJoinedSelect(left_packed, right_packed), context);

    SQLQueryPiece res{operator_node, ResultType::INSTANT_VECTOR, StoreMethod::VECTOR_GRID};
    res.select_query = makeEmitSelect(joined);
    res.metric_name_dropped = left_argument.metric_name_dropped && right_argument.metric_name_dropped;

    res.start_time = left_argument.start_time;
    res.end_time = left_argument.end_time;
    res.step = left_argument.step;

    return res;
}

}
