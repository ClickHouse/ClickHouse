#include <Storages/TimeSeries/PrometheusQueryToSQL/applyBinaryOperatorOr.h>

#include <Core/Field.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTWindowDefinition.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyBinaryOperatorSet.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/toVectorGrid.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/transformGroupASTForBinaryOperator.h>


namespace DB::PrometheusQueryToSQL
{

namespace
{

constexpr const char * leftValuesColumn = "left_values";
constexpr const char * rightValuesColumn = "right_values";
constexpr const char * leftJoinGroupColumn = "left_join_group";
constexpr const char * rightJoinGroupColumn = "right_join_group";
constexpr const char * seriesGroupColumn = "series_group";

String storeSubquery(ASTPtr query, ConverterContext & context)
{
    context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(query), SQLSubqueryType::TABLE});
    return context.subqueries.back().name;
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

ASTPtr emptyValues()
{
    return makeASTFunction(
        "CAST",
        make_intrusive<ASTLiteral>(Array{}),
        make_intrusive<ASTLiteral>("Array(Nullable(Float64))"));
}

ASTPtr valueOrEmpty(ASTPtr values)
{
    return makeASTFunction("ifNull", std::move(values), emptyValues());
}

/// One match group needs no PARTITION BY, and without it the window needs no sort.
void setWindowPartition(ASTPtr function, ASTPtr partition_expr)
{
    auto * function_node = function->as<ASTFunction>();
    function_node->setIsWindowFunction(true);
    function_node->setKind(ASTFunction::Kind::WINDOW_FUNCTION);

    auto window = make_intrusive<ASTWindowDefinition>();
    if (partition_expr)
    {
        auto partition_by = make_intrusive<ASTExpressionList>();
        partition_by->children.push_back(std::move(partition_expr));
        window->partition_by = partition_by;
        window->children.push_back(std::move(partition_by));
    }
    window->frame_is_default = false;
    window->frame_type = WindowFrame::FrameType::ROWS;
    window->frame_begin_type = WindowFrame::BoundaryType::Unbounded;
    window->frame_begin_preceding = true;
    window->frame_end_type = WindowFrame::BoundaryType::Unbounded;
    window->frame_end_preceding = false;

    function_node->window_definition = window;
    function_node->children.push_back(std::move(window));
}

/// Right samples stay only at steps where no left series in the match group has a value.
/// An empty presence means the match group has no left series, so the right values pass through.
ASTPtr maskValues(ASTPtr presence, ASTPtr values)
{
    return makeASTFunction(
        "if",
        makeASTFunction("empty", presence->clone()),
        values->clone(),
        makeASTFunction(
            "arrayMap",
            lambda2(
                "x",
                "y",
                makeASTFunction(
                    "if",
                    makeASTFunction("equals", id("x"), make_intrusive<ASTLiteral>(0u)),
                    id("y"),
                    make_intrusive<ASTLiteral>(Field{}))),
            std::move(presence),
            std::move(values)));
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

/// The match key is a column of one side. The two sides use different names so a join cannot mix them.
ASTPtr makeKeyedSide(const String & side, ASTPtr join_group, const char * key_name)
{
    SelectQueryBuilder builder;
    builder.select_list.push_back(id(ColumnNames::Group));
    builder.select_list.push_back(id(ColumnNames::Values));
    join_group->setAlias(key_name);
    builder.select_list.push_back(std::move(join_group));
    builder.from_table = side;
    return builder.getSelectQuery();
}

ASTPtr makeJoinedSelect(const String & left, const String & right)
{
    SelectQueryBuilder builder;

    auto left_values = valueOrEmpty(col(left, ColumnNames::Values));
    builder.select_list.push_back(makeASTFunction(
        "if",
        makeASTFunction("empty", left_values->clone()),
        col(right, ColumnNames::Group),
        col(left, ColumnNames::Group)));
    builder.select_list.back()->setAlias(seriesGroupColumn);

    builder.select_list.push_back(std::move(left_values));
    builder.select_list.back()->setAlias(leftValuesColumn);

    builder.select_list.push_back(valueOrEmpty(col(right, ColumnNames::Values)));
    builder.select_list.back()->setAlias(rightValuesColumn);

    builder.select_list.push_back(col(left, leftJoinGroupColumn));
    builder.select_list.push_back(col(right, rightJoinGroupColumn));

    builder.from_table = left;
    builder.join_kind = JoinKind::Full;
    builder.join_strictness = JoinStrictness::All;
    builder.join_table = right;
    builder.join_on = makeASTFunction(
        "equals",
        col(left, ColumnNames::Group),
        col(right, ColumnNames::Group));

    /// The row count threshold undervalues grid rows, which carry one array per series.
    builder.settings.push_back(SettingChange{"parallel_hash_join_threshold", Field{0u}});

    return builder.getSelectQuery();
}

/// A missing FULL JOIN side stores 0, and 0 is the empty-tag group.
ASTPtr matchKey()
{
    return makeASTFunction(
        "if",
        makeASTFunction("empty", id(leftValuesColumn)),
        id(rightJoinGroupColumn),
        id(leftJoinGroupColumn));
}

ASTPtr makePresenceWindow(bool one_match_group)
{
    /// A right-only row has empty left values and adds nothing to the mask.
    auto bits = makeASTFunction(
        "arrayMap",
        lambda1("x", makeASTFunction("isNotNull", id("x"))),
        id(leftValuesColumn));

    auto function = makeASTFunction("groupBitOrForEach", std::move(bits));
    setWindowPartition(function, one_match_group ? nullptr : matchKey());
    function->setAlias(ColumnNames::JoinPresence);
    return function;
}

ASTPtr makeWindowSelect(const String & joined, bool one_match_group)
{
    SelectQueryBuilder builder;
    builder.select_list.push_back(id(seriesGroupColumn));
    builder.select_list.push_back(id(leftValuesColumn));
    builder.select_list.push_back(id(rightValuesColumn));
    builder.select_list.push_back(id(leftJoinGroupColumn));
    builder.select_list.push_back(id(rightJoinGroupColumn));
    builder.select_list.push_back(makePresenceWindow(one_match_group));
    builder.from_table = joined;
    return builder.getSelectQuery();
}

/// multiIf is short-circuit: the mask runs only on right rows and the merge only where both sides have a row.
ASTPtr outputValues()
{
    auto masked_right = maskValues(id(ColumnNames::JoinPresence), id(rightValuesColumn));
    return makeASTFunction(
        "multiIf",
        makeASTFunction("empty", id(rightValuesColumn)),
        id(leftValuesColumn),
        makeASTFunction("empty", id(leftValuesColumn)),
        masked_right->clone(),
        preferLeft(id(leftValuesColumn), std::move(masked_right)));
}

ASTPtr makeMergedSelect(const String & presence)
{
    SelectQueryBuilder builder;

    builder.select_list.push_back(id(seriesGroupColumn));
    builder.select_list.back()->setAlias(ColumnNames::Group);

    builder.select_list.push_back(outputValues());
    builder.select_list.back()->setAlias(ColumnNames::Values);

    builder.from_table = presence;
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

    /// Each grid is a table expression once. The join matches series ids.
    /// The match key is a column of that side, so empty on() does not put a constant in JOIN ON.
    bool left_metric_name_dropped = left_argument.metric_name_dropped;
    ASTPtr left_join_group = transformGroupASTForBinaryOperator(
        operator_node,
        id(ColumnNames::Group),
        /*drop_metric_name=*/ true,
        left_metric_name_dropped);
    const bool one_match_group = isZeroGroupAST(left_join_group);
    String left_keyed = storeSubquery(makeKeyedSide(left, std::move(left_join_group), leftJoinGroupColumn), context);

    bool right_metric_name_dropped = right_argument.metric_name_dropped;
    String right_keyed = storeSubquery(
        makeKeyedSide(
            right,
            transformGroupASTForBinaryOperator(
                operator_node,
                id(ColumnNames::Group),
                /*drop_metric_name=*/ true,
                right_metric_name_dropped),
            rightJoinGroupColumn),
        context);

    String joined = storeSubquery(makeJoinedSelect(left_keyed, right_keyed), context);
    String presence = storeSubquery(makeWindowSelect(joined, one_match_group), context);

    SQLQueryPiece res{operator_node, ResultType::INSTANT_VECTOR, StoreMethod::VECTOR_GRID};
    res.select_query = makeMergedSelect(presence);
    res.metric_name_dropped = left_argument.metric_name_dropped && right_argument.metric_name_dropped;

    res.start_time = left_argument.start_time;
    res.end_time = left_argument.end_time;
    res.step = left_argument.step;

    return res;
}

}
