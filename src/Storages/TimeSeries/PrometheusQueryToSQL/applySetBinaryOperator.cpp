#include <Storages/TimeSeries/PrometheusQueryToSQL/applySetBinaryOperator.h>

#include <unordered_map>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/dropStaleMarkers.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/toVectorGrid.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/transformGroupASTForBinaryOperator.h>
#include <Common/Exception.h>


namespace DB::ErrorCodes
{
extern const int CANNOT_EXECUTE_PROMQL_QUERY;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
enum class SetOperation
{
    And,
    Or,
    Unless,
};

struct ImplInfo
{
    SetOperation operation;
};

const ImplInfo * getImplInfo(std::string_view operator_name)
{
    static const std::unordered_map<std::string_view, ImplInfo> impl_map = {
        {"and", {SetOperation::And}},
        {"or", {SetOperation::Or}},
        {"unless", {SetOperation::Unless}},
    };

    auto it = impl_map.find(operator_name);
    if (it == impl_map.end())
        return nullptr;

    return &it->second;
}

/// Build a compact per-step presence mask for a vector grid: 1 where at least one series of the group has
/// a value at that step, 0 otherwise.
ASTPtr makePresenceMask(ASTPtr values)
{
    auto lambda = makeASTFunction(
        "lambda",
        makeASTFunction("tuple", make_intrusive<ASTIdentifier>("x")),
        makeASTFunction("isNotNull", make_intrusive<ASTIdentifier>("x")));

    return makeASTFunction("groupBitOrForEach", makeASTFunction("arrayMap", std::move(lambda), std::move(values)));
}

ASTPtr makeNonEmptyValuesPredicate(ASTPtr values)
{
    return makeASTFunction(
        "arrayExists",
        makeASTFunction(
            "lambda",
            makeASTFunction("tuple", make_intrusive<ASTIdentifier>("x")),
            makeASTFunction("isNotNull", make_intrusive<ASTIdentifier>("x"))),
        std::move(values));
}

ASTPtr makePresenceMaskedValues(ASTPtr values, ASTPtr presence_mask, bool keep_when_present)
{
    ASTPtr condition = keep_when_present
        ? makeASTFunction("greater", make_intrusive<ASTIdentifier>("presence"), make_intrusive<ASTLiteral>(0u))
        : makeASTFunction("equals", make_intrusive<ASTIdentifier>("presence"), make_intrusive<ASTLiteral>(0u));

    return makeASTFunction(
        "arrayMap",
        makeASTFunction(
            "lambda",
            makeASTFunction("tuple", make_intrusive<ASTIdentifier>("value"), make_intrusive<ASTIdentifier>("presence")),
            makeASTFunction("if", std::move(condition), make_intrusive<ASTIdentifier>("value"), make_intrusive<ASTLiteral>(Field{}))),
        std::move(values),
        std::move(presence_mask));
}

/// Registers a named subquery. Subqueries read by more than one downstream step must be added with
/// SQLSubqueryType::MATERIALIZED_TABLE so they are evaluated once: ClickHouse inlines WITH subqueries
/// per use, and `checkSharedSubqueriesAreMaterialized` asserts the mark in debug builds.
/// (If the setting `enable_materialized_cte` is disabled, a shared subquery is evaluated a second
/// time, which is still correct because group ids are the same within one query.)
String materializeTable(ASTPtr && select_query, ConverterContext & context, SQLSubqueryType subquery_type = SQLSubqueryType::TABLE)
{
    context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(select_query), subquery_type});
    return context.subqueries.back().name;
}

void checkArgumentTypes(
    const PrometheusQueryTree::BinaryOperator * operator_node,
    const SQLQueryPiece & left_argument,
    const SQLQueryPiece & right_argument,
    const ConverterContext & context)
{
    std::string_view operator_name = operator_node->operator_name;

    if (left_argument.type != ResultType::INSTANT_VECTOR || right_argument.type != ResultType::INSTANT_VECTOR)
    {
        throw Exception(
            ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
            "Binary operator '{}' expects two arguments of type {}, got {} and {}",
            operator_name,
            ResultType::INSTANT_VECTOR,
            left_argument.type,
            right_argument.type);
    }

    if (operator_node->group_left)
    {
        throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY, "Binary operator '{}' doesn't allow group_left", operator_name);
    }

    if (operator_node->group_right)
    {
        throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY, "Binary operator '{}' doesn't allow group_right", operator_name);
    }

    if (operator_node->bool_modifier)
    {
        throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY, "Binary operator '{}' doesn't allow bool modifier", operator_name);
    }

    (void)context;
}

String prepareSide(
    const PrometheusQueryTree::BinaryOperator * operator_node,
    SQLQueryPiece && argument,
    ConverterContext & context,
    bool & metric_name_dropped,
    bool read_twice)
{
    argument = toVectorGrid(std::move(argument), context);
    metric_name_dropped = argument.metric_name_dropped;

    String input = materializeTable(std::move(argument.select_query), context);

    SelectQueryBuilder builder;
    builder.from_table = input;

    builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));
    builder.select_list.back()->setAlias(ColumnNames::OriginalGroup);

    /// Set operators match by all labels except `__name__` unless `on` or `ignoring` changes the match key.
    bool metric_name_dropped_from_join_group = metric_name_dropped;
    ASTPtr join_group = transformGroupASTForBinaryOperator(
        operator_node,
        make_intrusive<ASTIdentifier>(ColumnNames::Group),
        /* drop_metric_name = */ true,
        metric_name_dropped_from_join_group);
    builder.select_list.push_back(std::move(join_group));
    builder.select_list.back()->setAlias(ColumnNames::JoinGroup);

    /// For set matching a stale marker means the series is absent at that step, so it must not be seen
    /// as present by the presence mask, nor by the non-empty row predicates. Normalize it to NULL up front.
    builder.select_list.push_back(dropStaleMarkers(make_intrusive<ASTIdentifier>(ColumnNames::Values)));
    builder.select_list.back()->setAlias(ColumnNames::Values);

    /// A side read twice (by the presence-counting step and by the final join or union) is added
    /// as a materialized CTE to be evaluated once.
    return materializeTable(
        builder.getSelectQuery(), context, read_twice ? SQLSubqueryType::MATERIALIZED_TABLE : SQLSubqueryType::TABLE);
}

ASTPtr selectPresenceByJoinGroup(const String & table_name)
{
    SelectQueryBuilder builder;
    builder.from_table = table_name;
    builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::JoinGroup));
    builder.select_list.back()->setAlias(ColumnNames::JoinGroup);
    builder.select_list.push_back(makePresenceMask(make_intrusive<ASTIdentifier>(ColumnNames::Values)));
    builder.select_list.back()->setAlias(ColumnNames::Values);
    builder.group_by.push_back(make_intrusive<ASTIdentifier>(ColumnNames::JoinGroup));
    return builder.getSelectQuery();
}

ASTPtr selectOriginalSeries(const String & table_name)
{
    SelectQueryBuilder builder;
    builder.from_table = table_name;
    builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::OriginalGroup));
    builder.select_list.back()->setAlias(ColumnNames::Group);
    builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Values));
    return builder.getSelectQuery();
}

ASTPtr selectLeftByMissingGroup(const String & left, const String & presence)
{
    SelectQueryBuilder builder;
    builder.from_table = left;
    builder.select_list.push_back(make_intrusive<ASTIdentifier>(Strings{left, ColumnNames::OriginalGroup}));
    builder.select_list.back()->setAlias(ColumnNames::Group);
    builder.select_list.push_back(make_intrusive<ASTIdentifier>(Strings{left, ColumnNames::Values}));
    builder.select_list.back()->setAlias(ColumnNames::Values);

    builder.join_kind = JoinKind::Left;
    builder.join_strictness = JoinStrictness::Anti;
    builder.join_table = presence;
    builder.join_on = makeASTFunction(
        "equals",
        make_intrusive<ASTIdentifier>(Strings{left, ColumnNames::JoinGroup}),
        make_intrusive<ASTIdentifier>(Strings{presence, ColumnNames::JoinGroup}));

    return builder.getSelectQuery();
}

ASTPtr selectLeftMaskedByPresence(const String & left, const String & presence, bool keep_when_present, ConverterContext & context)
{
    SelectQueryBuilder builder;
    builder.from_table = left;
    builder.select_list.push_back(make_intrusive<ASTIdentifier>(Strings{left, ColumnNames::OriginalGroup}));
    builder.select_list.back()->setAlias(ColumnNames::Group);
    builder.select_list.push_back(makePresenceMaskedValues(
        make_intrusive<ASTIdentifier>(Strings{left, ColumnNames::Values}),
        make_intrusive<ASTIdentifier>(Strings{presence, ColumnNames::Values}),
        keep_when_present));
    builder.select_list.back()->setAlias(ColumnNames::Values);

    builder.join_kind = JoinKind::Inner;
    /// The presence side has one row per join group, but several original series can share that group (for example `on()`).
    /// Use ALL to keep every matching LHS series even if the optimizer reorders the join.
    builder.join_strictness = JoinStrictness::All;
    builder.join_table = presence;
    builder.join_on = makeASTFunction(
        "equals",
        make_intrusive<ASTIdentifier>(Strings{left, ColumnNames::JoinGroup}),
        make_intrusive<ASTIdentifier>(Strings{presence, ColumnNames::JoinGroup}));

    String masked = materializeTable(builder.getSelectQuery(), context);

    SelectQueryBuilder filter_builder;
    filter_builder.from_table = masked;
    filter_builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));
    filter_builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Values));
    filter_builder.where = makeNonEmptyValuesPredicate(make_intrusive<ASTIdentifier>(ColumnNames::Values));
    return filter_builder.getSelectQuery();
}

ASTPtr selectUnion(ASTPtr && first, ASTPtr && second, ConverterContext & context, bool combine_matching_groups)
{
    String first_table = materializeTable(std::move(first), context);
    String second_table = materializeTable(std::move(second), context);

    SelectQueryBuilder builder;
    builder.from_table = first_table;
    builder.union_table = second_table;
    builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));
    builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Values));

    if (!combine_matching_groups)
        return builder.getSelectQuery();

    String union_table = materializeTable(builder.getSelectQuery(), context);

    SelectQueryBuilder combine_builder;
    combine_builder.from_table = union_table;
    combine_builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));
    combine_builder.select_list.back()->setAlias(ColumnNames::Group);
    combine_builder.select_list.push_back(makeASTFunction("anyForEach", make_intrusive<ASTIdentifier>(ColumnNames::Values)));
    combine_builder.select_list.back()->setAlias(ColumnNames::Values);
    combine_builder.group_by.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));
    return combine_builder.getSelectQuery();
}

ASTPtr selectAnd(const String & left, const String & right, ConverterContext & context)
{
    String right_presence = materializeTable(selectPresenceByJoinGroup(right), context);
    return selectLeftMaskedByPresence(left, right_presence, /* keep_when_present = */ true, context);
}

ASTPtr selectUnless(const String & left, const String & right, ConverterContext & context)
{
    /// The presence table is read twice - by the missing-group anti join and by the masking join.
    String right_presence = materializeTable(selectPresenceByJoinGroup(right), context, SQLSubqueryType::MATERIALIZED_TABLE);
    ASTPtr unmatched_groups = selectLeftByMissingGroup(left, right_presence);
    ASTPtr unmatched_steps = selectLeftMaskedByPresence(left, right_presence, /* keep_when_present = */ false, context);
    return selectUnion(std::move(unmatched_groups), std::move(unmatched_steps), context, /* combine_matching_groups = */ false);
}

ASTPtr selectOr(const String & left, const String & right, ConverterContext & context)
{
    /// The presence table is read twice - by the missing-group anti join and by the masking join.
    String left_presence = materializeTable(selectPresenceByJoinGroup(left), context, SQLSubqueryType::MATERIALIZED_TABLE);
    ASTPtr left_all = selectOriginalSeries(left);
    ASTPtr right_unmatched_groups = selectLeftByMissingGroup(right, left_presence);
    ASTPtr right_unmatched_steps = selectLeftMaskedByPresence(right, left_presence, /* keep_when_present = */ false, context);
    ASTPtr right_unmatched
        = selectUnion(std::move(right_unmatched_groups), std::move(right_unmatched_steps), context, /* combine_matching_groups = */ false);
    return selectUnion(std::move(left_all), std::move(right_unmatched), context, /* combine_matching_groups = */ true);
}
}


bool isSetBinaryOperator(std::string_view operator_name)
{
    return getImplInfo(operator_name) != nullptr;
}


SQLQueryPiece applySetBinaryOperator(
    const PrometheusQueryTree::BinaryOperator * operator_node, SQLQueryPiece && left_argument, SQLQueryPiece && right_argument, ConverterContext & context)
{
    const auto & operator_name = operator_node->operator_name;
    const ImplInfo * impl_info = getImplInfo(operator_name);
    chassert(impl_info);

    checkArgumentTypes(operator_node, left_argument, right_argument, context);

    if (left_argument.store_method == StoreMethod::EMPTY)
    {
        if (impl_info->operation == SetOperation::Or)
        {
            right_argument.node = operator_node;
            return std::move(right_argument);
        }
        return SQLQueryPiece{operator_node, operator_node->result_type, StoreMethod::EMPTY};
    }

    if (right_argument.store_method == StoreMethod::EMPTY)
    {
        if (impl_info->operation == SetOperation::And)
            return SQLQueryPiece{operator_node, operator_node->result_type, StoreMethod::EMPTY};
        left_argument.node = operator_node;
        return std::move(left_argument);
    }

    const TimestampType start_time = left_argument.start_time;
    const TimestampType end_time = left_argument.end_time;
    const DurationType step = left_argument.step;

    bool left_metric_name_dropped = false;
    bool right_metric_name_dropped = false;
    /// `or` reads both sides twice (presence counting + the final selects), `unless` reads its left side twice
    /// (the missing-group and masking branches); `and` reads each side once.
    bool left_read_twice = impl_info->operation != SetOperation::And;
    bool right_read_twice = impl_info->operation == SetOperation::Or;
    String left = prepareSide(operator_node, std::move(left_argument), context, left_metric_name_dropped, left_read_twice);
    String right = prepareSide(operator_node, std::move(right_argument), context, right_metric_name_dropped, right_read_twice);

    ASTPtr result_ast;
    switch (impl_info->operation)
    {
        case SetOperation::And:
            result_ast = selectAnd(left, right, context);
            break;
        case SetOperation::Or:
            result_ast = selectOr(left, right, context);
            break;
        case SetOperation::Unless:
            result_ast = selectUnless(left, right, context);
            break;
    }

    SQLQueryPiece res{operator_node, operator_node->result_type, StoreMethod::VECTOR_GRID};
    res.select_query = std::move(result_ast);
    res.start_time = start_time;
    res.end_time = end_time;
    res.step = step;
    res.metric_name_dropped
        = (impl_info->operation == SetOperation::Or) ? (left_metric_name_dropped && right_metric_name_dropped) : left_metric_name_dropped;

    return res;
}

}
