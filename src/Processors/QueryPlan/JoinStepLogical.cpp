#include <Processors/QueryPlan/JoinStepLogical.h>

#include <Processors/QueryPlan/JoinStep.h>

#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/Transforms/JoiningTransform.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/TableJoin.h>
#include <IO/Operators.h>
#include <Common/JSONBuilder.h>
#include <Common/typeid_cast.h>
#include <Interpreters/TableJoin.h>
#include <ranges>

namespace DB
{

namespace Settings
{
    extern const SettingsJoinAlgorithm join_algorithm;
    extern const SettingsBool join_any_take_last_row;
}

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
    extern const int INVALID_JOIN_ON_EXPRESSION;
}

std::string_view toString(PredicateOperator op)
{
    switch (op)
    {
        case PredicateOperator::Equal: return "=";
        case PredicateOperator::NullSafeEqual: return "<=>";
        case PredicateOperator::Less: return "<";
        case PredicateOperator::LessOrEquals: return "<=";
        case PredicateOperator::Greater: return ">";
        case PredicateOperator::GreaterOrEquals: return ">=";
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Illegal value for PredicateOperator: {}",
        static_cast<std::underlying_type_t<PredicateOperator>>(op));
}

std::optional<ASOFJoinInequality> operatorToAsofInequality(PredicateOperator op)
{
    switch (op)
    {
        case PredicateOperator::Less: return ASOFJoinInequality::Less;
        case PredicateOperator::LessOrEquals: return ASOFJoinInequality::LessOrEquals;
        case PredicateOperator::Greater: return ASOFJoinInequality::Greater;
        case PredicateOperator::GreaterOrEquals: return ASOFJoinInequality::GreaterOrEquals;
        default: return {};
    }
}

std::string formatJoinCondition(const JoinCondition & join_condition)
{
    auto quote_string = std::views::transform([](const auto & s) { return fmt::format("({})", s.column_name); });
    auto format_predicate = std::views::transform([](const auto & p) { return fmt::format("{} {} {}", p.left_node.column_name, toString(p.op), p.right_node.column_name); });
    Strings desc;
    desc.push_back(fmt::format("Keys: ({})", fmt::join(join_condition.predicates | format_predicate, " AND ")));
    if (!join_condition.left_filter_conditions.empty())
        desc.push_back(fmt::format("Left: ({})", fmt::join(join_condition.left_filter_conditions | quote_string, " AND ")));
    if (!join_condition.right_filter_conditions.empty())
        desc.push_back(fmt::format("Right: ({})", fmt::join(join_condition.right_filter_conditions | quote_string, " AND ")));
    if (!join_condition.residual_conditions.empty())
        desc.push_back(fmt::format("Residual: ({})", fmt::join(join_condition.residual_conditions | quote_string, " AND ")));
    return fmt::format("[{}]", fmt::join(desc, ", "));
}

std::vector<std::pair<String, String>> describeJoinActions(const JoinInfo & join_info)
{
    std::vector<std::pair<String, String>> description;

    description.emplace_back("Type", toString(join_info.kind));
    description.emplace_back("Strictness", toString(join_info.strictness));
    description.emplace_back("Locality", toString(join_info.locality));
    description.emplace_back("Expression", fmt::format("{} {}",
            join_info.expression.is_using ? "USING" : "ON",
            fmt::join(join_info.expression.disjunctive_conditions | std::views::transform(formatJoinCondition), " | ")));

    return description;
}


JoinStepLogical::JoinStepLogical(
    const Block & left_header_,
    const Block & right_header_,
    JoinInfo join_info_,
    JoinExpressionActions join_expression_actions_,
    Names required_output_columns_,
    ContextPtr context_)
    : expression_actions(std::move(join_expression_actions_))
    , join_info(std::move(join_info_))
    , required_output_columns(std::move(required_output_columns_))
    , query_context(std::move(context_))
{
    updateInputHeaders({left_header_, right_header_});
}

QueryPipelineBuilderPtr JoinStepLogical::updatePipeline(QueryPipelineBuilders /* pipelines */, const BuildQueryPipelineSettings & /* settings */)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot execute JoinStepLogical, it should be converted physical step first");
}


void JoinStepLogical::describePipeline(FormatSettings & settings) const
{
    IQueryPlanStep::describePipeline(processors, settings);
}

void JoinStepLogical::describeActions(FormatSettings & settings) const
{
    String prefix(settings.offset, ' ');

    for (const auto & [name, value] : describeJoinActions(join_info))
        settings.out << prefix << name << ": " << value << '\n';
}

void JoinStepLogical::describeActions(JSONBuilder::JSONMap & map) const
{
    for (const auto & [name, value] : describeJoinActions(join_info))
        map.add(name, value);
}

static Block stackHeadersFromStreams(const Headers & input_headers, const Names & required_output_columns)
{
    NameSet required_output_columns_set(required_output_columns.begin(), required_output_columns.end());

    Block result_header;
    for (const auto & header : input_headers)
    {
        for (const auto & column : header)
        {
            if (required_output_columns_set.contains(column.name))
            {
                result_header.insert(column);
            }
            else if (required_output_columns_set.empty())
            {
                /// If no required columns specified, use one first column.
                result_header.insert(column);
                return result_header;
            }
        }
    }
    return result_header;
}

void JoinStepLogical::updateOutputHeader()
{
    output_header = stackHeadersFromStreams(input_headers, required_output_columns);
}


JoinActionRef concatConditions(std::vector<JoinActionRef> & conditions, ActionsDAG & actions_dag)
{
    if (conditions.empty())
        return JoinActionRef(nullptr);

    if (conditions.size() == 1)
        return conditions.front();

    auto and_function = FunctionFactory::instance().get("and", query_context);
    ActionsDAG::NodeRawConstPtrs nodes = conditions | std::views::transform([&](const auto & r) { return r.node; }) | std::ranges::to<std::vector>();

    const auto & result_node = actions_dag.addFunction(and_function, nodes, {});
    actions_dag.addOrReplaceInOutputs(result_node);
    return JoinActionRef(&result_node);
}

bool canPushDownFromOn(JoinKind kind, JoinStrictness strictness, std::optional<JoinTableSide> side = {})
{
    if (strictness != JoinStrictness::All
     && strictness != JoinStrictness::Any
     && strictness != JoinStrictness::RightAny
     && strictness != JoinStrictness::Semi)
        return false;

    return kind == JoinKind::Inner
        || kind == JoinKind::Cross
        || kind == JoinKind::Comma
        || kind == JoinKind::Paste
        || (side == JoinTableSide::Left && kind == JoinKind::Right)
        || (side == JoinTableSide::Right && kind == JoinKind::Left);
}


// template <typename Func, typename... Args>
// void forJoinSides(std::tuple<Args...> && left, std::tuple<Args...> && right, Func && func) {
//     std::apply([&](auto &&... args)
//     {
//         func(JoinTableSide::Left, std::forward<decltype(args)>(args)...);
//     }, std::forward<std::tuple<Args...>>(left));

//     std::apply([&](auto &&... args)
//     {
//         func(JoinTableSide::Right, std::forward<decltype(args)>(args)...);
//     }, std::forward<std::tuple<Args...>>(right));
// }

JoinPtr JoinStepLogical::chooseJoinAlgorithm()
{
    std::vector<JoinActionRef> left_filters;
    std::vector<JoinActionRef> right_filters;
    std::vector<JoinActionRef> post_filters;

    const auto & settings = query_context->getSettingsRef();

    auto table_join = std::make_shared<TableJoin>(settings, query_context->getGlobalTemporaryVolume(), query_context->getTempDataOnDisk());
    table_join->setJoinInfo(join_info);

    std::visit([&table_join](const auto & storage_)
    {
        if (storage_)
            table_join->setStorage(storage_);
    }, prepared_join_storage);

    auto & table_join_clauses = table_join->getClauses();
    for (const auto & join_condition : join_info.expression.disjunctive_conditions)
    {
        auto & table_join_clause = table_join_clauses.emplace_back();
        for (size_t i = 0; i < join_condition.predicates.size(); ++i)
        {
            const auto & predicate = join_condition.predicates[i];
            if (PredicateOperator::Equals == predicate.op)
                table_join_clause.addKey(predicate.left_node.column_name, predicate.right_node.column_name, /* null_safe_comparison = */ false);
            else if (PredicateOperator::NullSafeEqual == predicate.op)
                table_join_clause.addKey(predicate.left_node.column_name, predicate.right_node.column_name, /* null_safe_comparison = */ true);
            else if (join_info.strictness == JoinStrictness::Asof)
                /// pass
            else
                throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION, "Invalid predicate operator in JOIN ON expression");
        }

        if (auto left_pre_filter_condition = concatConditions(join_condition.left_filter_conditions, expression_actions.left_pre_join_actions))
        {
            if (join_info.expression.disjunctive_conditions.size() == 1 && canPushDownFromOn(join_info.kind, join_info.strictness, JoinTableSide::Left))
                left_filters.push_back(left_pre_filter_condition);
            else
                table_join_clause.analyzer_left_filter_condition_column_name = left_pre_filter_condition.column_name;
        }

        if (auto right_pre_filter_condition = concatConditions(join_condition.right_filter_conditions, expression_actions.right_pre_join_actions);)
        {
            if (join_info.expression.disjunctive_conditions.size() == 1 && canPushDownFromOn(join_info.kind, join_info.strictness, JoinTableSide::Right))
                right_filters.push_back(right_pre_filter_condition);
            else
                table_join_clause.analyzer_right_filter_condition_column_name = right_pre_filter_condition.column_name;
        }

        if (auto residual_filter_condition = concatConditions(join_condition.residual_conditions, expression_actions.post_join_actions))
        {
            if (join_info.expression.disjunctive_conditions.size() == 1 && canPushDownFromOn(join_info.kind, join_info.strictness))
            {
                post_filters.push_back(residual_filter_condition);
            }
            else
            {
                LOG_DEBUG(&Poco::Logger::get("XXXX"), "{}:{}: expression_actions.post_join_actions {}", __FILE__, __LINE__, expression_actions.post_join_actions.dumpDAG());
                ExpressionActionsPtr & mixed_join_expression = table_join->getMixedJoinExpression();
                mixed_join_expression = std::make_shared<ExpressionActions>(
                    std::move(expression_actions.post_join_actions),
                    ExpressionActionsSettings::fromContext(query_context));
            }
        }
    }

    /// Find strictly only one inequality in predicate list for ASOF join
    if (join_info.strictness == JoinStrictness::Asof)
    {
        if (join_info.expression.disjunctive_conditions.size() != 1)
            throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION, "ASOF join does not support multiple disjuncts in JOIN ON expression");
        chassert(table_join_clauses.size() == 1);
        const auto & join_condition = join_info.expression.disjunctive_conditions.at(0);
        bool asof_predicate_found = false;
        for (size_t i = 0; i < join_condition.predicates.size(); ++i)
        {
            const auto & predicate = join_condition.predicates[i];
            auto asof_inequality_op = operatorToAsofInequality(predicate.op);
            if (!asof_inequality_op)
                continue;

            if (asof_predicate_found)
                throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION, "ASOF join does not support multiple inequality predicates in JOIN ON expression");
            table_join->setAsofInequality(*asof_inequality_op);
            join_clause.addKey(predicate.left_node.column_name, predicate.right_node.column_name, /* null_safe_comparison = */ false);
        }
        if (!asof_predicate_found)
            throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION, "ASOF join requires one inequality predicate in JOIN ON expression");
    }

    if (join_info.kind == JoinKind::Paste)
        return std::make_shared<PasteJoin>(table_join, input_headers[1]);

    return std::make_shared<HashJoin>(
        table_join, input_headers[1], settings[Setting::join_any_take_last_row]);
}

}
