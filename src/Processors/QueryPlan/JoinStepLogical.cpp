#include <Processors/QueryPlan/JoinStepLogical.h>

#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>

#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/Transforms/JoiningTransform.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/Context.h>
#include <IO/Operators.h>
#include <Common/JSONBuilder.h>
#include <Common/typeid_cast.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/ExpressionActions.h>
#include <Storages/StorageJoin.h>
#include <ranges>
#include <Core/Settings.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/PasteJoin.h>
#include <Interpreters/JoinUtils.h>
#include <Planner/PlannerJoins.h>
#include <DataTypes/DataTypesNumber.h>

#include <Functions/FunctionsLogical.h>
#include <Functions/FunctionsComparison.h>
#include <Functions/isNotDistinctFrom.h>
#include <Functions/IFunctionAdaptors.h>
#include <Functions/IsOperation.h>
#include <Functions/tuple.h>


namespace DB
{

namespace Setting
{
    extern const SettingsJoinAlgorithm join_algorithm;
    extern const SettingsBool join_any_take_last_row;
    extern const SettingsUInt64 default_max_bytes_in_join;
    extern const SettingsBool join_use_nulls;
}

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
    extern const int INVALID_JOIN_ON_EXPRESSION;
    extern const int NOT_FOUND_COLUMN_IN_BLOCK;
    extern const int INCORRECT_DATA;
}

std::string operatorToFunctionName(PredicateOperator op)
{
    switch (op)
    {
        case PredicateOperator::NullSafeEquals: return FunctionIsNotDistinctFrom::name;
        case PredicateOperator::Equals: return NameEquals::name;
        case PredicateOperator::Less: return NameLess::name;
        case PredicateOperator::LessOrEquals: return NameLessOrEquals::name;
        case PredicateOperator::Greater: return NameGreater::name;
        case PredicateOperator::GreaterOrEquals: return NameGreaterOrEquals::name;
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Illegal value for PredicateOperator: {}", static_cast<Int32>(op));
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

void formatJoinCondition(const JoinCondition & join_condition, WriteBuffer & buf)
{
    auto quote_string = std::views::transform([](const auto & s) { return fmt::format("({})", s.getColumnName()); });
    auto format_predicate = std::views::transform([](const auto & p) { return fmt::format("{} {} {}", p.left_node.getColumnName(), toString(p.op), p.right_node.getColumnName()); });
    buf << "[";
    buf << fmt::format("Predcates: ({})", fmt::join(join_condition.predicates | format_predicate, ", "));
    if (!join_condition.restrict_conditions.empty())
        buf << " " << fmt::format("Filter: ({})", fmt::join(join_condition.restrict_conditions | quote_string, ", "));
    buf << "]";
}

String formatJoinCondition(const JoinCondition & join_condition)
{
    WriteBufferFromOwnString buf;
    formatJoinCondition(join_condition, buf);
    return buf.str();
}

namespace
{

auto bitSetToList(BaseRelsSet s)
{
    return std::views::iota(0u, s.size()) | std::views::filter([&](size_t i) { return s.test(i); });
}

bool isSubsetOf(BaseRelsSet lhs, BaseRelsSet rhs)
{
    return (lhs & rhs) == lhs;
}

}

JoinStepLogical::JoinStepLogical(
    const Block & left_header,
    bool use_nulls_,
    JoinSettings join_settings_,
    SortingStep::Settings sorting_settings_)
    : use_nulls(use_nulls_)
    , join_settings(std::move(join_settings_))
    , sorting_settings(std::move(sorting_settings_))
{
    updateInputHeaders({std::move(left_header)});
}

QueryPipelineBuilderPtr JoinStepLogical::updatePipeline(QueryPipelineBuilders /* pipelines */, const BuildQueryPipelineSettings & /* settings */)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot execute JoinStepLogical, it should be converted physical step first");
}

void JoinStepLogical::describePipeline(FormatSettings & settings) const
{
    IQueryPlanStep::describePipeline(processors, settings);
}

template <typename ResultType>
class ActionsDescrtiptionBuilder;

template <>
class ActionsDescrtiptionBuilder<IQueryPlanStep::FormatSettings>
{
public:
    explicit ActionsDescrtiptionBuilder(IQueryPlanStep::FormatSettings settings_)
        : settings(std::move(settings_))
        , prefix(settings.offset, settings.indent_char)
    {}

    void add(String key, String value) { settings.out << prefix << key << ": " << value << "\n"; }
    void add(String key, ExpressionActions value)
    {
        settings.out << prefix << key << ":\n";
        value.describeActions(settings.out, prefix);
    }

    class NestedLevel
    {
    public:
        explicit NestedLevel(IQueryPlanStep::FormatSettings settings_) : settings(std::move(settings_)) {}

        ActionsDescrtiptionBuilder<IQueryPlanStep::FormatSettings> next()
        {
            auto new_settings = settings;
            new_settings.offset += settings.indent;
            return ActionsDescrtiptionBuilder<IQueryPlanStep::FormatSettings>(new_settings);
        }
        IQueryPlanStep::FormatSettings settings;
    };

    NestedLevel push(String key)
    {
        settings.out << prefix << key << ":\n";
        return NestedLevel(settings);
    }


    IQueryPlanStep::FormatSettings settings;
    String prefix;
};

template <>
class ActionsDescrtiptionBuilder<JSONBuilder::JSONMap>
{
public:
    explicit ActionsDescrtiptionBuilder(JSONBuilder::JSONMap & map_)
        : map(map_)
    {}

    void add(String key, String value) { map.add(key, value); }
    void add(String key, ExpressionActions value) { map.add(key, value.toTree()); }

    class NestedLevel
    {
    public:
        explicit NestedLevel(JSONBuilder::JSONArray & arr_) : arr(arr_) {}

        ActionsDescrtiptionBuilder<JSONBuilder::JSONMap> next()
        {
            auto current = std::make_unique<JSONBuilder::JSONMap>();
            ActionsDescrtiptionBuilder<JSONBuilder::JSONMap> result(*current);
            arr.add(std::move(current));
            return result;
        }

        JSONBuilder::JSONArray & arr;
    };

    NestedLevel push(String key)
    {
        auto level = std::make_unique<JSONBuilder::JSONArray>();
        NestedLevel nested(*level);
        map.add(key, std::move(level));
        return nested;
    }

    JSONBuilder::JSONMap & map;
};

template <typename ResultType>
void JoinStepLogical::describeJoinActionsImpl(ResultType & result) const
{
    ActionsDescrtiptionBuilder<ResultType> root_description(result);


    auto join_description = root_description.push("JoinOperators");
    // Display information for all join operators
    for (size_t join_num = 0; join_num < join_operators.size(); ++join_num)
    {
        auto description = join_description.next();

        const auto & join_info = join_operators[join_num];

        description.add("Type", toString(join_info.kind));
        description.add("Strictness", toString(join_info.strictness));
        description.add("Locality", toString(join_info.locality));

        {
            WriteBufferFromOwnString join_expression_str;
            join_expression_str << (join_info.expression.is_using ? "USING" : "ON") << " ";
            formatJoinCondition(join_info.expression.condition, join_expression_str);
            for (const auto & condition : join_info.expression.disjunctive_conditions)
            {
                join_expression_str << " | ";
                formatJoinCondition(condition, join_expression_str);
            }
            description.add("Expression", join_expression_str.str());
        }

        auto sorted_actions = join_info.expression_actions.actions
            | std::views::transform([](const auto & p) { return std::make_pair(p.first, p.second.get()); }) | std::ranges::to<std::vector>();
        std::ranges::sort(sorted_actions, [](const auto & lhs, const auto & rhs) { return lhs.first.to_ullong() < rhs.first.to_ullong(); });
        for (const auto & [inputs, actions] : sorted_actions)
        {
            if (!actions)
                continue;
            description.add(fmt::format("Actions[{}]", fmt::join(bitSetToList(inputs), ", ")), ExpressionActions(actions->clone()));
        }
    }

    root_description.add("Required Output", fmt::format("[{}]", fmt::join(required_output_columns, ", ")));
}

void JoinStepLogical::describeActions(FormatSettings & settings) const
{
    describeJoinActionsImpl(settings);
}

void JoinStepLogical::describeActions(JSONBuilder::JSONMap & map) const
{
    describeJoinActionsImpl(map);
}

static ActionsDAG::NodeRawConstPtrs getAnyColumn(const ActionsDAG::NodeRawConstPtrs & nodes)
{
    if (nodes.empty())
        throw Exception(ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK, "No columns in JOIN result");

    ActionsDAG::NodeRawConstPtrs result_nodes;
    for (const auto * output : nodes)
    {
        /// Add column as many times as it is used in output
        /// Otherwise, invariants in other parts of the code may be violated
        if (output->result_name == nodes.at(0)->result_name)
            result_nodes.push_back(output);
    }
    return result_nodes;
}

size_t JoinStepLogical::getNumberOfTables() const
{
    chassert(input_headers.size() == join_operators.size() + 1);
    return input_headers.size();
}

JoinOperator & JoinStepLogical::addInput(JoinOperator join_operator, const Header & header)
{
    input_headers.push_back(header);
    updateOutputHeader();
    return join_operators.emplace_back(std::move(join_operator));
}

BaseRelsSet JoinStepLogical::getNullExtendedTables() const
{
    BaseRelsSet mask;
    for (size_t i = 0; i < join_operators.size(); ++i)
    {
        auto kind = join_operators[i].kind;
        if (kind == JoinKind::Left || kind == JoinKind::Full)
            mask.set(i + 1);
        if (kind == JoinKind::Right || kind == JoinKind::Full)
            mask |= BaseRelsSet((1u << (i + 1)) - 1);
    }
    return mask;
}

Headers JoinStepLogical::getCurrentHeaders() const
{
    Headers result;
    auto null_mask = getNullExtendedTables();
    for (size_t input_no = 0; input_no < input_headers.size(); ++input_no)
    {
        const auto & input_header = input_headers[input_no];
        auto & header = result.emplace_back(input_header);
        if (null_mask.test(input_no))
            JoinCommon::convertColumnsToNullable(header);
    }
    return result;
}

void JoinStepLogical::addRequiredOutput(const NameSet & names)
{
    required_output_columns.insert(names.begin(), names.end());
}

void JoinStepLogical::updateOutputHeader()
{
    Header & header = output_header.emplace();

    auto null_mask = getNullExtendedTables();
    for (size_t i = 0; i < input_headers.size(); ++i)
    {
        for (auto column : input_headers[i])
        {
            if (null_mask.test(i))
                JoinCommon::convertColumnToNullable(column);

            if (required_output_columns.empty())
            {
                header.insert(std::move(column));
                break;
            }

            if (required_output_columns.contains(column.name))
                header.insert(std::move(column));
        }
    }
}

JoinActionRef addNewOutput(const ActionsDAG::Node & node, ActionsDAG * actions_dag)
{
    actions_dag->addOrReplaceInOutputs(node);
    return JoinActionRef(&node, actions_dag);
}

/// We may have expressions like `a and b` that work even if `a` and `b` are non-boolean and expression return boolean or nullable.
/// In some contexts, we may split `a` and `b`, but we still want to have the same logic applied, as if it were still an `and` operand.
JoinActionRef toBoolIfNeeded(JoinActionRef condition, const FunctionOverloadResolverPtr & concat_function)
{
    auto output_type = removeNullable(condition.getType());
    WhichDataType which_type(output_type);
    if (!which_type.isUInt8())
    {
        auto * actions_dag = condition.getActions();
        DataTypePtr uint8_ty = std::make_shared<DataTypeUInt8>();
        ColumnWithTypeAndName rhs;
        const ActionsDAG::Node * rhs_node = nullptr;
        if (concat_function->getName() == "and")
            rhs_node = &actions_dag->addColumn(ColumnWithTypeAndName(uint8_ty->createColumnConst(1, 1), uint8_ty, "true"));
        else if (concat_function->getName() == "or")
            rhs_node = &actions_dag->addColumn(ColumnWithTypeAndName(uint8_ty->createColumnConst(0, 0), uint8_ty, "false"));

        if (rhs_node)
        {
            rhs_node = &actions_dag->addFunction(concat_function, {condition.getNode(), rhs_node}, {});
            actions_dag->addOrReplaceInOutputs(*rhs_node);
            return JoinActionRef(rhs_node, actions_dag);
        }
    }
    return condition;
}


ActionsDAG * getCommonActionsDag(const std::span<JoinActionRef> & nodes)
{
    if (nodes.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Empty JoinActionRef list");

#ifdef DEBUG_OR_SANITIZER_BUILD
    for (const auto & node : nodes)
    {
        if (node.getActions() != nodes.front().getActions())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Different actions DAGs in JoinActionRef list: [{}]",
                fmt::join(nodes | std::views::transform([](const auto & x) { return x.getColumnName(); }), ", "));
    }
#endif

    return nodes.front().getActions();
}

JoinActionRef concatConditionsWithFunction(std::span<JoinActionRef> conditions, const FunctionOverloadResolverPtr & concat_function)
{
    if (conditions.empty())
        return JoinActionRef(nullptr);

    if (conditions.size() == 1)
        return toBoolIfNeeded(conditions.front(), concat_function);

    auto * actions_dag = getCommonActionsDag(conditions);

    auto nodes = std::ranges::to<ActionsDAG::NodeRawConstPtrs>(std::views::transform(conditions, [](const auto & x) { return x.getNode(); }));

    const auto & result_node = actions_dag->addFunction(concat_function, nodes, {});
    actions_dag->addOrReplaceInOutputs(result_node);
    return JoinActionRef(&result_node, actions_dag);
}

JoinActionRef concatConditions(std::span<JoinActionRef> conditions)
{
    FunctionOverloadResolverPtr and_function = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionAnd>());
    return concatConditionsWithFunction(conditions, and_function);
}

JoinActionRef concatMergeConditions(std::vector<JoinActionRef> & conditions, ActionsDAG * action_dag)
{
    auto matching = std::ranges::partition(conditions, [=](const auto & node) { return node.getActions() != action_dag; });

    auto condition = concatConditions(std::span<JoinActionRef>(matching.begin(), matching.end()));

    conditions.erase(matching.begin(), conditions.end());
    if (condition)
        conditions.push_back(condition);
    return condition;
}

const ActionsDAG::Node & findOrAddInput(ActionsDAG * actions_dag, const ColumnWithTypeAndName & column)
{
    for (const auto * node : actions_dag->getInputs())
    {
        if (node->result_name == column.name)
            return *node;
    }

    return actions_dag->addInput(column);
}

JoinActionRef predicateToCondition(const JoinPredicate & predicate, ActionsDAG * actions_dag)
{
    const auto & left_node = findOrAddInput(actions_dag, predicate.left_node.getColumn());
    const auto & right_node = findOrAddInput(actions_dag, predicate.right_node.getColumn());

    auto operator_function = FunctionFactory::instance().get(operatorToFunctionName(predicate.op), nullptr);
    const auto & result_node = actions_dag->addFunction(operator_function, {&left_node, &right_node}, {});
    actions_dag->addOrReplaceInOutputs(result_node);
    return JoinActionRef(&result_node, actions_dag);
}

bool canPushDownFromOn(const JoinOperator & join_info, std::optional<JoinTableSide> side = {})
{
    bool is_suitable_kind = join_info.kind == JoinKind::Inner
        || join_info.kind == JoinKind::Cross
        || join_info.kind == JoinKind::Comma
        || join_info.kind == JoinKind::Paste
        || (side == JoinTableSide::Left && join_info.kind == JoinKind::Right)
        || (side == JoinTableSide::Right && join_info.kind == JoinKind::Left);

    return is_suitable_kind
        && join_info.expression.disjunctive_conditions.empty()
        && join_info.strictness == JoinStrictness::All;
}

void addRequiredInputToOutput(ActionsDAG * dag, const NameSet & required_output_columns)
{
    NameSet existing_output_columns;
    auto & outputs = dag->getOutputs();
    for (const auto & node : outputs)
        existing_output_columns.insert(node->result_name);

    for (const auto * node : dag->getInputs())
    {
        if (!required_output_columns.contains(node->result_name)
         || existing_output_columns.contains(node->result_name))
            continue;
        outputs.push_back(node);
    }
}

struct JoinPlanningContext
{
    std::shared_ptr<TableJoin> table_join;

    JoinSettings & join_settings;
    SortingStep::Settings & sorting_settings;
    const ExpressionActionsSettings & expression_actions_settings;

    const NameSet & required_output_columns;

    PreparedJoinStorage & prepared_join_storage;

    UInt64 max_threads;
    UInt64 max_entries_for_hash_table_stats;
    String initial_query_id;
    std::chrono::milliseconds lock_acquire_timeout;

    UInt64 hash_table_key_hash = {};
    bool is_asof = false;
    bool is_using = false;

    bool use_nulls = false;
};

void predicateOperandsToCommonType(JoinPredicate & predicate, const JoinPlanningContext & join_context)
{
    auto & left_node = predicate.left_node;
    auto & right_node = predicate.right_node;
    const auto & left_type = left_node.getType();
    const auto & right_type = right_node.getType();

    if (left_type->equals(*right_type))
        return;

    DataTypePtr common_type;
    try
    {
        common_type = getLeastSupertype(DataTypes{left_type, right_type});
    }
    catch (Exception & ex)
    {
        ex.addMessage("JOIN cannot infer common type in ON section for keys. Left key '{}' type {}. Right key '{}' type {}",
            left_node.getColumnName(), left_type->getName(),
            right_node.getColumnName(), right_type->getName());
        throw;
    }

    if (!left_type->equals(*common_type))
    {
        const auto & result_name = join_context.is_using ? left_node.getColumnName() : "";
        auto * actions = left_node.getActions();
        left_node = addNewOutput(actions->addCast(*left_node.getNode(), common_type, result_name), actions);
    }

    if (!join_context.prepared_join_storage && !right_type->equals(*common_type))
    {
        const std::string & result_name = join_context.is_using ? right_node.getColumnName() : "";
        auto * actions = right_node.getActions();
        right_node = addNewOutput(actions->addCast(*right_node.getNode(), common_type, result_name), actions);
    }
}

bool addJoinConditionToTableJoin(JoinCondition & join_condition, TableJoin::JoinOnClause & table_join_clause, ActionsDAG * post_join_actions, const JoinPlanningContext & join_context)
{
    std::vector<JoinPredicate> new_predicates;
    for (size_t i = 0; i < join_condition.predicates.size(); ++i)
    {
        auto & predicate = join_condition.predicates[i];
        predicateOperandsToCommonType(predicate, join_context);
        if (PredicateOperator::Equals == predicate.op || PredicateOperator::NullSafeEquals == predicate.op)
        {
            auto * left_key_node = predicate.left_node.getNode();
            auto * right_key_node = predicate.right_node.getNode();
            bool null_safe_comparison = PredicateOperator::NullSafeEquals == predicate.op;
            if (null_safe_comparison && isNullableOrLowCardinalityNullable(left_key_node->result_type) && isNullableOrLowCardinalityNullable(right_key_node->result_type))
            {
                /**
                  * In case of null-safe comparison (a IS NOT DISTINCT FROM b),
                  * we need to wrap keys with a non-nullable type.
                  * The type `tuple` can be used for this purpose,
                  * because value tuple(NULL) is not NULL itself (moreover it has type Tuple(Nullable(T) which is not Nullable).
                  * Thus, join algorithm will match keys with values tuple(NULL).
                  * Example:
                  *   SELECT * FROM t1 JOIN t2 ON t1.a <=> t2.b
                  * This will be semantically transformed to:
                  *   SELECT * FROM t1 JOIN t2 ON tuple(t1.a) == tuple(t2.b)
                  */

                FunctionOverloadResolverPtr wrap_nullsafe_function = std::make_shared<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionTuple>());

                auto * left_actions = predicate.left_node.getActions();
                const auto & new_left_node = left_actions->addFunction(wrap_nullsafe_function, {left_key_node}, {});
                left_actions->addOrReplaceInOutputs(new_left_node);
                predicate.left_node = JoinActionRef(&new_left_node, left_actions);

                auto * right_actions = predicate.right_node.getActions();
                const auto & new_right_node = right_actions->addFunction(wrap_nullsafe_function, {right_key_node}, {});
                right_actions->addOrReplaceInOutputs(new_right_node);
                predicate.right_node = JoinActionRef(&new_right_node, right_actions);
            }

            table_join_clause.addKey(predicate.left_node.getColumnName(), predicate.right_node.getColumnName(), null_safe_comparison);
            new_predicates.push_back(predicate);
        }
        else if (join_context.is_asof)
        {
            /// ASOF preficate is handled later, keep it in predicate list
            new_predicates.push_back(predicate);
        }
        else
        {
            /// Move non-equality predicates to residual conditions
            auto predicate_action = predicateToCondition(predicate, post_join_actions);
            join_condition.restrict_conditions.push_back(predicate_action);
        }
    }

    join_condition.predicates = std::move(new_predicates);

    return !join_condition.predicates.empty();
}

struct ActionsTriple
{
    ActionsDAG * left_pre_join_actions = nullptr;
    ActionsDAG * right_pre_join_actions = nullptr;
    ActionsDAG * post_join_actions = nullptr;
};

JoinActionRef buildSingleActionForJoinCondition(JoinCondition & join_condition, ActionsTriple dags)
{
    std::vector<JoinActionRef> all_conditions;

    auto * result_dag = dags.post_join_actions;
    if (auto condition = concatMergeConditions(join_condition.restrict_conditions, dags.left_pre_join_actions))
    {
        const auto & node = findOrAddInput(result_dag, condition.getColumn());
        result_dag->addOrReplaceInOutputs(node);
        all_conditions.emplace_back(&node, result_dag);
    }

    if (auto condition = concatMergeConditions(join_condition.restrict_conditions, dags.right_pre_join_actions))
    {
        const auto & node = findOrAddInput(result_dag, condition.getColumn());
        result_dag->addOrReplaceInOutputs(node);
        all_conditions.emplace_back(&node, result_dag);
    }


    auto residual_conditions_action = concatMergeConditions(join_condition.restrict_conditions, result_dag);
    if (residual_conditions_action)
        all_conditions.push_back(residual_conditions_action);

    for (const auto & predicate : join_condition.predicates)
    {
        auto predicate_action = predicateToCondition(predicate, result_dag);
        all_conditions.push_back(predicate_action);
    }

    return concatMergeConditions(all_conditions, result_dag);
}

JoinActionRef buildSingleActionForJoinExpression(JoinExpression & join_expression, ActionsTriple dags)
{
    std::vector<JoinActionRef> all_conditions;

    if (auto condition = buildSingleActionForJoinCondition(join_expression.condition, dags))
        all_conditions.push_back(condition);

    for (auto & join_condition : join_expression.disjunctive_conditions)
        if (auto condition = buildSingleActionForJoinCondition(join_condition, dags))
            all_conditions.push_back(condition);

    FunctionOverloadResolverPtr or_function = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionOr>());
    return concatConditionsWithFunction(all_conditions, or_function);
}

void JoinStepLogical:: setHashTableCacheKey(UInt64 hash_table_key_hash_, size_t idx)
{
    if (idx >= hash_table_key_hashes.size())
        hash_table_key_hashes.resize(idx + 1, 0);
    hash_table_key_hashes[idx] = hash_table_key_hash_;
}

void JoinStepLogical::setPreparedJoinStorage(PreparedJoinStorage storage) { prepared_join_storage = std::move(storage); }

static Block blockWithColumns(ColumnsWithTypeAndName columns)
{
    Block block;
    for (const auto & column : columns)
        block.insert(ColumnWithTypeAndName(column.column ? column.column : column.type->createColumn(), column.type, column.name));
    return block;
}

static void addToNullableActions(ActionsDAG * dag, const FunctionOverloadResolverPtr & to_nullable_function)
{
    for (auto & output_node : dag->getOutputs())
    {
        DataTypePtr type_to_check = output_node->result_type;
        if (const auto * type_to_check_low_cardinality = typeid_cast<const DataTypeLowCardinality *>(type_to_check.get()))
            type_to_check = type_to_check_low_cardinality->getDictionaryType();

        if (type_to_check->canBeInsideNullable())
            output_node = &dag->addFunction(to_nullable_function, {output_node}, output_node->result_name);
    }
}

void appendRequiredOutputsToActions(JoinActionRef & post_filter, ActionsTriple expression_actions)
{
    NameSet required_output_columns_set(required_output_columns.begin(), required_output_columns.end());
    addRequiredInputToOutput(expression_actions.left_pre_join_actions, required_output_columns_set);
    addRequiredInputToOutput(expression_actions.right_pre_join_actions, required_output_columns_set);
    addRequiredInputToOutput(expression_actions.post_join_actions, required_output_columns_set);

    ActionsDAG::NodeRawConstPtrs new_outputs;
    for (const auto * output : expression_actions.post_join_actions->getOutputs())
    {
        if (required_output_columns_set.contains(output->result_name))
            new_outputs.push_back(output);
    }

    if (new_outputs.empty())
    {
        new_outputs = getAnyColumn(expression_actions.post_join_actions->getOutputs());
    }

    if (post_filter)
        new_outputs.push_back(post_filter.getNode());
    expression_actions.post_join_actions->getOutputs() = std::move(new_outputs);
    expression_actions.post_join_actions->removeUnusedActions();
}

template <typename F>
void forEachJoinAction(JoinCondition & join_condition, F && f)
{
    for (auto & pred : join_condition.predicates)
    {
        f(pred.left_node);
        f(pred.right_node);
    }

    for (auto & node : join_condition.restrict_conditions)
    {
        f(node);
    }
}


template <typename F>
void forEachJoinAction(JoinExpression & join_expression, F && f)
{
    forEachJoinAction(join_expression.condition, f);
    for (auto & join_condition : join_expression.disjunctive_conditions)
        forEachJoinAction(join_condition, f);
}


void buildPhysicalJoinNode(
    JoinStepLogical::PhysicalJoinNode & result_node,
    JoinStepLogical::PhysicalJoinTree & join_tree,
    JoinOperator & join_info,
    JoinPlanningContext & join_context,
    bool is_explain_logical)
{

    auto left_rels = result_node.left_child;
    auto right_rels = result_node.right_child;

    if ((left_rels & right_rels).any())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Left and right tables are intersected: {} <-> {}", left_rels.to_ullong(), right_rels.to_ullong());

    LOG_DEBUG(&Poco::Logger::get("XXXX"), "{}:{}: JOINIGN: [{}] <-> [{}]", __FILE__, __LINE__,
        fmt::join(bitSetToList(result_node.left_child), ","),
        fmt::join(bitSetToList(result_node.right_child), ","));

    auto * post_join_actions = result_node.actions.get();
    auto * left_pre_join_actions = join_tree.nodes.at(result_node.left_child).actions.get();
    auto * right_pre_join_actions = join_tree.nodes.at(result_node.right_child).actions.get();

    LOG_DEBUG(&Poco::Logger::get("XXXX"), "{}:{}: > post_join_actions {}", __FILE__, __LINE__, post_join_actions->dumpDAG());
    LOG_DEBUG(&Poco::Logger::get("XXXX"), "{}:{}: > left_pre_join_actions {}", __FILE__, __LINE__, left_pre_join_actions->dumpDAG());
    LOG_DEBUG(&Poco::Logger::get("XXXX"), "{}:{}: > right_pre_join_actions {}", __FILE__, __LINE__, right_pre_join_actions->dumpDAG());
    forEachJoinAction(join_info.expression, [&](JoinActionRef & action)
    {
        if (action.canBeCalculated(result_node.left_child))
            action.setActions(left_pre_join_actions);
        else if (action.canBeCalculated(result_node.right_child))
            action.setActions(right_pre_join_actions);
        else if (action.canBeCalculated(result_node.left_child | result_node.right_child))
            action.setActions(post_join_actions);
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot determine actions for JoinActionRef {}", action.getColumnName());
    });

    for (auto && [src, actions] : join_info.expression_actions.actions)
    {
        if (!actions)
            continue;
        ActionsDAG * join_actions = nullptr;
        if (isSubsetOf(src, result_node.left_child))
            join_actions = left_pre_join_actions;
        else if (isSubsetOf(src, result_node.right_child))
            join_actions = right_pre_join_actions;
        else if (isSubsetOf(src, result_node.left_child | result_node.right_child))
            join_actions = post_join_actions;
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot determine actions source: left {}, right{}, actions {} {}",
                result_node.left_child.to_ullong(), result_node.right_child.to_ullong(), src.to_ullong(), actions->dumpNames());
        join_actions->mergeInplace(std::move(*actions));
    }
    join_info.expression_actions.actions.clear();

    LOG_DEBUG(&Poco::Logger::get("XXXX"), "{}:{}: >>> post_join_actions {}", __FILE__, __LINE__, post_join_actions->dumpDAG());
    LOG_DEBUG(&Poco::Logger::get("XXXX"), "{}:{}: >>> left_pre_join_actions {}", __FILE__, __LINE__, left_pre_join_actions->dumpDAG());
    LOG_DEBUG(&Poco::Logger::get("XXXX"), "{}:{}: >>> right_pre_join_actions {}", __FILE__, __LINE__, right_pre_join_actions->dumpDAG());

    auto & join_expression = join_info.expression;
    auto & table_join = join_context.table_join;

    join_context.is_asof = join_info.strictness == JoinStrictness::Asof;
    join_context.is_using = join_expression.is_using;

    auto & table_join_clauses = table_join->getClauses();

    if (!isCrossOrComma(join_info.kind) && !isPaste(join_info.kind))
    {
        bool has_keys = addJoinConditionToTableJoin(
            join_expression.condition, table_join_clauses.emplace_back(),
            post_join_actions, join_context);

        if (!has_keys)
        {
            if (!TableJoin::isEnabledAlgorithm(join_context.join_settings.join_algorithms, JoinAlgorithm::HASH))
                throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION, "Cannot convert JOIN ON expression to CROSS JOIN, because hash join is disabled");

            table_join_clauses.pop_back();
            bool can_convert_to_cross = (isInner(join_info.kind) || isCrossOrComma(join_info.kind))
                && join_info.strictness == JoinStrictness::All
                && join_expression.disjunctive_conditions.empty()
                && join_expression.condition.restrict_conditions.empty();

            if (!can_convert_to_cross)
                throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION, "Cannot determine join keys in JOIN ON expression {}",
                    formatJoinCondition(join_expression.condition));
            join_info.kind = JoinKind::Cross;
        }
    }

    if (auto left_pre_filter_condition = concatMergeConditions(join_expression.condition.restrict_conditions, left_pre_join_actions))
    {
        table_join_clauses.at(table_join_clauses.size() - 1).analyzer_left_filter_condition_column_name = left_pre_filter_condition.getColumnName();
    }

    if (auto right_pre_filter_condition = concatMergeConditions(join_expression.condition.restrict_conditions, right_pre_join_actions))
    {
        table_join_clauses.at(table_join_clauses.size() - 1).analyzer_right_filter_condition_column_name = right_pre_filter_condition.getColumnName();
    }

    if (join_info.strictness == JoinStrictness::Asof)
    {
        if (!join_expression.disjunctive_conditions.empty())
            throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION, "ASOF join does not support multiple disjuncts in JOIN ON expression");

        /// Find strictly only one inequality in predicate list for ASOF join
        chassert(table_join_clauses.size() == 1);
        auto & join_predicates = join_expression.condition.predicates;
        bool asof_predicate_found = false;
        for (auto & predicate : join_predicates)
        {
            predicateOperandsToCommonType(predicate, join_context);
            auto asof_inequality_op = operatorToAsofInequality(predicate.op);
            if (!asof_inequality_op)
                continue;

            if (asof_predicate_found)
                throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION, "ASOF join does not support multiple inequality predicates in JOIN ON expression");
            asof_predicate_found = true;
            table_join->setAsofInequality(*asof_inequality_op);
            table_join_clauses.front().addKey(predicate.left_node.getColumnName(), predicate.right_node.getColumnName(), /* null_safe_comparison = */ false);
        }
        if (!asof_predicate_found)
            throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION, "ASOF join requires one inequality predicate in JOIN ON expression, in {}",
                formatJoinCondition(join_expression.condition));
    }

    for (auto & join_condition : join_expression.disjunctive_conditions)
    {
        auto & table_join_clause = table_join_clauses.emplace_back();
        bool has_keys = addJoinConditionToTableJoin(join_condition, table_join_clause, post_join_actions, join_context);
        if (!has_keys)
            throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION, "Cannot determine join keys in JOIN ON expression {}",
                formatJoinCondition(join_condition));
        if (auto left_pre_filter_condition = concatMergeConditions(join_condition.restrict_conditions, left_pre_join_actions))
            table_join_clause.analyzer_left_filter_condition_column_name = left_pre_filter_condition.getColumnName();
        if (auto right_pre_filter_condition = concatMergeConditions(join_condition.restrict_conditions, right_pre_join_actions))
            table_join_clause.analyzer_right_filter_condition_column_name = right_pre_filter_condition.getColumnName();
    }

    JoinActionRef residual_filter_condition(nullptr);
    if (join_expression.disjunctive_conditions.empty())
    {
        residual_filter_condition = concatMergeConditions(join_expression.condition.restrict_conditions, post_join_actions);
    }
    else
    {
        auto is_residual_condition = [=](const auto & node) { return node.getActions() == post_join_actions; };
        bool need_residual_filter = std::ranges::any_of(join_expression.condition.restrict_conditions, is_residual_condition);
        for (const auto & join_condition : join_expression.disjunctive_conditions)
        {
            need_residual_filter = need_residual_filter || std::ranges::any_of(join_condition.restrict_conditions, is_residual_condition);
            if (need_residual_filter)
                break;
        }

        if (need_residual_filter)
            residual_filter_condition = buildSingleActionForJoinExpression(join_expression, {left_pre_join_actions, right_pre_join_actions, post_join_actions});
    }

    bool need_add_nullable = join_info.kind == JoinKind::Left
        || join_info.kind == JoinKind::Right
        || join_info.kind == JoinKind::Full;

    if (need_add_nullable && join_context.use_nulls)
    {
        if (residual_filter_condition)
        {
            throw Exception(
                ErrorCodes::INVALID_JOIN_ON_EXPRESSION,
                "{} JOIN ON expression '{}' contains column from left and right table, which is not supported with `join_use_nulls`",
                toString(join_info.kind), residual_filter_condition.getColumnName());
        }

        auto to_nullable_function = FunctionFactory::instance().get("toNullable", nullptr);
        if (join_info.kind == JoinKind::Left || join_info.kind == JoinKind::Full)
            addToNullableActions(right_pre_join_actions, to_nullable_function);
        if (join_info.kind == JoinKind::Right || join_info.kind == JoinKind::Full)
            addToNullableActions(left_pre_join_actions, to_nullable_function);
    }

    if (residual_filter_condition && canPushDownFromOn(join_info))
    {
        result_node.filter = residual_filter_condition;
    }
    else if (residual_filter_condition)
    {
        ActionsDAG dag;
        if (is_explain_logical)
        {
            /// Keep post_join_actions for explain
            dag = post_join_actions->clone();
        }
        else
        {
            /// Move post_join_actions to join, replace with no-op dag
            dag = std::move(*post_join_actions);
            *post_join_actions = ActionsDAG(dag.getRequiredColumns());
        }
        auto & outputs = dag.getOutputs();
        for (const auto * node : outputs)
        {
            if (node->result_name == residual_filter_condition.getColumnName())
            {
                outputs = {node};
                break;
            }
        }
        ExpressionActionsPtr & mixed_join_expression = table_join->getMixedJoinExpression();
        mixed_join_expression = std::make_shared<ExpressionActions>(std::move(dag), join_context.expression_actions_settings);
    }

    appendRequiredOutputsToActions(post_filter, {left_pre_join_actions, right_pre_join_actions, post_join_actions});

    table_join->setInputColumns(
        left_pre_join_actions->getNamesAndTypesList(),
        right_pre_join_actions->getNamesAndTypesList());
    table_join->setUsedColumns(post_join_actions->getRequiredColumnsNames());
    table_join->setJoinInfo(join_info);

    Block left_sample_block = blockWithColumns(left_pre_join_actions->getResultColumns());
    Block right_sample_block = blockWithColumns(right_pre_join_actions->getResultColumns());

    // if (swap_inputs)
    // {
    //     table_join->swapSides();
    //     std::swap(left_sample_block, right_sample_block);
    //     std::swap(hash_table_key_hash_left, hash_table_key_hash_right);
    // }

    JoinAlgorithmSettings algo_settings(
        join_context.join_settings,
        join_context.max_threads,
        join_context.max_entries_for_hash_table_stats,
        std::move(join_context.initial_query_id),
        join_context.lock_acquire_timeout);

    auto join_algorithm_ptr = chooseJoinAlgorithm(
        table_join, join_context.prepared_join_storage, left_sample_block, right_sample_block, algo_settings, join_context.hash_table_key_hash);

    result_node.join_strategy = join_algorithm_ptr;
}

struct JoinEdge
{
    BaseRelsSet lhs;
    BaseRelsSet rhs;

    JoinOperator join_operator;
};

JoinStepLogical::PhysicalJoinTree JoinStepLogical::convertToPhysical(
    bool is_explain_logical,
    UInt64 max_threads,
    UInt64 max_entries_for_hash_table_stats,
    String initial_query_id,
    std::chrono::milliseconds lock_acquire_timeout,
    const ExpressionActionsSettings & actions_settings)
{
    std::vector<JoinEdge> join_order;
    for (size_t i = 0; i < join_operators.size(); ++i)
    {
        UInt64 rhs = 1u << (i + 1);
        join_order.push_back({
            rhs - 1, rhs,
            std::move(join_operators[i])});
    }

    PhysicalJoinTree physical_tree;

    std::vector<ColumnsWithTypeAndName> current_headers;
    for (const auto & header : input_headers)
        current_headers.push_back(header.getColumnsWithTypeAndName());

    for (auto & join_edge : join_order)
    {
        ActionsDAGPtr left_actions;

        for (auto src : {join_edge.lhs, join_edge.rhs})
        {
            auto & child_node = physical_tree.nodes[src];
            if (src.count() == 1)
            {
                child_node.actions = std::move(join_edge.join_operator.expression_actions.getActions(src, current_headers));
                child_node.left_child = 0;
                child_node.right_child = 0;
            }
        }

        auto & physical_node = physical_tree.nodes[join_edge.lhs | join_edge.rhs];
        physical_node.left_child = join_edge.lhs;
        physical_node.right_child = join_edge.rhs;

        ColumnsWithTypeAndName current_step_inputs;
        for (auto child : {join_edge.lhs, join_edge.rhs})
        {
            for (const auto * col : physical_tree.nodes[child].actions->getOutputs())
            {
                if (required_output_columns.contains(col->result_name))
                {
                    ColumnWithTypeAndName column(nullptr, col->result_type, col->result_name);
                    if ((child == join_edge.lhs && (join_edge.join_operator.kind == JoinKind::Right || join_edge.join_operator.kind == JoinKind::Full))
                     || (child == join_edge.rhs && (join_edge.join_operator.kind == JoinKind::Left || join_edge.join_operator.kind == JoinKind::Full)))
                        column.type = JoinCommon::convertTypeToNullable(column.type);
                    current_step_inputs.push_back(std::move(column));

                }
            }
        }
        physical_node.actions = std::make_unique<ActionsDAG>(current_step_inputs);

        auto table_join = std::make_shared<TableJoin>(join_settings, use_nulls,
            Context::getGlobalContextInstance()->getGlobalTemporaryVolume(),
            Context::getGlobalContextInstance()->getTempDataOnDisk());

        JoinPlanningContext join_context{
            table_join,
            join_settings,
            sorting_settings,
            actions_settings,
            required_output_columns,
            prepared_join_storage,
            max_threads,
            max_entries_for_hash_table_stats,
            initial_query_id,
            lock_acquire_timeout,
        };

        if (prepared_join_storage)
        {
            prepared_join_storage.visit([&table_join](const auto & storage_)
            {
                table_join->setStorageJoin(storage_);
            });
        }

        if (join_edge.rhs.count() == 1)
        {
            size_t pos = std::countr_zero(join_edge.rhs.to_ullong());
            if (hash_table_key_hashes.size() > pos)
                join_context.hash_table_key_hash = hash_table_key_hashes.at(pos);
        }

        buildPhysicalJoinNode(physical_node, physical_tree, join_edge.join_operator, join_context, is_explain_logical);
    }

    return physical_tree;
}

bool JoinStepLogical::hasPreparedJoinStorage() const
{
    return prepared_join_storage;
}

bool JoinStepLogical::canFlatten() const
{
    if (join_operators.at(0).kind == JoinKind::Full || join_operators.at(0).kind == JoinKind::Paste)
        return false;
    if (join_operators.at(0).strictness != JoinStrictness::All)
        return false;
    if (prepared_join_storage)
        return false;
    if (getNumberOfTables() >= BaseRelsSet().size())
        return false;
    return true;
}


std::optional<ActionsDAG> JoinStepLogical::getFilterActions(JoinTableSide side, String & filter_column_name)
{
    UNUSED(side);
    UNUSED(filter_column_name);
    /*
    if (join_info.strictness != JoinStrictness::All)
        return {};

    auto & join_expression = join_info.expression;
    if (!join_expression.disjunctive_conditions.empty())
        return {};

    if (!canPushDownFromOn(join_info, side))
        return {};

    const ActionsDAGPtr & actions_dag = side == JoinTableSide::Left ? expression_actions.left_pre_join_actions : expression_actions.right_pre_join_actions;
    std::vector<JoinActionRef> & conditions = side == JoinTableSide::Left ? join_expression.condition.left_filter_conditions : join_expression.condition.right_filter_conditions;

    if (auto filter_condition = concatMergeConditions(conditions, actions_dag))
    {
        filter_column_name = filter_condition.getColumnName();
        conditions.clear();
        ActionsDAG new_dag(actions_dag->getResultColumns());
        new_dag.getOutputs() = new_dag.getInputs();

        ActionsDAG result = std::move(*actions_dag);
        *actions_dag = std::move(new_dag);

        updateInputHeader(result.getResultColumns(), side == JoinTableSide::Left ? 0 : 1);

        return result;
    }
    */
    return {};
}

void JoinStepLogical::serializeSettings(QueryPlanSerializationSettings & settings) const
{
    join_settings.updatePlanSettings(settings);
    sorting_settings.updatePlanSettings(settings);
}

void JoinStepLogical::serialize(Serialization & ctx) const
{
    if (prepared_join_storage)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Serialization of JoinStepLogical with prepared storage is not implemented");

    if (swap_inputs)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Serialization of JoinStepLogical with swapped inputs is not implemented");

    UInt8 flags = 0;
    if (use_nulls)
        flags |= 1;

    writeIntBinary(flags, ctx.out);

    JoinActionRef::ActionsDAGRawPtrs dags = {
        // TODO
    };

    writeVarUInt(dags.size(), ctx.out);
    for (const auto * dag : dags)
        dag->serialize(ctx.out, ctx.registry);

    /// TODO
    join_operators[0].serialize(ctx.out, dags);

    writeVarUInt(required_output_columns.size(), ctx.out);
    for (const auto & name : required_output_columns)
        writeStringBinary(name, ctx.out);
}

std::unique_ptr<IQueryPlanStep> JoinStepLogical::deserialize(Deserialization & ctx)
{
    if (ctx.input_headers.size() != 2)
        throw Exception(ErrorCodes::INCORRECT_DATA, "JoinStepLogical must have two input streams");

    UInt8 flags;
    readIntBinary(flags, ctx.in);

    bool use_nulls = flags & 1;

    std::vector<ActionsDAGPtr> dag_ptrs;
    {
        UInt64 num_dags;
        readVarUInt(num_dags, ctx.in);

        if (num_dags != 3)
            throw Exception(ErrorCodes::INCORRECT_DATA, "JoinStepLogical deserialization expect 3 DAGs, got {}", num_dags);

        dag_ptrs.reserve(num_dags);
        for (size_t i = 0; i < num_dags; ++i)
            dag_ptrs.emplace_back(std::make_unique<ActionsDAG>(ActionsDAG::deserialize(ctx.in, ctx.registry, ctx.context)));
    }

    JoinExpressionActions expression_actions;

    JoinActionRef::ActionsDAGRawPtrs dags = {
        /// TODO
    };

    auto join_info = JoinOperator::deserialize(ctx.in, dags);

    SortingStep::Settings sort_settings(ctx.settings);
    JoinSettings join_settings(ctx.settings);

    return std::make_unique<JoinStepLogical>(
        ctx.input_headers.front(),
        use_nulls,
        std::move(join_settings),
        std::move(sort_settings));
}

void registerJoinStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("Join", JoinStepLogical::deserialize);
}

}
