#include <Interpreters/Set.h>
#include <Common/ProfileEvents.h>
#include <Interpreters/ArrayJoinAction.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/Context.h>
#include <Columns/ColumnArray.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/IFunction.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <optional>
#include <Columns/ColumnSet.h>
#include <queue>
#include <stack>

#if defined(MEMORY_SANITIZER)
    #include <sanitizer/msan_interface.h>
#endif

#if defined(ADDRESS_SANITIZER)
    #include <sanitizer/asan_interface.h>
#endif

namespace ProfileEvents
{
    extern const Event FunctionExecute;
    extern const Event CompiledFunctionExecute;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_FOUND_COLUMN_IN_BLOCK;
    extern const int TOO_MANY_TEMPORARY_COLUMNS;
    extern const int TOO_MANY_TEMPORARY_NON_CONST_COLUMNS;
    extern const int TYPE_MISMATCH;
}

ExpressionActions::~ExpressionActions() = default;

ExpressionActions::ExpressionActions(ActionsDAGPtr actions_dag_)
{
    actions_dag = actions_dag_->clone();

    actions_dag->compileExpressions();

    linearizeActions();

    const auto & settings = actions_dag->getSettings();

    if (settings.max_temporary_columns && num_columns > settings.max_temporary_columns)
        throw Exception(ErrorCodes::TOO_MANY_TEMPORARY_COLUMNS,
                        "Too many temporary columns: {}. Maximum: {}",
                        actions_dag->dumpNames(), std::to_string(settings.max_temporary_columns));
}

ExpressionActionsPtr ExpressionActions::clone() const
{
    return std::make_shared<ExpressionActions>(*this);
}

void ExpressionActions::linearizeActions()
{
    /// This function does the topological sort or DAG and fills all the fields of ExpressionActions.
    /// Algorithm traverses DAG starting from nodes without children.
    /// For every node we support the number of created children, and if all children are created, put node into queue.
    struct Data
    {
        const Node * node = nullptr;
        size_t num_created_children = 0;
        std::vector<const Node *> parents;

        ssize_t position = -1;
        size_t num_created_parents = 0;
        bool used_in_result = false;
    };

    const auto & nodes = getNodes();
    const auto & index = actions_dag->getIndex();
    const auto & inputs = actions_dag->getInputs();

    std::vector<Data> data(nodes.size());
    std::unordered_map<const Node *, size_t> reverse_index;

    for (const auto & node : nodes)
    {
        size_t id = reverse_index.size();
        data[id].node = &node;
        reverse_index[&node] = id;
    }

    /// There are independent queues for arrayJoin and other actions.
    /// We delay creation of arrayJoin as long as we can, so that they will be executed closer to end.
    std::queue<const Node *> ready_nodes;
    std::queue<const Node *> ready_array_joins;

    for (const auto * node : index)
        data[reverse_index[node]].used_in_result = true;

    for (const auto & node : nodes)
    {
        for (const auto & child : node.children)
            data[reverse_index[child]].parents.emplace_back(&node);
    }

    for (const auto & node : nodes)
    {
        if (node.children.empty())
            ready_nodes.emplace(&node);
    }

    /// Every argument will have fixed position in columns list.
    /// If argument is removed, it's position may be reused by other action.
    std::stack<size_t> free_positions;

    while (!ready_nodes.empty() || !ready_array_joins.empty())
    {
        auto & stack = ready_nodes.empty() ? ready_array_joins : ready_nodes;
        const Node * node = stack.front();
        stack.pop();

        auto & cur = data[reverse_index[node]];

        /// Select position for action result.
        size_t free_position = num_columns;
        if (free_positions.empty())
            ++num_columns;
        else
        {
            free_position = free_positions.top();
            free_positions.pop();
        }

        cur.position = free_position;

        ExpressionActions::Arguments arguments;
        arguments.reserve(cur.node->children.size());
        for (auto * child : cur.node->children)
        {
            auto & arg = data[reverse_index[child]];

            if (arg.position < 0)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Argument was not calculated for {}", child->result_name);

            ++arg.num_created_parents;

            ExpressionActions::Argument argument;
            argument.pos = arg.position;
            argument.needed_later = arg.used_in_result || arg.num_created_parents != arg.parents.size();

            if (!argument.needed_later)
                free_positions.push(argument.pos);

            arguments.emplace_back(argument);
        }

        if (node->type == ActionsDAG::ActionType::INPUT)
        {
            /// Argument for input is special. It contains the position from required columns.
            ExpressionActions::Argument argument;
            argument.needed_later = !cur.parents.empty();
            arguments.emplace_back(argument);

            //required_columns.push_back({node->result_name, node->result_type});
        }

        actions.push_back({node, arguments, free_position});

        for (const auto & parent : cur.parents)
        {
            auto & parent_data = data[reverse_index[parent]];
            ++parent_data.num_created_children;

            if (parent_data.num_created_children == parent->children.size())
            {
                auto & push_stack = parent->type == ActionsDAG::ActionType::ARRAY_JOIN ? ready_array_joins : ready_nodes;
                push_stack.push(parent);
            }
        }
    }

    result_positions.reserve(index.size());

    for (const auto & node : index)
    {
        auto pos = data[reverse_index[node]].position;

        if (pos < 0)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Action for {} was not calculated", node->result_name);

        result_positions.push_back(pos);

        ColumnWithTypeAndName col{node->column, node->result_type, node->result_name};
        sample_block.insert(std::move(col));
    }

    for (const auto * input : inputs)
    {
        const auto & cur = data[reverse_index[input]];
        auto pos = required_columns.size();
        actions[cur.position].arguments.front().pos = pos;
        required_columns.push_back({input->result_name, input->result_type});
        input_positions[input->result_name].emplace_back(pos);
    }
}


static WriteBuffer & operator << (WriteBuffer & out, const ExpressionActions::Argument & argument)
{
    return out << (argument.needed_later ? ": " : ":: ") << argument.pos;
}

std::string ExpressionActions::Action::toString() const
{
    WriteBufferFromOwnString out;
    switch (node->type)
    {
        case ActionsDAG::ActionType::COLUMN:
            out << "COLUMN "
                << (node->column ? node->column->getName() : "(no column)");
            break;

        case ActionsDAG::ActionType::ALIAS:
            out << "ALIAS " << node->children.front()->result_name << " " << arguments.front();
            break;

        case ActionsDAG::ActionType::FUNCTION:
            out << "FUNCTION " << (node->is_function_compiled ? "[compiled] " : "")
                << (node->function_base ? node->function_base->getName() : "(no function)") << "(";
            for (size_t i = 0; i < node->children.size(); ++i)
            {
                if (i)
                    out << ", ";
                out << node->children[i]->result_name << " " << arguments[i];
            }
            out << ")";
            break;

        case ActionsDAG::ActionType::ARRAY_JOIN:
            out << "ARRAY JOIN " << node->children.front()->result_name << " " << arguments.front();
            break;

        case ActionsDAG::ActionType::INPUT:
            out << "INPUT " << arguments.front();
            break;
    }

    out << " -> " << node->result_name
        << " " << (node->result_type ? node->result_type->getName() : "(no type)") << " : " << result_position;
    return out.str();
}

void ExpressionActions::checkLimits(const ColumnsWithTypeAndName & columns) const
{
    auto max_temporary_non_const_columns = actions_dag->getSettings().max_temporary_non_const_columns;
    if (max_temporary_non_const_columns)
    {
        size_t non_const_columns = 0;
        for (const auto & column : columns)
            if (column.column && !isColumnConst(*column.column))
                ++non_const_columns;

        if (non_const_columns > max_temporary_non_const_columns)
        {
            WriteBufferFromOwnString list_of_non_const_columns;
            for (const auto & column : columns)
                if (column.column && !isColumnConst(*column.column))
                    list_of_non_const_columns << "\n" << column.name;

            throw Exception("Too many temporary non-const columns:" + list_of_non_const_columns.str()
                + ". Maximum: " + std::to_string(max_temporary_non_const_columns),
                ErrorCodes::TOO_MANY_TEMPORARY_NON_CONST_COLUMNS);
        }
    }
}

namespace
{
    /// This struct stores context needed to execute actions.
    ///
    /// Execution model is following:
    ///   * execution is performed over list of columns (with fixed size = ExpressionActions::num_columns)
    ///   * every argument has fixed position in columns list, every action has fixed position for result
    ///   * if argument is not needed anymore (Argument::needed_later == false), it is removed from list
    ///   * argument for INPUT is in inputs[inputs_pos[argument.pos]]
    ///
    /// Columns on positions `ExpressionActions::result_positions` are inserted back into block.
    struct ExecutionContext
    {
        ColumnsWithTypeAndName & inputs;
        ColumnsWithTypeAndName columns = {};
        std::vector<ssize_t> inputs_pos = {};
        size_t num_rows;
    };
}

static void executeAction(const ExpressionActions::Action & action, ExecutionContext & execution_context, bool dry_run)
{
    auto & inputs = execution_context.inputs;
    auto & columns = execution_context.columns;
    auto & num_rows = execution_context.num_rows;

    switch (action.node->type)
    {
        case ActionsDAG::ActionType::FUNCTION:
        {
            auto & res_column = columns[action.result_position];
            if (res_column.type || res_column.column)
                throw Exception("Result column is not empty", ErrorCodes::LOGICAL_ERROR);

            res_column.type = action.node->result_type;
            res_column.name = action.node->result_name;

            ColumnsWithTypeAndName arguments(action.arguments.size());
            for (size_t i = 0; i < arguments.size(); ++i)
            {
                if (!action.arguments[i].needed_later)
                    arguments[i] = std::move(columns[action.arguments[i].pos]);
                else
                    arguments[i] = columns[action.arguments[i].pos];
            }

            ProfileEvents::increment(ProfileEvents::FunctionExecute);
            if (action.node->is_function_compiled)
                ProfileEvents::increment(ProfileEvents::CompiledFunctionExecute);

            res_column.column = action.node->function->execute(arguments, res_column.type, num_rows, dry_run);
            break;
        }

        case ActionsDAG::ActionType::ARRAY_JOIN:
        {
            size_t array_join_key_pos = action.arguments.front().pos;
            auto array_join_key = columns[array_join_key_pos];

            /// Remove array join argument in advance if it is not needed.
            if (!action.arguments.front().needed_later)
                columns[array_join_key_pos] = {};

            array_join_key.column = array_join_key.column->convertToFullColumnIfConst();

            const ColumnArray * array = typeid_cast<const ColumnArray *>(array_join_key.column.get());
            if (!array)
                throw Exception("ARRAY JOIN of not array: " + action.node->result_name, ErrorCodes::TYPE_MISMATCH);

            for (auto & column : columns)
                if (column.column)
                    column.column = column.column->replicate(array->getOffsets());

            for (auto & column : inputs)
                if (column.column)
                    column.column = column.column->replicate(array->getOffsets());

            auto & res_column = columns[action.result_position];

            res_column.column = array->getDataPtr();
            res_column.type = assert_cast<const DataTypeArray &>(*array_join_key.type).getNestedType();
            res_column.name = action.node->result_name;

            num_rows = res_column.column->size();
            break;
        }

        case ActionsDAG::ActionType::COLUMN:
        {
            auto & res_column = columns[action.result_position];
            res_column.column = action.node->column->cloneResized(num_rows);
            res_column.type = action.node->result_type;
            res_column.name = action.node->result_name;
            break;
        }

        case ActionsDAG::ActionType::ALIAS:
        {
            const auto & arg = action.arguments.front();
            if (action.result_position != arg.pos)
            {
                columns[action.result_position].column = columns[arg.pos].column;
                columns[action.result_position].type = columns[arg.pos].type;

                if (!arg.needed_later)
                    columns[arg.pos] = {};
            }

            columns[action.result_position].name = action.node->result_name;

            break;
        }

        case ActionsDAG::ActionType::INPUT:
        {
            auto pos = execution_context.inputs_pos[action.arguments.front().pos];
            if (pos < 0)
            {
                /// Here we allow to skip input if it is not in block (in case it is not needed).
                /// It may be unusual, but some code depend on such behaviour.
                if (action.arguments.front().needed_later)
                    throw Exception(ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK,
                                    "Not found column {} in block",
                                    action.node->result_name);
            }
            else
                columns[action.result_position] = std::move(inputs[pos]);

            break;
        }
    }
}

void ExpressionActions::execute(Block & block, size_t & num_rows, bool dry_run) const
{
    ExecutionContext execution_context
    {
        .inputs = block.data,
        .num_rows = num_rows,
    };

    execution_context.inputs_pos.assign(required_columns.size(), -1);

    for (size_t pos = 0; pos < block.columns(); ++pos)
    {
        const auto & col = block.getByPosition(pos);
        auto it = input_positions.find(col.name);
        if (it != input_positions.end())
        {
            for (auto input_pos : it->second)
            {
                if (execution_context.inputs_pos[input_pos] < 0)
                {
                    execution_context.inputs_pos[input_pos] = pos;
                    break;
                }
            }
        }
    }

    execution_context.columns.resize(num_columns);

    for (const auto & action : actions)
    {
        try
        {
            executeAction(action, execution_context, dry_run);
            checkLimits(execution_context.columns);

            //std::cerr << "Action: " << action.toString() << std::endl;
            //for (const auto & col : execution_context.columns)
            //    std::cerr << col.dumpStructure() << std::endl;
        }
        catch (Exception & e)
        {
            e.addMessage(fmt::format("while executing '{}'", action.toString()));
            throw;
        }
    }

    if (actions_dag->getSettings().project_input)
    {
        block.clear();
    }
    else
    {
        std::sort(execution_context.inputs_pos.rbegin(), execution_context.inputs_pos.rend());
        for (auto input : execution_context.inputs_pos)
            if (input >= 0)
                block.erase(input);
    }

    for (auto pos : result_positions)
        if (execution_context.columns[pos].column)
            block.insert(execution_context.columns[pos]);

    num_rows = execution_context.num_rows;
}

void ExpressionActions::execute(Block & block, bool dry_run) const
{
    size_t num_rows = block.rows();

    execute(block, num_rows, dry_run);

    if (!block)
        block.insert({DataTypeUInt8().createColumnConst(num_rows, 0), std::make_shared<DataTypeUInt8>(), "_dummy"});
}

Names ExpressionActions::getRequiredColumns() const
{
    Names names;
    for (const auto & input : required_columns)
        names.push_back(input.name);
    return names;
}

bool ExpressionActions::hasArrayJoin() const
{
    for (const auto & action : actions)
        if (action.node->type == ActionsDAG::ActionType::ARRAY_JOIN)
            return true;

    return false;
}


std::string ExpressionActions::getSmallestColumn(const NamesAndTypesList & columns)
{
    std::optional<size_t> min_size;
    String res;

    for (const auto & column : columns)
    {
        /// @todo resolve evil constant
        size_t size = column.type->haveMaximumSizeOfValue() ? column.type->getMaximumSizeOfValueInMemory() : 100;

        if (!min_size || size < *min_size)
        {
            min_size = size;
            res = column.name;
        }
    }

    if (!min_size)
        throw Exception("No available columns", ErrorCodes::LOGICAL_ERROR);

    return res;
}

std::string ExpressionActions::dumpActions() const
{
    WriteBufferFromOwnString ss;

    ss << "input:\n";
    for (const auto & input_column : required_columns)
        ss << input_column.name << " " << input_column.type->getName() << "\n";

    ss << "\nactions:\n";
    for (const auto & action : actions)
        ss << action.toString() << '\n';

    ss << "\noutput:\n";
    NamesAndTypesList output_columns = sample_block.getNamesAndTypesList();
    for (const auto & output_column : output_columns)
        ss << output_column.name << " " << output_column.type->getName() << "\n";

    ss << "\nproject input: " << actions_dag->getSettings().project_input << "\noutput positions:";
    for (auto pos : result_positions)
        ss << " " << pos;
    ss << "\n";

    return ss.str();
}

bool ExpressionActions::checkColumnIsAlwaysFalse(const String & column_name) const
{
    /// Check has column in (empty set).
    String set_to_check;

    for (auto it = actions.rbegin(); it != actions.rend(); ++it)
    {
        const auto & action = *it;
        if (action.node->type == ActionsDAG::ActionType::FUNCTION && action.node->function_base)
        {
            if (action.node->result_name == column_name && action.node->children.size() > 1)
            {
                auto name = action.node->function_base->getName();
                if ((name == "in" || name == "globalIn"))
                {
                    set_to_check = action.node->children[1]->result_name;
                    break;
                }
            }
        }
    }

    if (!set_to_check.empty())
    {
        for (const auto & action : actions)
        {
            if (action.node->type == ActionsDAG::ActionType::COLUMN && action.node->result_name == set_to_check)
            {
                // Constant ColumnSet cannot be empty, so we only need to check non-constant ones.
                if (const auto * column_set = checkAndGetColumn<const ColumnSet>(action.node->column.get()))
                {
                    if (column_set->getData()->isCreated() && column_set->getData()->getTotalRowCount() == 0)
                        return true;
                }
            }
        }
    }

    return false;
}

void ExpressionActionsChain::addStep(NameSet non_constant_inputs)
{
    if (steps.empty())
        throw Exception("Cannot add action to empty ExpressionActionsChain", ErrorCodes::LOGICAL_ERROR);

    ColumnsWithTypeAndName columns = steps.back()->getResultColumns();
    for (auto & column : columns)
        if (column.column && isColumnConst(*column.column) && non_constant_inputs.count(column.name))
            column.column = nullptr;

    steps.push_back(std::make_unique<ExpressionActionsStep>(std::make_shared<ActionsDAG>(columns)));
}

void ExpressionActionsChain::finalize()
{
    /// Finalize all steps. Right to left to define unnecessary input columns.
    for (int i = static_cast<int>(steps.size()) - 1; i >= 0; --i)
    {
        Names required_output = steps[i]->required_output;
        std::unordered_map<String, size_t> required_output_indexes;
        for (size_t j = 0; j < required_output.size(); ++j)
            required_output_indexes[required_output[j]] = j;
        auto & can_remove_required_output = steps[i]->can_remove_required_output;

        if (i + 1 < static_cast<int>(steps.size()))
        {
            const NameSet & additional_input = steps[i + 1]->additional_input;
            for (const auto & it : steps[i + 1]->getRequiredColumns())
            {
                if (additional_input.count(it.name) == 0)
                {
                    auto iter = required_output_indexes.find(it.name);
                    if (iter == required_output_indexes.end())
                        required_output.push_back(it.name);
                    else if (!can_remove_required_output.empty())
                        can_remove_required_output[iter->second] = false;
                }
            }
        }
        steps[i]->finalize(required_output);
    }

    /// Adding the ejection of unnecessary columns to the beginning of each step.
    for (size_t i = 1; i < steps.size(); ++i)
    {
        size_t columns_from_previous = steps[i - 1]->getResultColumns().size();

        /// If unnecessary columns are formed at the output of the previous step, we'll add them to the beginning of this step.
        /// Except when we drop all the columns and lose the number of rows in the block.
        if (!steps[i]->getResultColumns().empty()
            && columns_from_previous > steps[i]->getRequiredColumns().size())
            steps[i]->prependProjectInput();
    }
}

std::string ExpressionActionsChain::dumpChain() const
{
    WriteBufferFromOwnString ss;

    for (size_t i = 0; i < steps.size(); ++i)
    {
        ss << "step " << i << "\n";
        ss << "required output:\n";
        for (const std::string & name : steps[i]->required_output)
            ss << name << "\n";
        ss << "\n" << steps[i]->dump() << "\n";
    }

    return ss.str();
}

ExpressionActionsChain::ArrayJoinStep::ArrayJoinStep(ArrayJoinActionPtr array_join_, ColumnsWithTypeAndName required_columns_)
    : Step({})
    , array_join(std::move(array_join_))
    , result_columns(std::move(required_columns_))
{
    for (auto & column : result_columns)
    {
        required_columns.emplace_back(NameAndTypePair(column.name, column.type));

        if (array_join->columns.count(column.name) > 0)
        {
            const auto * array = typeid_cast<const DataTypeArray *>(column.type.get());
            column.type = array->getNestedType();
            /// Arrays are materialized
            column.column = nullptr;
        }
    }
}

void ExpressionActionsChain::ArrayJoinStep::finalize(const Names & required_output_)
{
    NamesAndTypesList new_required_columns;
    ColumnsWithTypeAndName new_result_columns;

    NameSet names(required_output_.begin(), required_output_.end());
    for (const auto & column : result_columns)
    {
        if (array_join->columns.count(column.name) != 0 || names.count(column.name) != 0)
            new_result_columns.emplace_back(column);
    }
    for (const auto & column : required_columns)
    {
        if (array_join->columns.count(column.name) != 0 || names.count(column.name) != 0)
            new_required_columns.emplace_back(column);
    }

    std::swap(required_columns, new_required_columns);
    std::swap(result_columns, new_result_columns);
}

ExpressionActionsChain::JoinStep::JoinStep(
    std::shared_ptr<TableJoin> analyzed_join_,
    JoinPtr join_,
    ColumnsWithTypeAndName required_columns_)
    : Step({})
    , analyzed_join(std::move(analyzed_join_))
    , join(std::move(join_))
    , result_columns(std::move(required_columns_))
{
    for (const auto & column : result_columns)
        required_columns.emplace_back(column.name, column.type);

    analyzed_join->addJoinedColumnsAndCorrectNullability(result_columns);
}

void ExpressionActionsChain::JoinStep::finalize(const Names & required_output_)
{
    /// We need to update required and result columns by removing unused ones.
    NamesAndTypesList new_required_columns;
    ColumnsWithTypeAndName new_result_columns;

    /// That's an input columns we need.
    NameSet required_names(required_output_.begin(), required_output_.end());
    for (const auto & name : analyzed_join->keyNamesLeft())
        required_names.emplace(name);

    for (const auto & column : required_columns)
    {
        if (required_names.count(column.name) != 0)
            new_required_columns.emplace_back(column);
    }

    /// Result will also contain joined columns.
    for (const auto & column : analyzed_join->columnsAddedByJoin())
        required_names.emplace(column.name);

    for (const auto & column : result_columns)
    {
        if (required_names.count(column.name) != 0)
            new_result_columns.emplace_back(column);
    }

    std::swap(required_columns, new_required_columns);
    std::swap(result_columns, new_result_columns);
}

ActionsDAGPtr & ExpressionActionsChain::Step::actions()
{
    return typeid_cast<ExpressionActionsStep *>(this)->actions_dag;
}

const ActionsDAGPtr & ExpressionActionsChain::Step::actions() const
{
    return typeid_cast<const ExpressionActionsStep *>(this)->actions_dag;
}

}
