#include <Interpreters/Set.h>
#include <Common/ProfileEvents.h>
#include <Common/SipHash.h>
#include <Interpreters/ArrayJoinAction.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionJIT.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/Context.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnArray.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <Functions/IFunction.h>
#include <IO/Operators.h>
#include <optional>
#include <Columns/ColumnSet.h>
#include <queue>
#include <stack>

#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

#include <common/defines.h>

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
    extern const int DUPLICATE_COLUMN;
    extern const int UNKNOWN_IDENTIFIER;
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
            argument.pos = required_columns.size();
            argument.needed_later = !cur.parents.empty();
            arguments.emplace_back(argument);

            required_columns.push_back({node->result_name, node->result_type});
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
}


static std::ostream & operator << (std::ostream & out, const ExpressionActions::Argument & argument)
{
    return out << (argument.needed_later ? ": " : ":: ") << argument.pos;
}

std::string ExpressionActions::Action::toString() const
{
    std::stringstream out;
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
            std::stringstream list_of_non_const_columns;
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

    execution_context.inputs_pos.reserve(required_columns.size());

    for (const auto & column : required_columns)
    {
        ssize_t pos = -1;
        if (block.has(column.name))
            pos = block.getPositionByName(column.name);
        execution_context.inputs_pos.push_back(pos);
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

void ActionsDAG::updateHeader(Block & block) const
{
    if (settings.project_input)
        block.clear();
    else
    {
        for (const auto & node : nodes)
            if (node.type == ActionType::INPUT && block.has(node.result_name))
                block.erase(node.result_name);
    }

    for (const auto & node : index)
    {
        ColumnPtr col;

        if (node->column && isColumnConst(*node->column))
            col = node->column->cloneResized(0);

        block.insert({node->column, node->result_type, node->result_name});
    }
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
    std::stringstream ss;
    ss.exceptions(std::ios::failbit);

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


bool ActionsDAG::hasArrayJoin() const
{
    for (const auto & node : nodes)
        if (node.type == ActionType::ARRAY_JOIN)
            return true;

    return false;
}

bool ActionsDAG::empty() const
{
    for (const auto & node : nodes)
        if (node.type != ActionType::INPUT)
            return false;

    return true;
}

ActionsDAGPtr ActionsDAG::splitActionsBeforeArrayJoin(const NameSet & array_joined_columns)
{
    /// Split DAG into two parts.
    /// (this_nodes, this_index) is a part which depends on ARRAY JOIN and stays here.
    /// (split_nodes, split_index) is a part which will be moved before ARRAY JOIN.
    std::list<Node> this_nodes;
    std::list<Node> split_nodes;
    Index this_index;
    Index split_index;

    struct Frame
    {
        Node * node;
        size_t next_child_to_visit = 0;
    };

    struct Data
    {
        bool depend_on_array_join = false;
        bool visited = false;
        bool used_in_result = false;

        /// Copies of node in one of the DAGs.
        /// For COLUMN and INPUT both copies may exist.
        Node * to_this = nullptr;
        Node * to_split = nullptr;
    };

    std::stack<Frame> stack;
    std::unordered_map<Node *, Data> data;

    for (const auto & node : index)
        data[node].used_in_result = true;

    /// DFS. Decide if node depends on ARRAY JOIN and move it to one of the DAGs.
    for (auto & node : nodes)
    {
        if (!data[&node].visited)
            stack.push({.node = &node});

        while (!stack.empty())
        {
            auto & cur = stack.top();
            auto & cur_data = data[cur.node];

            /// At first, visit all children. We depend on ARRAY JOIN if any child does.
            while (cur.next_child_to_visit < cur.node->children.size())
            {
                auto * child = cur.node->children[cur.next_child_to_visit];
                auto & child_data = data[child];

                if (!child_data.visited)
                {
                    stack.push({.node = child});
                    break;
                }

                ++cur.next_child_to_visit;
                if (child_data.depend_on_array_join)
                    cur_data.depend_on_array_join = true;
            }

            /// Make a copy part.
            if (cur.next_child_to_visit == cur.node->children.size())
            {
                if (cur.node->type == ActionType::INPUT && array_joined_columns.count(cur.node->result_name))
                    cur_data.depend_on_array_join = true;

                cur_data.visited = true;
                stack.pop();

                if (cur_data.depend_on_array_join)
                {
                    auto & copy = this_nodes.emplace_back(*cur.node);
                    cur_data.to_this = &copy;

                    /// Replace children to newly created nodes.
                    for (auto & child : copy.children)
                    {
                        auto & child_data = data[child];

                        /// If children is not created, int may be from split part.
                        if (!child_data.to_this)
                        {
                            if (child->type == ActionType::COLUMN) /// Just create new node for COLUMN action.
                            {
                                child_data.to_this = &this_nodes.emplace_back(*child);
                            }
                            else
                            {
                                /// Node from split part is added as new input.
                                Node input_node;
                                input_node.type = ActionType::INPUT;
                                input_node.result_type = child->result_type;
                                input_node.result_name = child->result_name; // getUniqueNameForIndex(index, child->result_name);
                                child_data.to_this = &this_nodes.emplace_back(std::move(input_node));

                                /// This node is needed for current action, so put it to index also.
                                split_index.replace(child_data.to_split);
                            }
                        }

                        child = child_data.to_this;
                    }
                }
                else
                {
                    auto & copy = split_nodes.emplace_back(*cur.node);
                    cur_data.to_split = &copy;

                    /// Replace children to newly created nodes.
                    for (auto & child : copy.children)
                    {
                        child = data[child].to_split;
                        assert(child != nullptr);
                    }

                    if (cur_data.used_in_result)
                    {
                        split_index.replace(&copy);

                        /// If this node is needed in result, add it as input.
                        Node input_node;
                        input_node.type = ActionType::INPUT;
                        input_node.result_type = node.result_type;
                        input_node.result_name = node.result_name;
                        cur_data.to_this = &this_nodes.emplace_back(std::move(input_node));
                    }
                }
            }
        }
    }

    for (auto * node : index)
        this_index.insert(data[node].to_this);

    /// Consider actions are empty if all nodes are constants or inputs.
    bool split_actions_are_empty = true;
    for (const auto & node : split_nodes)
        if (!node.children.empty())
            split_actions_are_empty = false;

    if (split_actions_are_empty)
        return {};

    index.swap(this_index);
    nodes.swap(this_nodes);

    auto split_actions = cloneEmpty();
    split_actions->nodes.swap(split_nodes);
    split_actions->index.swap(split_index);
    split_actions->settings.project_input = false;

    return split_actions;
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
    std::stringstream ss;
    ss.exceptions(std::ios::failbit);

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
    return typeid_cast<ExpressionActionsStep *>(this)->actions;
}

const ActionsDAGPtr & ExpressionActionsChain::Step::actions() const
{
    return typeid_cast<const ExpressionActionsStep *>(this)->actions;
}

ActionsDAG::ActionsDAG(const NamesAndTypesList & inputs)
{
    for (const auto & input : inputs)
        addInput(input.name, input.type);
}

ActionsDAG::ActionsDAG(const ColumnsWithTypeAndName & inputs)
{
    for (const auto & input : inputs)
    {
        if (input.column && isColumnConst(*input.column))
            addInput(input);
        else
            addInput(input.name, input.type);
    }
}

ActionsDAG::Node & ActionsDAG::addNode(Node node, bool can_replace)
{
    auto it = index.find(node.result_name);
    if (it != index.end() && !can_replace)
        throw Exception("Column '" + node.result_name + "' already exists", ErrorCodes::DUPLICATE_COLUMN);

    auto & res = nodes.emplace_back(std::move(node));

    index.replace(&res);
    return res;
}

ActionsDAG::Node & ActionsDAG::getNode(const std::string & name)
{
    auto it = index.find(name);
    if (it == index.end())
        throw Exception("Unknown identifier: '" + name + "'", ErrorCodes::UNKNOWN_IDENTIFIER);

    return **it;
}

const ActionsDAG::Node & ActionsDAG::addInput(std::string name, DataTypePtr type)
{
    Node node;
    node.type = ActionType::INPUT;
    node.result_type = std::move(type);
    node.result_name = std::move(name);

    return addNode(std::move(node));
}

const ActionsDAG::Node & ActionsDAG::addInput(ColumnWithTypeAndName column)
{
    Node node;
    node.type = ActionType::INPUT;
    node.result_type = std::move(column.type);
    node.result_name = std::move(column.name);
    node.column = std::move(column.column);

    return addNode(std::move(node));
}

const ActionsDAG::Node & ActionsDAG::addColumn(ColumnWithTypeAndName column)
{
    if (!column.column)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot add column {} because it is nullptr", column.name);

    Node node;
    node.type = ActionType::COLUMN;
    node.result_type = std::move(column.type);
    node.result_name = std::move(column.name);
    node.column = std::move(column.column);

    return addNode(std::move(node));
}

const ActionsDAG::Node & ActionsDAG::addAlias(const std::string & name, std::string alias, bool can_replace)
{
    auto & child = getNode(name);

    Node node;
    node.type = ActionType::ALIAS;
    node.result_type = child.result_type;
    node.result_name = std::move(alias);
    node.column = child.column;
    node.allow_constant_folding = child.allow_constant_folding;
    node.children.emplace_back(&child);

    return addNode(std::move(node), can_replace);
}

const ActionsDAG::Node & ActionsDAG::addArrayJoin(const std::string & source_name, std::string result_name)
{
    auto & child = getNode(source_name);

    const DataTypeArray * array_type = typeid_cast<const DataTypeArray *>(child.result_type.get());
    if (!array_type)
        throw Exception("ARRAY JOIN requires array argument", ErrorCodes::TYPE_MISMATCH);

    Node node;
    node.type = ActionType::ARRAY_JOIN;
    node.result_type = array_type->getNestedType();
    node.result_name = std::move(result_name);
    node.children.emplace_back(&child);

    return addNode(std::move(node));
}

const ActionsDAG::Node & ActionsDAG::addFunction(
    const FunctionOverloadResolverPtr & function,
    const Names & argument_names,
    std::string result_name,
    const Context & context [[maybe_unused]])
{
    const auto & all_settings = context.getSettingsRef();
    settings.max_temporary_columns = all_settings.max_temporary_columns;
    settings.max_temporary_non_const_columns = all_settings.max_temporary_non_const_columns;

#if USE_EMBEDDED_COMPILER
    settings.compile_expressions = settings.compile_expressions;
    settings.min_count_to_compile_expression = settings.min_count_to_compile_expression;

    if (!compilation_cache)
        compilation_cache = context.getCompiledExpressionCache();
#endif

    size_t num_arguments = argument_names.size();

    Node node;
    node.type = ActionType::FUNCTION;
    node.function_builder = function;
    node.children.reserve(num_arguments);

    bool all_const = true;
    ColumnsWithTypeAndName arguments(num_arguments);

    for (size_t i = 0; i < num_arguments; ++i)
    {
        auto & child = getNode(argument_names[i]);
        node.children.emplace_back(&child);
        node.allow_constant_folding = node.allow_constant_folding && child.allow_constant_folding;

        ColumnWithTypeAndName argument;
        argument.column = child.column;
        argument.type = child.result_type;
        argument.name = child.result_name;

        if (!argument.column || !isColumnConst(*argument.column))
            all_const = false;

        arguments[i] = std::move(argument);
    }

    node.function_base = function->build(arguments);
    node.result_type = node.function_base->getResultType();
    node.function = node.function_base->prepare(arguments);

    /// If all arguments are constants, and function is suitable to be executed in 'prepare' stage - execute function.
    /// But if we compile expressions compiled version of this function maybe placed in cache,
    /// so we don't want to unfold non deterministic functions
    if (all_const && node.function_base->isSuitableForConstantFolding()
        && (!settings.compile_expressions || node.function_base->isDeterministic()))
    {
        size_t num_rows = arguments.empty() ? 0 : arguments.front().column->size();
        auto col = node.function->execute(arguments, node.result_type, num_rows, true);

        /// If the result is not a constant, just in case, we will consider the result as unknown.
        if (isColumnConst(*col))
        {
            /// All constant (literal) columns in block are added with size 1.
            /// But if there was no columns in block before executing a function, the result has size 0.
            /// Change the size to 1.

            if (col->empty())
                col = col->cloneResized(1);

            node.column = std::move(col);
        }
    }

    /// Some functions like ignore() or getTypeName() always return constant result even if arguments are not constant.
    /// We can't do constant folding, but can specify in sample block that function result is constant to avoid
    /// unnecessary materialization.
    if (!node.column && node.function_base->isSuitableForConstantFolding())
    {
        if (auto col = node.function_base->getResultIfAlwaysReturnsConstantAndHasArguments(arguments))
        {
            node.column = std::move(col);
            node.allow_constant_folding = false;
        }
    }

    if (result_name.empty())
    {
        result_name = function->getName() + "(";
        for (size_t i = 0; i < argument_names.size(); ++i)
        {
            if (i)
                result_name += ", ";
            result_name += argument_names[i];
        }
        result_name += ")";
    }

    node.result_name = std::move(result_name);

    return addNode(std::move(node));
}

NamesAndTypesList ActionsDAG::getRequiredColumns() const
{
    NamesAndTypesList result;
    for (const auto & node : nodes)
        if (node.type == ActionType::INPUT)
            result.emplace_back(node.result_name, node.result_type);

    return result;
}

ColumnsWithTypeAndName ActionsDAG::getResultColumns() const
{
    ColumnsWithTypeAndName result;
    result.reserve(index.size());
    for (const auto & node : index)
        result.emplace_back(node->column, node->result_type, node->result_name);

    return result;
}

NamesAndTypesList ActionsDAG::getNamesAndTypesList() const
{
    NamesAndTypesList result;
    for (const auto & node : index)
        result.emplace_back(node->result_name, node->result_type);

    return result;
}

Names ActionsDAG::getNames() const
{
    Names names;
    names.reserve(index.size());
    for (const auto & node : index)
        names.emplace_back(node->result_name);

    return names;
}

std::string ActionsDAG::dumpNames() const
{
    WriteBufferFromOwnString out;
    for (auto it = nodes.begin(); it != nodes.end(); ++it)
    {
        if (it != nodes.begin())
            out << ", ";
        out << it->result_name;
    }
    return out.str();
}

void ActionsDAG::removeUnusedActions(const Names & required_names)
{
    std::unordered_set<Node *> nodes_set;
    std::vector<Node *> required_nodes;
    required_nodes.reserve(required_names.size());

    for (const auto & name : required_names)
    {
        auto it = index.find(name);
        if (it == index.end())
            throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER,
                            "Unknown column: {}, there are only columns {}", name, dumpNames());

        if (nodes_set.insert(*it).second)
            required_nodes.push_back(*it);
    }

    removeUnusedActions(required_nodes);
}

void ActionsDAG::removeUnusedActions(const std::vector<Node *> & required_nodes)
{
    {
        Index new_index;

        for (auto * node : required_nodes)
            new_index.insert(node);

        index.swap(new_index);
    }

    removeUnusedActions();
}

void ActionsDAG::removeUnusedActions()
{
    std::unordered_set<const Node *> visited_nodes;
    std::stack<Node *> stack;

    for (auto * node : index)
    {
        visited_nodes.insert(node);
        stack.push(node);
    }

    while (!stack.empty())
    {
        auto * node = stack.top();
        stack.pop();

        if (!node->children.empty() && node->column && isColumnConst(*node->column) && node->allow_constant_folding)
        {
            /// Constant folding.
            node->type = ActionsDAG::ActionType::COLUMN;
            node->children.clear();
        }

        for (auto * child : node->children)
        {
            if (visited_nodes.count(child) == 0)
            {
                stack.push(child);
                visited_nodes.insert(child);
            }
        }
    }

    nodes.remove_if([&](const Node & node) { return visited_nodes.count(&node) == 0; });
}

void ActionsDAG::addAliases(const NamesWithAliases & aliases, std::vector<Node *> & result_nodes)
{
    std::vector<Node *> required_nodes;

    for (const auto & item : aliases)
    {
        auto & child = getNode(item.first);
        required_nodes.push_back(&child);
    }

    result_nodes.reserve(aliases.size());

    for (size_t i = 0; i < aliases.size(); ++i)
    {
        const auto & item = aliases[i];
        auto * child = required_nodes[i];

        if (!item.second.empty() && item.first != item.second)
        {
            Node node;
            node.type = ActionType::ALIAS;
            node.result_type = child->result_type;
            node.result_name = std::move(item.second);
            node.column = child->column;
            node.allow_constant_folding = child->allow_constant_folding;
            node.children.emplace_back(child);

            auto & alias = addNode(std::move(node), true);
            result_nodes.push_back(&alias);
        }
        else
            result_nodes.push_back(child);
    }
}

void ActionsDAG::addAliases(const NamesWithAliases & aliases)
{
    std::vector<Node *> result_nodes;
    addAliases(aliases, result_nodes);
}

void ActionsDAG::project(const NamesWithAliases & projection)
{
    std::vector<Node *> result_nodes;
    addAliases(projection, result_nodes);
    removeUnusedActions(result_nodes);
    projectInput();
    settings.projected_output = true;
}

void ActionsDAG::removeColumn(const std::string & column_name)
{
    auto & node = getNode(column_name);
    index.remove(&node);
}

bool ActionsDAG::tryRestoreColumn(const std::string & column_name)
{
    if (index.contains(column_name))
        return true;

    for (auto it = nodes.rbegin(); it != nodes.rend(); ++it)
    {
        auto & node = *it;
        if (node.result_name == column_name)
        {
            index.replace(&node);
            return true;
        }
    }

    return false;
}

ActionsDAGPtr ActionsDAG::clone() const
{
    auto actions = cloneEmpty();

    std::unordered_map<const Node *, Node *> copy_map;

    for (const auto & node : nodes)
    {
        auto & copy_node = actions->nodes.emplace_back(node);
        copy_map[&node] = &copy_node;
    }

    for (auto & node : actions->nodes)
        for (auto & child : node.children)
            child = copy_map[child];

    for (const auto & node : index)
        actions->index.insert(copy_map[node]);

    return actions;
}

void ActionsDAG::compileExpressions()
{
#if USE_EMBEDDED_COMPILER
    if (settings.compile_expressions)
    {
        compileFunctions();
        removeUnusedActions();
    }
#endif
}

std::string ActionsDAG::dumpDAG() const
{
    std::unordered_map<const Node *, size_t> map;
    for (const auto & node : nodes)
    {
        size_t idx = map.size();
        map[&node] = idx;
    }

    std::stringstream out;
    for (const auto & node : nodes)
    {
        out << map[&node] << " : ";
        switch (node.type)
        {
            case ActionsDAG::ActionType::COLUMN:
                out << "COLUMN ";
                break;

            case ActionsDAG::ActionType::ALIAS:
                out << "ALIAS ";
                break;

            case ActionsDAG::ActionType::FUNCTION:
                out << "FUNCTION ";
                break;

            case ActionsDAG::ActionType::ARRAY_JOIN:
                out << "ARRAY JOIN ";
                break;

            case ActionsDAG::ActionType::INPUT:
                out << "INPUT ";
                break;
        }

        out << "(";
        for (size_t i = 0; i < node.children.size(); ++i)
        {
            if (i)
                out << ", ";
            out << map[node.children[i]];
        }
        out << ")";

        out << " " << (node.column ? node.column->getName() : "(no column)");
        out << " " << (node.result_type ? node.result_type->getName() : "(no type)");
        out << " " << (!node.result_name.empty() ? node.result_name : "(no name)");
        if (node.function_base)
            out << " [" << node.function_base->getName() << "]";

        out << "\n";
    }

    return out.str();
}

}
