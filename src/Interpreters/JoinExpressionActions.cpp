#include <Interpreters/JoinExpressionActions.h>
#include <stack>
#include <Core/Block.h>
#include <boost/noncopyable.hpp>
#include <Functions/isNotDistinctFrom.h>


#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsLogical.h>
#include <Functions/FunctionsComparison.h>

#include <Interpreters/ActionsDAG.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_IDENTIFIER;
}

std::string_view toString(JoinConditionOperator op)
{
    switch (op)
    {
        case JoinConditionOperator::Equals: return "=";
        case JoinConditionOperator::NullSafeEquals: return "<=>";
        case JoinConditionOperator::Less: return "<";
        case JoinConditionOperator::LessOrEquals: return "<=";
        case JoinConditionOperator::Greater: return ">";
        case JoinConditionOperator::GreaterOrEquals: return ">=";
        case JoinConditionOperator::And: return "AND";
        case JoinConditionOperator::Or: return "OR";
        case JoinConditionOperator::Unknown: break;
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Illegal value for JoinConditionOperator: {}", static_cast<Int32>(op));
}

struct JoinExpressionActions::Data : boost::noncopyable
{
    using NodeToSourceMapping = std::unordered_map<NodeRawPtr, BitSet>;

    Data(ActionsDAG && actions_dag_, NodeToSourceMapping && expression_sources_)
        : actions_dag(std::move(actions_dag_)), expression_sources(std::move(expression_sources_))
    {
        lhs_rels.set(0);
        rhs_rels.set(1);
    }

    BitSet lhs_rels;
    BitSet rhs_rels;
    ActionsDAG actions_dag;
    NodeToSourceMapping expression_sources;
};

JoinExpressionActions::JoinExpressionActions(const Block & left_header, const Block & right_header)
{
    Data::NodeToSourceMapping expression_sources;
    ActionsDAG actions_dag;

    for (const auto & column : left_header)
    {
        const auto * node = &actions_dag.addInput(column.name, column.type);
        expression_sources[node].set(0);
    }

    for (const auto & column : right_header)
    {
        const auto * node = &actions_dag.addInput(column.name, column.type);
        expression_sources[node].set(1);
    }

    data = std::make_shared<Data>(std::move(actions_dag), std::move(expression_sources));
}

JoinExpressionActions::JoinExpressionActions(const Block & left_header, const Block & right_header, ActionsDAG && actions_dag_)
{
    Data::NodeToSourceMapping expression_sources;

    const auto & input_nodes = actions_dag_.getInputs();
    if (input_nodes.size() != left_header.columns() + right_header.columns())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Input nodes size mismatch in dag: {}, expected: [{}], [{}]",
                        actions_dag_.dumpDAG(), left_header.dumpNames(), right_header.dumpNames());

    for (size_t i = 0; i < input_nodes.size(); ++i)
    {
        BitSet rels;
        if (input_nodes[i]->type != ActionsDAG::ActionType::INPUT)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Input node {} is not INPUT in dag: {}",
                            i, actions_dag_.dumpDAG());
        rels.set(i < left_header.columns() ? 0 : 1);
        expression_sources[input_nodes[i]] = rels;
    }

    data = std::make_shared<Data>(std::move(actions_dag_), std::move(expression_sources));
}


using NodeRawPtr = JoinExpressionActions::NodeRawPtr;

BitSet getExpressionSourcesImpl(std::unordered_map<NodeRawPtr, BitSet> & expression_sources, const JoinActionRef & action)
{
    const auto * node = action.getNode();
    if (auto it = expression_sources.find(node); it != expression_sources.end())
        return it->second;

    std::stack<std::pair<NodeRawPtr, size_t>> stack;
    stack.push({node, 0});

    while (!stack.empty())
    {
        auto & [current, child_idx] = stack.top();

        if (expression_sources.contains(current))
        {
            stack.pop();
            continue;
        }

        if (current->type == ActionsDAG::ActionType::INPUT)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Unknown input node {} in expression sources", current->result_name);

        if (child_idx >= current->children.size())
        {
            BitSet sources;
            for (const auto & child : current->children)
                sources = sources | expression_sources.at(child);

            expression_sources[current] = sources;
            stack.pop();
            continue;
        }

        const auto * child = current->children[child_idx];
        child_idx++;

        if (!expression_sources.contains(child))
            stack.push({child, 0});
    }
    return expression_sources.at(node);
}

std::shared_ptr<ActionsDAG> JoinExpressionActions::getActionsDAG() const
{
    if (!data)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot get actions DAG for JoinExpressionActions");
    return std::shared_ptr<ActionsDAG>(data, &data->actions_dag);
}

JoinActionRef::JoinActionRef(NodeRawPtr node_, std::shared_ptr<JoinExpressionActions::Data> data_)
    : node_ptr(node_)
    , data(data_)
{
    if (node_ptr)
    {
        if (!data_)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot create JoinActionRef nullptr data");
        auto raw_nodes = data_->actions_dag.getNodes() | std::views::transform([](const auto & node) { return &node; });
        if (!std::ranges::contains(raw_nodes, node_ptr))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot create JoinActionRef for node {} not in actions DAG", node_ptr->result_name);
    }
}

JoinActionRef::JoinActionRef(NodeRawPtr node_, JoinExpressionActions & expression_actions_)
    : JoinActionRef(node_, expression_actions_.data)
{
}

const ActionsDAG::Node * JoinActionRef::getNode() const
{
    if (!node_ptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot get node for nullptr");
    if (data.expired())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Pointer to actions DAG is expired");
    return node_ptr;
}

ColumnWithTypeAndName JoinActionRef::getColumn() const
{
    const auto * node = getNode();
    return {node->column, node->result_type, node->result_name};
}

const String & JoinActionRef::getColumnName() const
{
    return getNode()->result_name;
}

DataTypePtr JoinActionRef::getType() const
{
    return getNode()->result_type;
}

JoinActionRef JoinExpressionActions::findNode(const String & column_name, bool is_input, bool throw_if_not_found) const
{
    const auto & nodes = is_input ? data->actions_dag.getInputs() : data->actions_dag.getOutputs();
    for (const auto & node : nodes)
        if (node->result_name == column_name)
            return JoinActionRef(node, data);
    if (throw_if_not_found)
        throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER, "Cannot find column {} in actions DAG {}:\n{}",
            column_name, is_input ? "input" : "output", data->actions_dag.dumpDAG());
    return JoinActionRef(nullptr);
}

ActionsDAG JoinExpressionActions::getSubDAG(JoinActionRef action)
{
    return getSubDAG(std::views::single(action));
}

JoinExpressionActions JoinExpressionActions::clone(std::vector<JoinActionRef> & nodes) const
{
    ActionsDAG::NodePtrMap node_map;
    auto actions_dag = getActionsDAG()->clone(node_map);
    JoinExpressionActions::Data::NodeToSourceMapping new_expression_sources;
    for (const auto & [node, source] : data->expression_sources)
    {
        auto it = node_map.find(node);
        if (it == node_map.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot find node {} in node map", node->result_name);
        new_expression_sources[it->second] = source;
    }

    auto result_data = std::make_shared<Data>(std::move(actions_dag), std::move(new_expression_sources));
    for (auto & node : nodes)
    {
        auto it = node_map.find(node.getNode());
        if (it == node_map.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot find node in node map");
        node = JoinActionRef(it->second, result_data);
    }
    return JoinExpressionActions(std::move(result_data));
}

BitSet JoinActionRef::getSourceRelations() const
{
    return getExpressionSourcesImpl(getData()->expression_sources, *this);
}

void JoinActionRef::setSourceRelations(const BitSet & source_relations) const
{
    auto data_ptr = getData();

    const auto * node = getNode();
    std::stack<const ActionsDAG::Node *> stack;
    stack.push(node);

    auto & expression_sources = data_ptr->expression_sources;
    while (!stack.empty())
    {
        const auto * current = stack.top();
        stack.pop();
        if (expression_sources[current] == source_relations)
            break;
        expression_sources[current] = source_relations;
        for (const auto * child : current->children)
            stack.push(child);
    }
}

std::string operatorToFunctionName(JoinConditionOperator op)
{
    switch (op)
    {
        case JoinConditionOperator::And: return FunctionAnd::name;
        case JoinConditionOperator::Or: return FunctionOr::name;
        case JoinConditionOperator::NullSafeEquals: return FunctionIsNotDistinctFrom::name;
        case JoinConditionOperator::Equals: return NameEquals::name;
        case JoinConditionOperator::Less: return NameLess::name;
        case JoinConditionOperator::LessOrEquals: return NameLessOrEquals::name;
        case JoinConditionOperator::Greater: return NameGreater::name;
        case JoinConditionOperator::GreaterOrEquals: return NameGreaterOrEquals::name;
        case JoinConditionOperator::Unknown: break;
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Illegal value for JoinConditionOperator: {}", static_cast<Int32>(op));
}

JoinConditionOperator functionNameToOperator(std::string_view name)
{
    using UnderlyingType = std::underlying_type_t<JoinConditionOperator>;
    for (UnderlyingType i = 0; i < static_cast<UnderlyingType>(JoinConditionOperator::Unknown); ++i)
    {
        if (operatorToFunctionName(static_cast<JoinConditionOperator>(i)) == name)
            return static_cast<JoinConditionOperator>(i);
    }
    return JoinConditionOperator::Unknown;
}

bool JoinActionRef::isFunction(JoinConditionOperator op) const
{
    const auto * node = getNode();
    if (node->type != ActionsDAG::ActionType::FUNCTION)
        return false;
    const auto & function_name = node->function ? node->function->getName() : "";
    return function_name == operatorToFunctionName(op);
}

bool JoinActionRef::fromLeft() const
{
    auto data_ptr = getData();
    auto src_rels = getExpressionSourcesImpl(data_ptr->expression_sources, *this);
    return src_rels.any() && isSubsetOf(src_rels, data_ptr->lhs_rels);
}

bool JoinActionRef::fromRight() const
{
    auto data_ptr = getData();
    auto src_rels = getExpressionSourcesImpl(data_ptr->expression_sources, *this);
    return src_rels.any() && isSubsetOf(src_rels, data_ptr->rhs_rels);
}

bool JoinActionRef::fromNone() const
{
    return getSourceRelations().none();
}

std::tuple<JoinConditionOperator, JoinActionRef, JoinActionRef> JoinActionRef::asBinaryPredicate() const
{
    auto data_ptr = getData();
    const auto * node = getNode();
    if (node->type != ActionsDAG::ActionType::FUNCTION || node->children.size() != 2)
        return {JoinConditionOperator::Unknown, nullptr, nullptr};

    const auto & function_name = node->function ? node->function->getName() : "";
    auto op = functionNameToOperator(function_name);
    if (op == JoinConditionOperator::Unknown)
        return {JoinConditionOperator::Unknown, nullptr, nullptr};

    JoinActionRef lhs = JoinActionRef(node->children[0], data_ptr);
    JoinActionRef rhs = JoinActionRef(node->children[1], data_ptr);
    return {op, lhs, rhs};
}

std::vector<JoinActionRef> JoinActionRef::getArguments(bool recursive) const
{
    UNUSED(recursive);
    const auto * node = getNode();
    std::vector<JoinActionRef> arguments;
    auto data_ptr = getData();
    for (const auto & child : node->children)
        arguments.emplace_back(child, data_ptr);
    return arguments;
}

std::shared_ptr<JoinExpressionActions::Data> JoinActionRef::getData() const
{
    auto data_ptr = data.lock();
    if (!data_ptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot get data for JoinActionRef");
    return data_ptr;
}

std::shared_ptr<JoinExpressionActions::Data> JoinActionRef::getData(const std::vector<JoinActionRef> & actions)
{
    if (actions.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot get actions DAG for empty actions");

    auto data_ptr = actions.front().getData();
    for (const auto & action : actions)
    {
        if (data_ptr.get() != action.getData().get())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "All actions must have the same actions DAG");
    }
    return data_ptr;
}

ActionsDAG & JoinActionRef::getActionsDAG(JoinExpressionActions::Data & data_)
{
    return data_.actions_dag;
}

static FunctionOverloadResolverPtr operatorToFunction(JoinConditionOperator op)
{
    switch (op)
    {
        case JoinConditionOperator::And:
            return std::make_shared<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionAnd>());
        case JoinConditionOperator::Or:
            return std::make_shared<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionOr>());
        case JoinConditionOperator::NullSafeEquals:
            return std::make_shared<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionIsNotDistinctFrom>());
        default:
            auto function_name = operatorToFunctionName(op);
            return FunctionFactory::instance().get(function_name, nullptr);
    }
}

JoinActionRef::AddFunction::AddFunction(JoinConditionOperator op) : AddFunction(operatorToFunction(op)) {}
JoinActionRef::AddFunction::AddFunction(std::shared_ptr<IFunction> function_) : AddFunction(std::make_shared<FunctionToOverloadResolverAdaptor>(function_)) {}
JoinActionRef::AddFunction::AddFunction(FunctionOverloadResolverPtr function_ptr_) : function_ptr(function_ptr_) {}

NodeRawPtr JoinActionRef::AddFunction::operator()(ActionsDAG & dag, std::vector<NodeRawPtr> nodes)
{
    return &dag.addFunction(function_ptr, std::move(nodes), {});
}

}
