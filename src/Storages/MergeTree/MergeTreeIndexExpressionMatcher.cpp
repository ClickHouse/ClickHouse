#include <Storages/MergeTree/MergeTreeIndexExpressionMatcher.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnFunction.h>
#include <Core/Block.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionsMiscellaneous.h>
#include <Functions/IFunction.h>
#include <Storages/MergeTree/RPNBuilder.h>

#include <fmt/format.h>


namespace DB
{

namespace
{

/// Compares expressions by structure: index expressions come from the old analyzer, so names differ and lambdas may be folded.
class ExpressionMatcher
{
public:
    static bool isSame(const ActionsDAG::Node * query_node, const ActionsDAG::Node * index_node, bool normalize_empty_string_comparisons)
    {
        return ExpressionMatcher(normalize_empty_string_comparisons).match({.node = query_node}, {.node = index_node});
    }

private:
    explicit ExpressionMatcher(bool normalize_empty_string_comparisons_)
        : normalize_empty_string_comparisons(normalize_empty_string_comparisons_)
    {
    }

    const bool normalize_empty_string_comparisons;

    struct Scope;

    /// Either a node of the DAG of `scope`, or a column captured by a folded lambda.
    struct Operand
    {
        const ActionsDAG::Node * node = nullptr;
        const ColumnWithTypeAndName * captured_column = nullptr;
        const Scope * scope = nullptr;
    };

    /// The body of a lambda: its inputs are captured operands or lambda arguments.
    struct Scope
    {
        const LambdaCapture * capture = nullptr;
        std::vector<Operand> captured = {};
        size_t depth = 0;
    };

    struct Lambda
    {
        const LambdaCapture * capture = nullptr;
        const ActionsDAG * body = nullptr;
        std::vector<Operand> captured = {};
    };

    static Operand resolve(Operand operand)
    {
        while (operand.node)
        {
            operand.node = operand.node->getWithoutAlias();
            if (operand.node->type != ActionsDAG::ActionType::INPUT || !operand.scope)
                break;

            const auto & captured_names = operand.scope->capture->captured_names;
            auto it = std::ranges::find(captured_names, operand.node->result_name);
            if (it == captured_names.end())
                break;
            operand = operand.scope->captured[static_cast<size_t>(it - captured_names.begin())];
        }
        return operand;
    }

    static std::optional<ColumnWithTypeAndName> getConstant(const Operand & operand)
    {
        ColumnWithTypeAndName constant;
        if (operand.captured_column)
            constant = *operand.captured_column;
        else if (operand.node->column && isColumnConst(*operand.node->column))
            constant = {operand.node->column, operand.node->result_type, ""};
        else
            return std::nullopt;

        WhichDataType which(constant.type);
        if (which.isFunction() || which.isSet())
            return std::nullopt;

        /// Constants of the index expression have no rows.
        if (const auto * column_const = typeid_cast<const ColumnConst *>(constant.column.get()))
            constant.column = column_const->getDataColumnPtr();
        if (constant.column->empty())
            return std::nullopt;
        return constant;
    }

    static std::optional<Lambda> getLambda(const Operand & operand)
    {
        if (operand.node && operand.node->type == ActionsDAG::ActionType::FUNCTION)
        {
            const auto * function = typeid_cast<const FunctionCapture *>(operand.node->function_base.get());
            if (!function)
                return std::nullopt;

            Lambda lambda{.capture = &function->getCapture(), .body = &function->getAcionsDAG()};
            for (const auto * child : operand.node->children)
                lambda.captured.push_back({.node = child, .scope = operand.scope});
            return lambda;
        }

        ColumnPtr column = operand.captured_column ? operand.captured_column->column : ColumnPtr(operand.node->column);
        if (!column)
            return std::nullopt;
        if (const auto * column_const = typeid_cast<const ColumnConst *>(column.get()))
            column = column_const->getDataColumnPtr();

        const auto * column_function = typeid_cast<const ColumnFunction *>(column.get());
        if (!column_function)
            return std::nullopt;
        const auto * function = typeid_cast<const FunctionExpression *>(column_function->getFunction().get());
        if (!function)
            return std::nullopt;

        Lambda lambda{.capture = &function->getCapture(), .body = &function->getAcionsDAG()};
        for (const auto & captured_column : column_function->getCapturedColumns())
            lambda.captured.push_back({.captured_column = &captured_column});
        return lambda;
    }

    static const ActionsDAG::Node * getLambdaResult(const Lambda & lambda)
    {
        for (const auto * output : lambda.body->getOutputs())
            if (output->result_name == lambda.capture->return_name)
                return output;
        return nullptr;
    }

    static std::optional<size_t> getLambdaArgumentIndex(const Operand & operand)
    {
        size_t index = 0;
        for (const auto & argument : operand.scope->capture->lambda_arguments)
        {
            if (argument.name == operand.node->result_name)
                return index;
            ++index;
        }
        return std::nullopt;
    }

    [[nodiscard]] bool matchLambdas(const Lambda & query, const Operand & query_operand, const Lambda & index, const Operand & index_operand) const
    {
        auto same_type = [](const auto & lhs, const auto & rhs) { return lhs.type->equals(*rhs.type); };
        if (!std::ranges::equal(query.capture->lambda_arguments, index.capture->lambda_arguments, same_type)
            || !query.capture->return_type->equals(*index.capture->return_type))
            return false;

        const auto * query_result = getLambdaResult(query);
        const auto * index_result = getLambdaResult(index);
        if (!query_result || !index_result)
            return false;

        size_t depth = (query_operand.scope ? query_operand.scope->depth : 0) + 1;
        if (depth != (index_operand.scope ? index_operand.scope->depth : 0) + 1)
            return false;

        Scope query_scope{.capture = query.capture, .captured = query.captured, .depth = depth};
        Scope index_scope{.capture = index.capture, .captured = index.captured, .depth = depth};
        return match({.node = query_result, .scope = &query_scope}, {.node = index_result, .scope = &index_scope});
    }

    /// The operand `a` of the index's `a = ''` / `a != ''` when the query has `empty(a)` / `notEmpty(a)` in its place.
    [[nodiscard]] std::optional<Operand> getEmptyStringComparisonOperand(const Operand & query, const Operand & index) const
    {
        if (!normalize_empty_string_comparisons || query.node->children.size() != 1 || index.node->children.size() != 2)
            return std::nullopt;

        const auto & query_function = query.node->function_base->getName();
        const auto & index_function = index.node->function_base->getName();
        if (!(query_function == "empty" && index_function == "equals") && !(query_function == "notEmpty" && index_function == "notEquals"))
            return std::nullopt;

        auto is_empty_string = [&](const ActionsDAG::Node * child)
        {
            auto constant = getConstant(resolve({.node = child, .scope = index.scope}));
            return constant && WhichDataType(constant->type).isString() && (*constant->column)[0] == Field(String());
        };

        bool first_is_empty = is_empty_string(index.node->children[0]);
        if (first_is_empty == is_empty_string(index.node->children[1]))
            return std::nullopt;
        return Operand{.node = index.node->children[first_is_empty ? 1 : 0], .scope = index.scope};
    }

    [[nodiscard]] bool match(Operand query, Operand index) const
    {
        query = resolve(query);
        index = resolve(index);

        auto query_constant = getConstant(query);
        auto index_constant = getConstant(index);
        if (query_constant || index_constant)
            return query_constant && index_constant && query_constant->type->equals(*index_constant->type)
                && (*query_constant->column)[0] == (*index_constant->column)[0];

        auto query_lambda = getLambda(query);
        auto index_lambda = getLambda(index);
        if (query_lambda || index_lambda)
            return query_lambda && index_lambda && matchLambdas(*query_lambda, query, *index_lambda, index);

        if (!query.node || !index.node || query.node->type != index.node->type)
            return false;

        switch (query.node->type)
        {
            case ActionsDAG::ActionType::INPUT:
            {
                if (!query.scope || !index.scope)
                    return !query.scope && !index.scope && query.node->result_name == index.node->result_name;

                auto query_argument = getLambdaArgumentIndex(query);
                return query_argument && query.scope->depth == index.scope->depth && query_argument == getLambdaArgumentIndex(index);
            }
            case ActionsDAG::ActionType::FUNCTION:
                if (auto index_operand = getEmptyStringComparisonOperand(query, index))
                    return match({.node = query.node->children.front(), .scope = query.scope}, *index_operand);
                if (query.node->function_base->getName() != index.node->function_base->getName())
                    return false;
                [[fallthrough]];
            case ActionsDAG::ActionType::ARRAY_JOIN:
            {
                if (query.node->children.size() != index.node->children.size())
                    return false;
                for (size_t i = 0; i < query.node->children.size(); ++i)
                {
                    Operand query_child{.node = query.node->children[i], .scope = query.scope};
                    Operand index_child{.node = index.node->children[i], .scope = index.scope};
                    if (!match(query_child, index_child))
                        return false;
                }
                return true;
            }
            default:
                return false;
        }
    }
};

}

bool isSameIndexExpression(const ActionsDAG::Node * query_node, const ActionsDAG::Node * index_node, bool normalize_empty_string_comparisons)
{
    return ExpressionMatcher::isSame(query_node, index_node, normalize_empty_string_comparisons);
}

std::optional<size_t> findIndexPosition(
    const Block & header,
    const ActionsDAG::NodeRawConstPtrs & index_nodes,
    const RPNBuilderTreeNode & node,
    bool normalize_empty_string_comparisons)
{
    auto position = header.findPositionByName(node.getColumnName());
    if (position && ExpressionMatcher::isSame(node.getDAGNode(), index_nodes[*position], normalize_empty_string_comparisons))
        return position;
    return std::nullopt;
}

Names collectJSONIndexColumns(const Block & header, const ActionsDAG::NodeRawConstPtrs & index_nodes)
{
    Names names(header.columns());
    for (size_t position = 0; position < names.size(); ++position)
    {
        const auto * index_node = index_nodes[position]->getWithoutAlias();
        if (index_node->type != ActionsDAG::ActionType::FUNCTION || index_node->children.size() != 1)
            continue;

        const auto & function_name = index_node->function_base->getName();
        if (function_name != "JSONAllPaths" && function_name != "JSONAllValues")
            continue;

        const auto * argument = index_node->children.front()->getWithoutAlias();
        const auto & name = header.getByPosition(position).name;
        if (argument->type == ActionsDAG::ActionType::INPUT && name == fmt::format("{}({})", function_name, argument->result_name))
            names[position] = name;
    }
    return names;
}

std::optional<size_t> findFunctionIndexPosition(
    const Block & header,
    const ActionsDAG::NodeRawConstPtrs & index_nodes,
    const String & function_name,
    const String & argument_name,
    const ActionsDAG::Node * argument_node,
    bool normalize_empty_string_comparisons)
{
    auto position = header.findPositionByName(fmt::format("{}({})", function_name, argument_name));
    if (!position)
        return std::nullopt;

    const auto * index_node = index_nodes[*position]->getWithoutAlias();
    if (index_node->type != ActionsDAG::ActionType::FUNCTION || !index_node->function_base
        || index_node->function_base->getName() != function_name || index_node->children.size() != 1)
        return std::nullopt;

    const auto * argument = index_node->children.front()->getWithoutAlias();
    bool same_argument = argument_node
        ? ExpressionMatcher::isSame(argument_node, argument, normalize_empty_string_comparisons)
        : argument->type == ActionsDAG::ActionType::INPUT && argument->result_name == argument_name;
    if (!same_argument)
        return std::nullopt;

    return position;
}

std::optional<size_t> findMapIndexPosition(
    const Block & header,
    const ActionsDAG::NodeRawConstPtrs & index_nodes,
    const String & function_name,
    const String & map_column_name,
    const ActionsDAG::Node * map_node,
    bool normalize_empty_string_comparisons)
{
    auto position = findFunctionIndexPosition(header, index_nodes, function_name, map_column_name, map_node, normalize_empty_string_comparisons);
    if (!position || !isMap(index_nodes[*position]->getWithoutAlias()->children.front()->getWithoutAlias()->result_type))
        return std::nullopt;

    return position;
}

}
