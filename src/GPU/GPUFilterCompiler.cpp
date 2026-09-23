#include <GPU/GPUFilterCompiler.h>

#if USE_GPU

#include <GPU/GPUTypeMapping.h>

#include <Columns/ColumnConst.h>
#include <Columns/IColumn.h>
#include <Core/AccurateComparison.h>
#include <Core/Field.h>
#include <Functions/IFunction.h>
#include <Interpreters/ExpressionActions.h>

#include <bit>

namespace DB::GPU
{

namespace
{

GPUFilterValueKind kindOf(GPUElementType element_type)
{
    switch (element_type)
    {
        case GPUElementType::Int8:
        case GPUElementType::Int16:
        case GPUElementType::Int32:
        case GPUElementType::Int64:
            return GPUFilterValueKind::Signed;
        case GPUElementType::Float32:
        case GPUElementType::Float64:
            return GPUFilterValueKind::Float;
        default:
            return GPUFilterValueKind::Unsigned;
    }
}

std::optional<GPUFilterOp> comparisonOf(const String & name)
{
    if (name == "equals")
        return GPUFilterOp::Equals;
    if (name == "notEquals")
        return GPUFilterOp::NotEquals;
    if (name == "less")
        return GPUFilterOp::Less;
    if (name == "lessOrEquals")
        return GPUFilterOp::LessOrEquals;
    if (name == "greater")
        return GPUFilterOp::Greater;
    if (name == "greaterOrEquals")
        return GPUFilterOp::GreaterOrEquals;
    return {};
}

/// What a register holds after the action that filled it: the kind of its value, and the
/// constant it was loaded with, when it was one.
struct Register
{
    GPUFilterValueKind kind = GPUFilterValueKind::Unsigned;
    std::optional<uint32_t> constant;
};

class Compiler
{
public:
    explicit Compiler(String & refusal_) : refusal(refusal_) { }

    std::optional<CompiledGPUFilter> compile(const ExpressionActions & actions)
    {
        for (const auto & action : actions.getActions())
        {
            if (action.result_position >= max_filter_registers)
                return refuse("a predicate over more registers than the device has");
            if (!translate(action, actions))
                return {};
            registers_used = std::max<uint32_t>(registers_used, static_cast<uint32_t>(action.result_position) + 1);
        }

        const ColumnNumbers & results = actions.getResultPositions();
        if (results.size() != 1)
            return refuse("a predicate of other than one result");

        result.program.result = static_cast<uint32_t>(results.front());
        result.program.num_registers = registers_used;
        result.program.num_columns = static_cast<uint32_t>(result.columns.size());
        return std::move(result);
    }

private:
    std::optional<CompiledGPUFilter> refuse(const String & reason)
    {
        refusal = reason;
        return {};
    }

    bool refused(const String & reason)
    {
        refusal = reason;
        return false;
    }

    bool emit(GPUFilterOp op, size_t to, size_t first, size_t second = 0)
    {
        if (result.program.length >= max_filter_instructions)
            return refused("a predicate of more instructions than the device takes");

        result.program.code[result.program.length++]
            = {op, static_cast<uint32_t>(to), static_cast<uint32_t>(first), static_cast<uint32_t>(second)};
        return true;
    }

    bool translate(const ExpressionActions::Action & action, const ExpressionActions & actions)
    {
        switch (action.node->type)
        {
            case ActionsDAG::ActionType::INPUT:
                return translateInput(action, actions);
            case ActionsDAG::ActionType::COLUMN:
                return translateConstant(action);
            case ActionsDAG::ActionType::ALIAS:
                if (action.arguments.size() != 1)
                    return refused("an alias of other than one expression");
                registers[action.result_position] = registers[action.arguments.front().pos];
                return emit(GPUFilterOp::Move, action.result_position, action.arguments.front().pos);
            case ActionsDAG::ActionType::FUNCTION:
                return translateFunction(action);
            case ActionsDAG::ActionType::ARRAY_JOIN:
            case ActionsDAG::ActionType::PLACEHOLDER:
                return refused("an expression that is not a comparison, a logical function, a column or a constant");
        }
    }

    bool translateInput(const ExpressionActions::Action & action, const ExpressionActions & actions)
    {
        const auto element_type = elementTypeOf(*action.node->result_type);
        if (!element_type)
            return refused("a column of the predicate of a type the device has no element type for");

        /// The action's one argument numbers the column among the actions' required columns.
        const size_t required_index = action.arguments.front().pos;
        const NamesAndTypesList & required = actions.getRequiredColumnsWithTypes();
        if (required_index >= required.size())
            return refused("a column the predicate's actions do not require");

        while (result.columns.size() <= required_index)
        {
            if (result.columns.size() >= max_filter_columns)
                return refused("a predicate over more columns than the device takes");
            result.columns.push_back(*std::next(required.begin(), result.columns.size()));
        }

        registers[action.result_position] = {.kind = kindOf(*element_type), .constant = {}};
        return emit(GPUFilterOp::LoadColumn, action.result_position, required_index);
    }

    bool translateConstant(const ExpressionActions::Action & action)
    {
        const ActionsDAG::Node & node = *action.node;
        if (!node.column || !isColumnConst(*node.column))
            return refused("a column node that is not a constant");

        const auto element_type = elementTypeOf(*node.result_type);
        if (!element_type)
            return refused("a constant of the predicate of a type the device has no element type for");

        const Field value = (*node.column)[0];
        GPUFilterConstant constant;
        switch (kindOf(*element_type))
        {
            case GPUFilterValueKind::Signed:
                constant = {GPUFilterValueKind::Signed, static_cast<uint64_t>(value.safeGet<Int64>())};
                break;
            case GPUFilterValueKind::Float:
                constant = {GPUFilterValueKind::Float, std::bit_cast<uint64_t>(value.safeGet<Float64>())};
                break;
            case GPUFilterValueKind::Unsigned:
                constant = {GPUFilterValueKind::Unsigned, value.safeGet<UInt64>()};
                break;
        }

        const auto index = addConstant(constant);
        if (!index)
            return false;

        registers[action.result_position] = {.kind = constant.kind, .constant = *index};
        return emit(GPUFilterOp::LoadConstant, action.result_position, *index);
    }

    std::optional<uint32_t> addConstant(const GPUFilterConstant & constant)
    {
        if (result.program.num_constants >= max_filter_constants)
        {
            refused("a predicate of more constants than the device takes");
            return {};
        }

        const uint32_t index = result.program.num_constants++;
        result.program.constants[index] = constant;
        return index;
    }

    bool translateFunction(const ExpressionActions::Action & action)
    {
        const ActionsDAG::Node & node = *action.node;
        if (!node.function_base)
            return refused("a function node without a function");

        const String name = node.function_base->getName();
        const auto & arguments = action.arguments;

        if (const auto comparison = comparisonOf(name))
        {
            if (arguments.size() != 2)
                return refused("a comparison of other than two arguments");

            const auto left = reconciled(arguments[0].pos, arguments[1].pos);
            if (!left)
                return false;
            const auto right = reconciled(arguments[1].pos, arguments[0].pos);
            if (!right)
                return false;

            registers[action.result_position] = {};
            return emit(*comparison, action.result_position, *left, *right);
        }

        if (name == "and" || name == "or")
        {
            if (arguments.size() < 2)
                return refused("a logical function of fewer than two arguments");

            const GPUFilterOp op = name == "and" ? GPUFilterOp::And : GPUFilterOp::Or;
            registers[action.result_position] = {};
            if (!emit(op, action.result_position, arguments[0].pos, arguments[1].pos))
                return false;
            for (size_t i = 2; i < arguments.size(); ++i)
            {
                if (!emit(op, action.result_position, action.result_position, arguments[i].pos))
                    return false;
            }
            return true;
        }

        if (name == "not")
        {
            if (arguments.size() != 1)
                return refused("`not` of other than one argument");
            registers[action.result_position] = {};
            return emit(GPUFilterOp::Not, action.result_position, arguments.front().pos);
        }

        return refused("a function the device does not evaluate: `" + name + "`");
    }

    /// The register to compare in place of `own` against `other`: `own` itself when both hold
    /// integers or both floats, or when `own` holds the float; else, when `own` holds an integer
    /// constant a double holds exactly, a spare register loaded with that double. An integer
    /// column against a float is refused.
    std::optional<size_t> reconciled(size_t own, size_t other)
    {
        const Register & mine = registers[own];
        const bool own_float = mine.kind == GPUFilterValueKind::Float;
        const bool other_float = registers[other].kind == GPUFilterValueKind::Float;
        if (own_float == other_float || own_float)
            return own;

        if (!mine.constant)
        {
            refused("a comparison of an integer column with a float, which the device does not compare exactly");
            return {};
        }

        const GPUFilterConstant & constant = result.program.constants[*mine.constant];
        Float64 as_double = 0;
        const bool exact = constant.kind == GPUFilterValueKind::Signed
            ? accurate::convertNumeric<Int64, Float64>(static_cast<Int64>(constant.bits), as_double)
            : accurate::convertNumeric<UInt64, Float64>(constant.bits, as_double);
        if (!exact)
        {
            refused("an integer constant a double does not hold exactly, compared with a float");
            return {};
        }

        const auto index = addConstant({GPUFilterValueKind::Float, std::bit_cast<uint64_t>(as_double)});
        if (!index)
            return {};

        if (registers_used >= max_filter_registers)
        {
            refused("a predicate over more registers than the device has");
            return {};
        }
        const size_t spare = registers_used++;
        registers[spare] = {.kind = GPUFilterValueKind::Float, .constant = *index};
        if (!emit(GPUFilterOp::LoadConstant, spare, *index))
            return {};
        return spare;
    }

    String & refusal;
    CompiledGPUFilter result;
    Register registers[max_filter_registers];
    uint32_t registers_used = 0;
};

}

std::optional<CompiledGPUFilter> compileGPUFilter(const ActionsDAG & dag, const String & filter_column_name, String & refusal)
{
    const ActionsDAG::Node * root = dag.tryFindInOutputs(filter_column_name);
    if (!root)
    {
        refusal = "a predicate that is not among its expression's outputs";
        return {};
    }

    /// Only the predicate's own actions, laid out as the CPU would run them: without aliases, and
    /// without short-circuiting, which the device does not do.
    const ExpressionActions actions(ActionsDAG::cloneSubDAG({root}, /*remove_aliases=*/ true));
    return Compiler(refusal).compile(actions);
}

}

#endif
