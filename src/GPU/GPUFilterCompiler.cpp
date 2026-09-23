#include <GPU/GPUFilterCompiler.h>

#if USE_GPU

#include <GPU/GPUTypeMapping.h>

#include <Columns/ColumnConst.h>
#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <Functions/IFunction.h>

#include <bit>
#include <cmath>
#include <unordered_map>

namespace DB::GPU
{

namespace
{

/// What an expression leaves on the stack: a boolean of a comparison or a logical function, or a
/// value of a column or a constant.
enum class StackKind
{
    Boolean,
    Signed,
    Unsigned,
    Float,
};

StackKind stackKindOf(GPUElementType element_type)
{
    switch (element_type)
    {
        case GPUElementType::Int8:
        case GPUElementType::Int16:
        case GPUElementType::Int32:
        case GPUElementType::Int64:
            return StackKind::Signed;
        case GPUElementType::Float32:
        case GPUElementType::Float64:
            return StackKind::Float;
        default:
            return StackKind::Unsigned;
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

class Compiler
{
public:
    explicit Compiler(String & refusal_) : refusal(refusal_) { }

    std::optional<CompiledGPUFilter> compile(const ActionsDAG::Node & root)
    {
        const auto kind = compileNode(root);
        if (!kind)
            return {};

        if (*kind != StackKind::Boolean && !emit(GPUFilterOp::IsTrue, 0))
            return {};

        result.program.num_columns = static_cast<uint32_t>(result.columns.size());
        return std::move(result);
    }

private:
    bool refuse(const String & reason)
    {
        refusal = reason;
        return false;
    }

    bool emit(GPUFilterOp op, uint32_t operand)
    {
        if (result.program.length >= max_filter_instructions)
            return refuse("a predicate of more instructions than the device takes");

        result.program.code[result.program.length++] = {op, operand};
        return true;
    }

    /// The instruction just emitted, when it is a `PushConstant`.
    GPUFilterConstant * lastPushedConstant()
    {
        if (result.program.length == 0)
            return nullptr;

        const GPUFilterInstruction & last = result.program.code[result.program.length - 1];
        if (last.op != GPUFilterOp::PushConstant)
            return nullptr;

        return &result.program.constants[last.operand];
    }

    bool push()
    {
        ++depth;
        if (depth > max_filter_stack)
            return refuse("a predicate nested deeper than the device's stack");
        return true;
    }

    /// Compiles the expression and answers what it leaves on the stack.
    std::optional<StackKind> compileNode(const ActionsDAG::Node & node)
    {
        switch (node.type)
        {
            case ActionsDAG::ActionType::INPUT:
                return compileInput(node);
            case ActionsDAG::ActionType::COLUMN:
                return compileConstant(node);
            case ActionsDAG::ActionType::ALIAS:
                if (node.children.size() != 1)
                {
                    refuse("an alias of other than one expression");
                    return {};
                }
                return compileNode(*node.children.front());
            case ActionsDAG::ActionType::FUNCTION:
                return compileFunction(node);
            case ActionsDAG::ActionType::ARRAY_JOIN:
            case ActionsDAG::ActionType::PLACEHOLDER:
                refuse("an expression that is not a comparison, a logical function, a column or a constant");
                return {};
        }
    }

    std::optional<StackKind> compileInput(const ActionsDAG::Node & node)
    {
        const auto element_type = elementTypeOf(*node.result_type);
        if (!element_type)
        {
            refuse("a column of the predicate of a type the device has no element type for");
            return {};
        }

        auto [position, inserted] = column_positions.try_emplace(node.result_name, result.columns.size());
        if (inserted)
        {
            if (result.columns.size() >= max_filter_columns)
            {
                refuse("a predicate over more columns than the device takes");
                return {};
            }
            result.columns.emplace_back(node.result_name, node.result_type);
        }

        if (!emit(GPUFilterOp::PushColumn, static_cast<uint32_t>(position->second)) || !push())
            return {};
        return stackKindOf(*element_type);
    }

    std::optional<StackKind> compileConstant(const ActionsDAG::Node & node)
    {
        if (!node.column || !isColumnConst(*node.column))
        {
            refuse("a column node that is not a constant");
            return {};
        }

        const auto element_type = elementTypeOf(*node.result_type);
        if (!element_type)
        {
            refuse("a constant of the predicate of a type the device has no element type for");
            return {};
        }

        if (result.program.num_constants >= max_filter_constants)
        {
            refuse("a predicate of more constants than the device takes");
            return {};
        }

        const Field value = (*node.column)[0];
        GPUFilterConstant constant;
        switch (stackKindOf(*element_type))
        {
            case StackKind::Signed:
                constant = {GPUFilterValueKind::Signed, static_cast<uint64_t>(value.safeGet<Int64>())};
                break;
            case StackKind::Float:
                constant = {GPUFilterValueKind::Float, std::bit_cast<uint64_t>(value.safeGet<Float64>())};
                break;
            default:
                constant = {GPUFilterValueKind::Unsigned, value.safeGet<UInt64>()};
                break;
        }

        const uint32_t index = result.program.num_constants++;
        result.program.constants[index] = constant;

        if (!emit(GPUFilterOp::PushConstant, index) || !push())
            return {};
        return stackKindOf(*element_type);
    }

    std::optional<StackKind> compileFunction(const ActionsDAG::Node & node)
    {
        if (!node.function_base)
        {
            refuse("a function node without a function");
            return {};
        }

        const String name = node.function_base->getName();

        if (const auto comparison = comparisonOf(name))
            return compileComparison(node, *comparison);

        if (name == "and" || name == "or")
            return compileLogical(node, name == "and" ? GPUFilterOp::And : GPUFilterOp::Or);

        if (name == "not")
        {
            if (node.children.size() != 1)
            {
                refuse("`not` of other than one argument");
                return {};
            }

            const auto kind = compileNode(*node.children.front());
            if (!kind)
                return {};
            if (*kind != StackKind::Boolean && !emit(GPUFilterOp::IsTrue, 0))
                return {};
            if (!emit(GPUFilterOp::Not, 0))
                return {};
            return StackKind::Boolean;
        }

        refuse("a function the device does not evaluate: `" + name + "`");
        return {};
    }

    /// An integer and a float compare as doubles only when the integer is a constant a double
    /// holds exactly, in which case the constant is rewritten as that double.
    bool constantToFloat(GPUFilterConstant * constant)
    {
        static constexpr uint64_t exact_limit = 1ULL << 53;

        if (!constant)
            return refuse("a comparison of an integer column with a float, which the device does not compare exactly");

        double as_double = 0;
        if (constant->kind == GPUFilterValueKind::Signed)
        {
            const int64_t value = static_cast<int64_t>(constant->bits);
            if (value > static_cast<int64_t>(exact_limit) || value < -static_cast<int64_t>(exact_limit))
                return refuse("an integer constant a double does not hold exactly, compared with a float");
            as_double = static_cast<double>(value);
        }
        else
        {
            if (constant->bits > exact_limit)
                return refuse("an integer constant a double does not hold exactly, compared with a float");
            as_double = static_cast<double>(constant->bits);
        }

        *constant = {GPUFilterValueKind::Float, std::bit_cast<uint64_t>(as_double)};
        return true;
    }

    std::optional<StackKind> compileComparison(const ActionsDAG::Node & node, GPUFilterOp op)
    {
        if (node.children.size() != 2)
        {
            refuse("a comparison of other than two arguments");
            return {};
        }

        const auto left = compileNode(*node.children[0]);
        if (!left)
            return {};
        GPUFilterConstant * left_constant = lastPushedConstant();

        const auto right = compileNode(*node.children[1]);
        if (!right)
            return {};
        GPUFilterConstant * right_constant = lastPushedConstant();

        const bool left_float = *left == StackKind::Float;
        const bool right_float = *right == StackKind::Float;
        if (left_float != right_float && !constantToFloat(left_float ? right_constant : left_constant))
            return {};

        if (!emit(op, 0))
            return {};

        depth -= 1;
        return StackKind::Boolean;
    }

    std::optional<StackKind> compileLogical(const ActionsDAG::Node & node, GPUFilterOp op)
    {
        if (node.children.size() < 2)
        {
            refuse("a logical function of fewer than two arguments");
            return {};
        }

        for (size_t i = 0; i < node.children.size(); ++i)
        {
            const auto kind = compileNode(*node.children[i]);
            if (!kind)
                return {};
            if (*kind != StackKind::Boolean && !emit(GPUFilterOp::IsTrue, 0))
                return {};

            if (i != 0)
            {
                if (!emit(op, 0))
                    return {};
                depth -= 1;
            }
        }

        return StackKind::Boolean;
    }

    String & refusal;
    CompiledGPUFilter result;
    std::unordered_map<String, size_t> column_positions;
    size_t depth = 0;
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

    return Compiler(refusal).compile(*root);
}

}

#endif
