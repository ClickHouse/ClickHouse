#include <GPU/GPUFilterCompiler.h>

#if USE_GPU

#include <GPU/GPUTypeMapping.h>

#include <Columns/ColumnConst.h>
#include <Columns/IColumn.h>
#include <Core/AccurateComparison.h>
#include <Core/Field.h>
#include <Functions/IFunction.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/convertFieldToType.h>
#include <DataTypes/IDataType.h>

#include <bit>
#include <utility>

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

std::optional<GPUFilterOp> arithmeticOf(const String & name)
{
    if (name == "plus")
        return GPUFilterOp::Plus;
    if (name == "minus")
        return GPUFilterOp::Minus;
    if (name == "multiply")
        return GPUFilterOp::Multiply;
    if (name == "divide")
        return GPUFilterOp::Divide;
    return {};
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

struct Register
{
    GPUFilterValueKind kind = GPUFilterValueKind::Unsigned;
    std::optional<uint32_t> constant;
    /// Of a column.
    DataTypePtr type;
    /// A `String` constant, loaded only once a comparison says what it is compared as.
    std::optional<Field> pending_string;
};

GPUFilterConstant constantOf(GPUFilterValueKind kind, const Field & value)
{
    switch (kind)
    {
        case GPUFilterValueKind::Signed:
            return {GPUFilterValueKind::Signed, static_cast<uint64_t>(value.safeGet<Int64>())};
        case GPUFilterValueKind::Float:
            return {GPUFilterValueKind::Float, std::bit_cast<uint64_t>(value.safeGet<Float64>())};
        case GPUFilterValueKind::Unsigned:
            return {GPUFilterValueKind::Unsigned, value.safeGet<UInt64>()};
    }
    std::unreachable();
}

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
        const auto element_type = filterElementTypeOf(*action.node->result_type);
        if (!element_type)
            return refused("a column of the predicate of a type the device has no element type for");

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

        registers[action.result_position] = {.kind = kindOf(*element_type), .constant = {}, .type = action.node->result_type, .pending_string = {}};
        return emit(GPUFilterOp::LoadColumn, action.result_position, required_index);
    }

    bool translateConstant(const ExpressionActions::Action & action)
    {
        const ActionsDAG::Node & node = *action.node;
        if (!node.column || !isColumnConst(*node.column))
            return refused("a column node that is not a constant");

        const Field value = (*node.column)[0];

        /// A date written as a string, `date '1994-01-01'`, which a comparison with a column of dates converts.
        if (isString(node.result_type) && value.getType() == Field::Types::String)
        {
            registers[action.result_position] = {.kind = GPUFilterValueKind::Unsigned, .constant = {}, .type = {}, .pending_string = value};
            return true;
        }

        const auto element_type = filterElementTypeOf(*node.result_type);
        if (!element_type)
            return refused("a constant of the predicate of a type the device has no element type for");

        const GPUFilterConstant constant = constantOf(kindOf(*element_type), value);
        const auto index = addConstant(constant);
        if (!index)
            return false;

        registers[action.result_position] = {.kind = constant.kind, .constant = *index, .type = {}, .pending_string = {}};
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

            if (!loadPendingString(arguments[0].pos, arguments[1].pos) || !loadPendingString(arguments[1].pos, arguments[0].pos))
                return false;

            const auto left = reconciled(arguments[0].pos, arguments[1].pos);
            if (!left)
                return false;
            const auto right = reconciled(arguments[1].pos, arguments[0].pos);
            if (!right)
                return false;

            registers[action.result_position] = {};
            return emit(*comparison, action.result_position, *left, *right);
        }

        for (const auto & argument : arguments)
        {
            if (registers[argument.pos].pending_string)
                return refused("a string constant that is not compared with a column of dates");
        }

        if (const auto arithmetic = arithmeticOf(name))
        {
            /// In doubles on the device, which is what ClickHouse computes a `Float64` in as well, an operation at a time.
            if (arguments.size() != 2)
                return refused("an arithmetic function of other than two arguments");
            if (filterElementTypeOf(*node.result_type) != GPUElementType::Float64)
                return refused("an arithmetic function of a result other than a `Float64`");

            registers[action.result_position] = {.kind = GPUFilterValueKind::Float, .constant = {}, .type = {}, .pending_string = {}};
            return emit(*arithmetic, action.result_position, arguments[0].pos, arguments[1].pos);
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

    /// A string constant compared with a column of dates is the date it reads as, as ClickHouse compares them, and is
    /// loaded as one.
    bool loadPendingString(size_t own, size_t other)
    {
        Register & mine = registers[own];
        if (!mine.pending_string)
            return true;

        const DataTypePtr & type = registers[other].type;
        if (!type || !(isDateOrDate32(type) || isDateTime(type)))
            return refused("a string constant that is not compared with a column of dates");

        const Field converted = tryConvertFieldToType(*mine.pending_string, *type);
        const auto element_type = filterElementTypeOf(*type);
        if (converted.isNull() || !element_type)
            return refused("a string constant that is not a date of the column it is compared with");

        const GPUFilterConstant constant = constantOf(kindOf(*element_type), converted);
        const auto index = addConstant(constant);
        if (!index)
            return false;

        mine = {.kind = constant.kind, .constant = *index, .type = {}, .pending_string = {}};
        return emit(GPUFilterOp::LoadConstant, own, *index);
    }

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
        registers[spare] = {.kind = GPUFilterValueKind::Float, .constant = *index, .type = {}, .pending_string = {}};
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

std::optional<CompiledGPUFilter> compileGPUExpression(const ActionsDAG & dag, const String & output_name, String & refusal)
{
    const ActionsDAG::Node * root = dag.tryFindInOutputs(output_name);
    if (!root)
    {
        refusal = "an expression that is not among its step's outputs";
        return {};
    }

    if (root->type != ActionsDAG::ActionType::FUNCTION || filterElementTypeOf(*root->result_type) != GPUElementType::Float64)
    {
        refusal = "an expression that is not a function into a `Float64`";
        return {};
    }

    const ExpressionActions actions(ActionsDAG::cloneSubDAG({root}, true));
    for (const auto & column : actions.getRequiredColumnsWithTypes())
    {
        if (filterElementTypeOf(*column.type) != GPUElementType::Float64)
        {
            refusal = "an expression over a column other than a `Float64`";
            return {};
        }
    }
    return Compiler(refusal).compile(actions);
}

std::optional<CompiledGPUFilter> compileGPUFilter(const ActionsDAG & dag, const String & filter_column_name, String & refusal)
{
    const ActionsDAG::Node * root = dag.tryFindInOutputs(filter_column_name);
    if (!root)
    {
        refusal = "a predicate that is not among its expression's outputs";
        return {};
    }

    const ExpressionActions actions(ActionsDAG::cloneSubDAG({root},  true));
    return Compiler(refusal).compile(actions);
}

}

#endif
