#include <Interpreters/JIT/CompileDAG.h>

#if USE_EMBEDDED_COMPILER

#include <optional>

#include <llvm/IR/BasicBlock.h>
#include <llvm/IR/Function.h>
#include <llvm/IR/IRBuilder.h>

#include <Common/SipHash.h>
#include <Common/FieldVisitorToString.h>
#include <Columns/ColumnConst.h>
#include <DataTypes/Native.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/TimezoneMixin.h>
#include <Interpreters/JIT/JITError.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>

namespace DB
{

namespace
{

/// The rows where all terms of `guard` hold, as an `i1`.
llvm::Value * compileGuard(llvm::IRBuilder<> & b, const std::vector<CompileDAG::GuardTerm> & guard, const ValuesWithType & values)
{
    llvm::Value * mask = b.getTrue();
    for (const auto & term : guard)
    {
        const auto & condition = values[term.condition];
        llvm::Value * is_null = b.getFalse();
        ValueWithType nested = condition;
        if (condition.type->isNullable())
        {
            is_null = b.CreateExtractValue(condition.value, {1});
            nested = ValueWithType(b.CreateExtractValue(condition.value, {0}), removeNullable(condition.type));
        }
        auto * is_not_zero = nativeBoolCast(b, nested);

        llvm::Value * holds = nullptr;
        switch (term.kind)
        {
            case CompileDAG::GuardTerm::Kind::True:
                holds = b.CreateAnd(b.CreateNot(is_null), is_not_zero);
                break;
            case CompileDAG::GuardTerm::Kind::NotTrue:
                holds = b.CreateOr(is_null, b.CreateNot(is_not_zero));
                break;
            case CompileDAG::GuardTerm::Kind::NotFalse:
                holds = b.CreateOr(is_null, is_not_zero);
                break;
        }
        mask = b.CreateAnd(mask, holds);
    }
    return mask;
}

}

ValueWithType CompileDAG::compile(llvm::IRBuilderBase & builder, const ValuesWithType & input_nodes_values) const
{
    chassert(input_nodes_values.size() == getInputNodesCount());

    llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

    ValuesWithType compiled_values;
    compiled_values.resize(nodes.size());

    size_t input_nodes_values_index = 0;
    size_t compiled_values_index = 0;

    size_t dag_size = nodes.size();

    for (size_t i = 0; i < dag_size; ++i)
    {
        const auto & node = nodes[i];

        switch (node.type)
        {
            case CompileType::CONSTANT:
            {
                auto * native_value = node.skip_compile ? nullptr : getColumnNativeValue(b, node.result_type, *node.column, 0);
                compiled_values[compiled_values_index] = {native_value, node.result_type};
                break;
            }
            case CompileType::FUNCTION:
            {
                /// Errors reported while this node is compiled are attributed to it.
                if (auto * error_sink = JITErrorSink::current())
                    error_sink->setCurrentNode(i);

                /// A lazily evaluated node is computed on every row, but its errors count only where it is needed.
                std::optional<JITErrorMaskScope> guard_scope;
                if (!node.guard.empty())
                    guard_scope.emplace(compileGuard(b, node.guard, compiled_values));

                ValuesWithType temporary_values;
                temporary_values.reserve(node.arguments.size());
                for (auto argument_index : node.arguments)
                    temporary_values.emplace_back(compiled_values[argument_index]);

                ValueWithType compiled_value{node.function->compile(builder, temporary_values), node.function->getResultType()};
                if (!node.result_type->equals(*node.function->getResultType()))
                    compiled_values[compiled_values_index] = {nativeCast(b, compiled_value, node.result_type), node.result_type};
                else
                    compiled_values[compiled_values_index] = std::move(compiled_value);

                break;
            }
            case CompileType::INPUT:
            {
                compiled_values[compiled_values_index] = {input_nodes_values[input_nodes_values_index].value, node.result_type};
                ++input_nodes_values_index;
                break;
            }
        }

        ++compiled_values_index;
    }

    return compiled_values.back();
}

std::string CompileDAG::dump() const
{
    std::vector<std::string> dumped_values;
    dumped_values.resize(nodes.size());

    size_t dag_size = nodes.size();
    for (size_t i = 0; i < dag_size; ++i)
    {
        const auto & node = nodes[i];

        switch (node.type)
        {
            case CompileType::CONSTANT:
            {
                const auto & data = node.column->getDataColumn();

                dumped_values[i] = applyVisitor(FieldVisitorToString(), data[0]) + " : " + node.result_type->getName();
                break;
            }
            case CompileType::FUNCTION:
            {
                std::string function_dump = node.function->getName();
                function_dump += '(';

                for (auto argument_index : node.arguments)
                {
                    function_dump += dumped_values[argument_index];
                    function_dump += ", ";
                }

                if (!node.arguments.empty())
                {
                    function_dump.pop_back();
                    function_dump.pop_back();
                }

                function_dump += ')';

                dumped_values[i] = std::move(function_dump);
                break;
            }
            case CompileType::INPUT:
            {
                dumped_values[i] = node.result_type->getName();
                break;
            }
        }
    }

    return dumped_values.back();
}

namespace
{

/// A `DateTime` or a `DateTime64` without a time zone of its own is named without one, whatever time zone it holds: the
/// session's when the type was created, e.g. when the table was loaded. Compiled code that reads the time-zone table depends
/// on it, so expressions over types holding different time zones must not share compiled code.
void updateHashWithTimeZone(SipHash & hash, const DataTypePtr & type)
{
    if (const auto * timezone_mixin = dynamic_cast<const TimezoneMixin *>(removeNullable(type).get()))
    {
        const auto & time_zone = timezone_mixin->getTimeZone().getTimeZone();
        hash.update(time_zone.size());
        hash.update(time_zone);
    }
}

}

UInt128 CompileDAG::hash() const
{
    SipHash hash;
    for (const auto & node : nodes)
    {
        hash.update(node.type);

        const auto & result_type_name = node.result_type->getName();
        hash.update(result_type_name.size());
        hash.update(result_type_name);
        updateHashWithTimeZone(hash, node.result_type);

        switch (node.type)
        {
            case CompileType::CONSTANT:
            {
                node.column->getDataColumn().updateHashWithValue(0, hash);
                break;
            }
            case CompileType::FUNCTION:
            {
                const auto & function_name = node.function->getName();

                hash.update(function_name.size());
                hash.update(function_name);

                for (size_t arg : node.arguments)
                    hash.update(arg);

                hash.update(node.guard.size());
                for (const auto & term : node.guard)
                {
                    hash.update(term.condition);
                    hash.update(term.kind);
                }

                /// The settings the function captured, which can change its compiled code without changing its types
                /// (see `IFunctionBase::updateHash`): expressions that differ in them must not share compiled code.
                node.function->updateHash(hash);

                break;
            }
            case CompileType::INPUT:
            {
                break;
            }
        }
    }

    return hash.get128();
}

}

#endif
