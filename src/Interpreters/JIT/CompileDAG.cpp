#include "CompileDAG.h"

#if USE_EMBEDDED_COMPILER

#include <llvm/IR/BasicBlock.h>
#include <llvm/IR/Function.h>
#include <llvm/IR/IRBuilder.h>

#include <Common/SipHash.h>
#include <Common/FieldVisitorToString.h>
#include <Columns/ColumnConst.h>
#include <DataTypes/Native.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>

namespace DB
{

ValueWithType CompileDAG::compile(llvm::IRBuilderBase & builder, const ValuesWithType & input_nodes_values) const
{
    assert(input_nodes_values.size() == getInputNodesCount());

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
                auto * native_value = getColumnNativeValue(b, node.result_type, *node.column, 0);
                compiled_values[compiled_values_index] = {native_value, node.result_type};
                break;
            }
            case CompileType::FUNCTION:
            {
                ValuesWithType temporary_values;
                temporary_values.reserve(node.arguments.size());

                for (auto argument_index : node.arguments)
                {
                    assert(compiled_values[argument_index].value != nullptr);
                    temporary_values.emplace_back(compiled_values[argument_index]);
                }

                compiled_values[compiled_values_index] = {node.function->compile(builder, temporary_values), node.result_type};
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
                const auto * column = typeid_cast<const ColumnConst *>(node.column.get());
                const auto & data = column->getDataColumn();

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

UInt128 CompileDAG::hash() const
{
    SipHash hash;
    for (const auto & node : nodes)
    {
        hash.update(node.type);

        const auto & result_type_name = node.result_type->getName();
        hash.update(result_type_name.size());
        hash.update(result_type_name);

        switch (node.type)
        {
            case CompileType::CONSTANT:
            {
                assert_cast<const ColumnConst *>(node.column.get())->getDataColumn().updateHashWithValue(0, hash);
                break;
            }
            case CompileType::FUNCTION:
            {
                const auto & function_name = node.function->getName();

                hash.update(function_name.size());
                hash.update(function_name);

                for (size_t arg : node.arguments)
                    hash.update(arg);

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
