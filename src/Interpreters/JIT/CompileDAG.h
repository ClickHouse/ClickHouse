#pragma once

#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

#if USE_EMBEDDED_COMPILER

#include <vector>

#include <Core/Types.h>

#include <Columns/IColumn.h>
#include <DataTypes/IDataType.h>
#include <Functions/IFunction.h>

namespace llvm
{
    class Value;
    class IRBuilderBase;
}

namespace DB
{

/** This class is needed to compile part of ActionsDAG.
  * For example we have expression (a + 1) + (b + 1) in actions dag.
  * It must be added into CompileDAG in order of compile evaluation.
  * Node a, Constant 1, Function add(a + 1), Input b, Constant 1, Function add(b, 1), Function add(add(a + 1), add(a + 1)).
  *
  * Compile function must be called with input_nodes_values equal to input nodes count.
  * When compile method is called added nodes are compiled in order.
  */
class CompileDAG
{
public:

    enum class CompileType
    {
        INPUT = 0,
        CONSTANT = 1,
        FUNCTION = 2,
    };

    struct Node
    {
        CompileType type;
        DataTypePtr result_type;

        /// For CONSTANT
        ColumnPtr column;

        /// For FUNCTION
        FunctionBasePtr function;
        std::vector<size_t> arguments;
    };

    llvm::Value * compile(llvm::IRBuilderBase & builder, Values input_nodes_values) const;

    std::string dump() const;

    UInt128 hash() const;

    void addNode(Node node)
    {
        input_nodes_count += (node.type == CompileType::INPUT);
        nodes.emplace_back(std::move(node));
    }

    inline size_t getNodesCount() const { return nodes.size(); }
    inline size_t getInputNodesCount() const { return input_nodes_count; }

    inline Node & operator[](size_t index) { return nodes[index]; }
    inline const Node & operator[](size_t index) const { return nodes[index]; }

    inline Node & front() { return nodes.front(); }
    inline const Node & front() const { return nodes.front(); }

    inline Node & back() { return nodes.back(); }
    inline const Node & back() const { return nodes.back(); }

private:
    std::vector<Node> nodes;
    size_t input_nodes_count = 0;
};

}

#endif
