#pragma once

#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

#if USE_EMBEDDED_COMPILER
#    include <set>
#    include <Functions/IFunctionImpl.h>
#    include <Interpreters/Context.h>
#    include <Interpreters/ExpressionActions.h>
#    include <Common/LRUCache.h>


namespace DB
{

using CompilableExpression = std::function<llvm::Value * (llvm::IRBuilderBase &, const ValuePlaceholders &)>;

struct LLVMModuleState;

class LLVMFunction : public IFunctionBaseImpl
{
    std::string name;
    DataTypes arg_types;

    std::vector<FunctionBasePtr> originals;
    CompilableExpression expression;

    std::unique_ptr<LLVMModuleState> module_state;

public:

    struct CompileNode
    {
        enum class NodeType
        {
            INPUT = 0,
            CONSTANT = 1,
            FUNCTION = 2,
        };

        NodeType type;
        DataTypePtr result_type;

        /// For CONSTANT
        ColumnPtr column;

        /// For FUNCTION
        FunctionBasePtr function;
        std::vector<size_t> arguments;
    };

    struct CompileDAG : public std::vector<CompileNode>
    {
        std::string dump() const;
        UInt128 hash() const;
    };

    LLVMFunction(const CompileDAG & dag);

    bool isCompilable() const override { return true; }

    llvm::Value * compile(llvm::IRBuilderBase & builder, ValuePlaceholders values) const override;

    String getName() const override { return name; }

    const DataTypes & getArgumentTypes() const override { return arg_types; }

    const DataTypePtr & getResultType() const override { return originals.back()->getResultType(); }

    ExecutableFunctionImplPtr prepare(const ColumnsWithTypeAndName &) const override;

    bool isDeterministic() const override;

    bool isDeterministicInScopeOfQuery() const override;

    bool isSuitableForConstantFolding() const override;

    bool isInjective(const ColumnsWithTypeAndName & sample_block) const override;

    bool hasInformationAboutMonotonicity() const override;

    Monotonicity getMonotonicityForRange(const IDataType & type, const Field & left, const Field & right) const override;

    const LLVMModuleState * getLLVMModuleState() const { return module_state.get(); }
};

/** This child of LRUCache breaks one of it's invariants: total weight may be changed after insertion.
 * We have to do so, because we don't known real memory consumption of generated LLVM code for every function.
 */
class CompiledExpressionCache : public LRUCache<UInt128, IFunctionBase, UInt128Hash>
{
public:
    using Base = LRUCache<UInt128, IFunctionBase, UInt128Hash>;
    using Base::Base;
};

/// For each APPLY_FUNCTION action, try to compile the function to native code; if the only uses of a compilable
/// function's result are as arguments to other compilable functions, inline it and leave the now-redundant action as-is.
// void compileFunctions(ExpressionActions::Actions & actions, const Names & output_columns, const Block & sample_block, std::shared_ptr<CompiledExpressionCache> compilation_cache, size_t min_count_to_compile_expression);

}

#endif
