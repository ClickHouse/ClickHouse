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
    Names arg_names;
    DataTypes arg_types;

    std::vector<FunctionBasePtr> originals;
    std::unordered_map<StringRef, CompilableExpression> subexpressions;

    std::unique_ptr<LLVMModuleState> module_state;

public:
    LLVMFunction(const ExpressionActions::Actions & actions, const Block & sample_block);

    bool isCompilable() const override { return true; }

    llvm::Value * compile(llvm::IRBuilderBase & builder, ValuePlaceholders values) const override;

    String getName() const override { return name; }

    const Names & getArgumentNames() const { return arg_names; }

    const DataTypes & getArgumentTypes() const override { return arg_types; }

    const DataTypePtr & getReturnType() const override { return originals.back()->getReturnType(); }

    ExecutableFunctionImplPtr prepare(const Block &, const ColumnNumbers &, size_t) const override;

    bool isDeterministic() const override;

    bool isDeterministicInScopeOfQuery() const override;

    bool isSuitableForConstantFolding() const override;

    bool isInjective(const Block & sample_block) const override;

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
void compileFunctions(ExpressionActions::Actions & actions, const Names & output_columns, const Block & sample_block, std::shared_ptr<CompiledExpressionCache> compilation_cache, size_t min_count_to_compile_expression);

}

#endif
