#pragma once

#include <Common/config.h>

#if USE_EMBEDDED_COMPILER

#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Common/LRUCache.h>
#include <set>


namespace DB
{

struct LLVMContext;
using CompilableExpression = std::function<llvm::Value * (llvm::IRBuilderBase &, const ValuePlaceholders &)>;

class LLVMFunction : public IFunctionBase
{
    std::string name;
    Names arg_names;
    DataTypes arg_types;
    std::shared_ptr<LLVMContext> context;
    std::vector<FunctionBasePtr> originals;
    std::unordered_map<StringRef, CompilableExpression> subexpressions;
public:
    LLVMFunction(const ExpressionActions::Actions & actions, std::shared_ptr<LLVMContext> context, const Block & sample_block);

    bool isCompilable() const override { return true; }

    llvm::Value * compile(llvm::IRBuilderBase & builder, ValuePlaceholders values) const override { return subexpressions.at(name)(builder, values); }

    String getName() const override { return name; }

    const Names & getArgumentNames() const { return arg_names; }

    const DataTypes & getArgumentTypes() const override { return arg_types; }

    const DataTypePtr & getReturnType() const override { return originals.back()->getReturnType(); }

    PreparedFunctionPtr prepare(const Block &) const override;

    bool isDeterministic() const override;

    bool isDeterministicInScopeOfQuery() const override;

    bool isSuitableForConstantFolding() const override;

    bool isInjective(const Block & sample_block) override;

    bool hasInformationAboutMonotonicity() const override;

    Monotonicity getMonotonicityForRange(const IDataType & type, const Field & left, const Field & right) const override;

    std::shared_ptr<LLVMContext> getContext() const { return context; }

};

/** This child of LRUCache breaks one of it's invariants: total weight may be changed after insertion.
 * We have to do so, because we don't known real memory consumption of generated LLVM code for every function.
 */
class CompiledExpressionCache : public LRUCache<UInt128, LLVMFunction, UInt128Hash>
{
private:
    using Base = LRUCache<UInt128, LLVMFunction, UInt128Hash>;

public:
    using Base::Base;

    size_t weight() const;
};

/// For each APPLY_FUNCTION action, try to compile the function to native code; if the only uses of a compilable
/// function's result are as arguments to other compilable functions, inline it and leave the now-redundant action as-is.
void compileFunctions(ExpressionActions::Actions & actions, const Names & output_columns, const Block & sample_block, std::shared_ptr<CompiledExpressionCache> compilation_cache, size_t min_count_to_compile);

}

#endif
