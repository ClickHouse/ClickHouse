#pragma once
#include <Functions/IFunction.h>

namespace DB
{

/// Cache for functions result if it was executed on low cardinality column.
class PreparedFunctionLowCardinalityResultCache;
using PreparedFunctionLowCardinalityResultCachePtr = std::shared_ptr<PreparedFunctionLowCardinalityResultCache>;

class IExecutableFunctionImpl
{
public:
    virtual ~IExecutableFunctionImpl() = default;

    virtual String getName() const = 0;

    virtual void execute(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) = 0;
    virtual void executeDryRun(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count)
    {
        execute(block, arguments, result, input_rows_count);
    }

    /** Default implementation in presence of Nullable arguments or NULL constants as arguments is the following:
      *  if some of arguments are NULL constants then return NULL constant,
      *  if some of arguments are Nullable, then execute function as usual for block,
      *   where Nullable columns are substituted with nested columns (they have arbitrary values in rows corresponding to NULL value)
      *   and wrap result in Nullable column where NULLs are in all rows where any of arguments are NULL.
      */
    virtual bool useDefaultImplementationForNulls() const { return true; }

    /** If the function have non-zero number of arguments,
      *  and if all arguments are constant, that we could automatically provide default implementation:
      *  arguments are converted to ordinary columns with single value, then function is executed as usual,
      *  and then the result is converted to constant column.
      */
    virtual bool useDefaultImplementationForConstants() const { return false; }

    /** If function arguments has single low cardinality column and all other arguments are constants, call function on nested column.
      * Otherwise, convert all low cardinality columns to ordinary columns.
      * Returns ColumnLowCardinality if at least one argument is ColumnLowCardinality.
      */
    virtual bool useDefaultImplementationForLowCardinalityColumns() const { return true; }

    /** Some arguments could remain constant during this implementation.
      */
    virtual ColumnNumbers getArgumentsThatAreAlwaysConstant() const { return {}; }

    /** True if function can be called on default arguments (include Nullable's) and won't throw.
      * Counterexample: modulo(0, 0)
      */
    virtual bool canBeExecutedOnDefaultArguments() const { return true; }
};

using ExecutableFunctionImplPtr = std::unique_ptr<IExecutableFunctionImpl>;


class IFunctionBaseImpl
{
public:
    virtual ~IFunctionBaseImpl() = default;

    virtual String getName() const = 0;

    virtual const DataTypes & getArgumentTypes() const = 0;
    virtual const DataTypePtr & getReturnType() const = 0;

    virtual ExecutableFunctionImplPtr prepare(const Block & sample_block, const ColumnNumbers & arguments, size_t result) const = 0;

#if USE_EMBEDDED_COMPILER

    virtual bool isCompilable() const { return false; }

    virtual llvm::Value * compile(llvm::IRBuilderBase & /*builder*/, ValuePlaceholders /*values*/) const
    {
        throw Exception(getName() + " is not JIT-compilable", ErrorCodes::NOT_IMPLEMENTED);
    }

#endif

    virtual bool isStateful() const { return false; }

    virtual bool isSuitableForConstantFolding() const { return true; }
    virtual ColumnPtr getResultIfAlwaysReturnsConstantAndHasArguments(const Block & /*block*/, const ColumnNumbers & /*arguments*/) const { return nullptr; }

    virtual bool isInjective(const Block & /*sample_block*/) { return false; }
    virtual bool isDeterministic() const { return true; }
    virtual bool isDeterministicInScopeOfQuery() const { return true; }
    virtual bool hasInformationAboutMonotonicity() const { return false; }

    using Monotonicity = IFunctionBase::Monotonicity;
    virtual Monotonicity getMonotonicityForRange(const IDataType & /*type*/, const Field & /*left*/, const Field & /*right*/) const
    {
        throw Exception("Function " + getName() + " has no information about its monotonicity.", ErrorCodes::NOT_IMPLEMENTED);
    }
};

using FunctionBaseImplPtr = std::unique_ptr<IFunctionBaseImpl>;


class IFunctionOverloadResolverImpl
{
public:
    virtual ~IFunctionOverloadResolverImpl() = default;

    virtual String getName() const = 0;

    virtual FunctionBaseImplPtr build(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type) const = 0;

    virtual DataTypePtr getReturnType(const DataTypes & /*arguments*/) const
    {
        throw Exception("getReturnType is not implemented for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    /// Get the result type by argument type. If the function does not apply to these arguments, throw an exception.
    virtual DataTypePtr getReturnType(const ColumnsWithTypeAndName & arguments) const
    {
        DataTypes data_types(arguments.size());
        for (size_t i = 0; i < arguments.size(); ++i)
            data_types[i] = arguments[i].type;

        return getReturnType(data_types);
    }

    /// For non-variadic functions, return number of arguments; otherwise return zero (that should be ignored).
    virtual size_t getNumberOfArguments() const = 0;

    virtual bool isDeterministic() const { return true; }
    virtual bool isDeterministicInScopeOfQuery() const { return true; }
    virtual bool isStateful() const { return false; }
    virtual bool isVariadic() const { return false; }

    virtual void checkNumberOfArgumentsIfVariadic(size_t /*number_of_arguments*/) const
    {
        throw Exception("checkNumberOfArgumentsIfVariadic is not implemented for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    virtual void getLambdaArgumentTypes(DataTypes & /*arguments*/) const
    {
        throw Exception("Function " + getName() + " can't have lambda-expressions as arguments", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    virtual ColumnNumbers getArgumentsThatAreAlwaysConstant() const { return {}; }
    virtual ColumnNumbers getArgumentsThatDontImplyNullableReturnType(size_t /*number_of_arguments*/) const { return {}; }

    /** If useDefaultImplementationForNulls() is true, than change arguments for getReturnType() and buildImpl():
      *  if some of arguments are Nullable(Nothing) then don't call getReturnType(), call buildImpl() with return_type = Nullable(Nothing),
      *  if some of arguments are Nullable, then:
      *   - Nullable types are substituted with nested types for getReturnType() function
      *   - wrap getReturnType() result in Nullable type and pass to buildImpl
      *
      * Otherwise build returns buildImpl(arguments, getReturnType(arguments));
      */
    virtual bool useDefaultImplementationForNulls() const { return true; }

    /** If useDefaultImplementationForNulls() is true, than change arguments for getReturnType() and buildImpl().
      * If function arguments has low cardinality types, convert them to ordinary types.
      * getReturnType returns ColumnLowCardinality if at least one argument type is ColumnLowCardinality.
      */
    virtual bool useDefaultImplementationForLowCardinalityColumns() const { return true; }

    /// If it isn't, will convert all ColumnLowCardinality arguments to full columns.
    virtual bool canBeExecutedOnLowCardinalityDictionary() const { return true; }
};

using FunctionOverloadResolverImplPtr = std::unique_ptr<IFunctionOverloadResolverImpl>;


/// Previous function interface.
class IFunction : public std::enable_shared_from_this<IFunction>
{
public:
    virtual ~IFunction() = default;

    virtual String getName() const = 0;

    virtual void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) = 0;
    virtual void executeImplDryRun(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count)
    {
        executeImpl(block, arguments, result, input_rows_count);
    }

    /** Default implementation in presence of Nullable arguments or NULL constants as arguments is the following:
      *  if some of arguments are NULL constants then return NULL constant,
      *  if some of arguments are Nullable, then execute function as usual for block,
      *   where Nullable columns are substituted with nested columns (they have arbitrary values in rows corresponding to NULL value)
      *   and wrap result in Nullable column where NULLs are in all rows where any of arguments are NULL.
      */
    virtual bool useDefaultImplementationForNulls() const { return true; }

    /** If the function have non-zero number of arguments,
      *  and if all arguments are constant, that we could automatically provide default implementation:
      *  arguments are converted to ordinary columns with single value, then function is executed as usual,
      *  and then the result is converted to constant column.
      */
    virtual bool useDefaultImplementationForConstants() const { return false; }

    /** If function arguments has single low cardinality column and all other arguments are constants, call function on nested column.
      * Otherwise, convert all low cardinality columns to ordinary columns.
      * Returns ColumnLowCardinality if at least one argument is ColumnLowCardinality.
      */
    virtual bool useDefaultImplementationForLowCardinalityColumns() const { return true; }

    /// If it isn't, will convert all ColumnLowCardinality arguments to full columns.
    virtual bool canBeExecutedOnLowCardinalityDictionary() const { return true; }

    /** Some arguments could remain constant during this implementation.
      */
    virtual ColumnNumbers getArgumentsThatAreAlwaysConstant() const { return {}; }

    /** True if function can be called on default arguments (include Nullable's) and won't throw.
      * Counterexample: modulo(0, 0)
      */
    virtual bool canBeExecutedOnDefaultArguments() const { return true; }

#if USE_EMBEDDED_COMPILER

    virtual bool isCompilable() const
    {
        throw Exception("isCompilable without explicit types is not implemented for IFunction", ErrorCodes::NOT_IMPLEMENTED);
    }

    virtual llvm::Value * compile(llvm::IRBuilderBase & /*builder*/, ValuePlaceholders /*values*/) const
    {
        throw Exception("compile without explicit types is not implemented for IFunction", ErrorCodes::NOT_IMPLEMENTED);
    }

#endif


    /** Should we evaluate this function while constant folding, if arguments are constants?
      * Usually this is true. Notable counterexample is function 'sleep'.
      * If we will call it during query analysis, we will sleep extra amount of time.
      */
    virtual bool isSuitableForConstantFolding() const { return true; }

    /** Some functions like ignore(...) or toTypeName(...) always return constant result which doesn't depend on arguments.
      * In this case we can calculate result and assume that it's constant in stream header.
      * There is no need to implement function if it has zero arguments.
      * Must return ColumnConst with single row or nullptr.
      */
    virtual ColumnPtr getResultIfAlwaysReturnsConstantAndHasArguments(const Block & /*block*/, const ColumnNumbers & /*arguments*/) const { return nullptr; }

    /** Function is called "injective" if it returns different result for different values of arguments.
      * Example: hex, negate, tuple...
      *
      * Function could be injective with some arguments fixed to some constant values.
      * Examples:
      *  plus(const, x);
      *  multiply(const, x) where x is an integer and constant is not divisible by two;
      *  concat(x, 'const');
      *  concat(x, 'const', y) where const contain at least one non-numeric character;
      *  concat with FixedString
      *  dictGet... functions takes name of dictionary as its argument,
      *   and some dictionaries could be explicitly defined as injective.
      *
      * It could be used, for example, to remove useless function applications from GROUP BY.
      *
      * Sometimes, function is not really injective, but considered as injective, for purpose of query optimization.
      * For example, toString function is not injective for Float64 data type,
      *  as it returns 'nan' for many different representation of NaNs.
      * But we assume, that it is injective. This could be documented as implementation-specific behaviour.
      *
      * sample_block should contain data types of arguments and values of constants, if relevant.
      */
    virtual bool isInjective(const Block & /*sample_block*/) { return false; }

    /** Function is called "deterministic", if it returns same result for same values of arguments.
      * Most of functions are deterministic. Notable counterexample is rand().
      * Sometimes, functions are "deterministic" in scope of single query
      *  (even for distributed query), but not deterministic it general.
      * Example: now(). Another example: functions that work with periodically updated dictionaries.
      */

    virtual bool isDeterministic() const { return true; }

    virtual bool isDeterministicInScopeOfQuery() const { return true; }

    virtual bool isStateful() const { return false; }

    /** Lets you know if the function is monotonic in a range of values.
      * This is used to work with the index in a sorted chunk of data.
      * And allows to use the index not only when it is written, for example `date >= const`, but also, for example, `toMonth(date) >= 11`.
      * All this is considered only for functions of one argument.
      */
    virtual bool hasInformationAboutMonotonicity() const { return false; }

    using Monotonicity = IFunctionBase::Monotonicity;
    /** Get information about monotonicity on a range of values. Call only if hasInformationAboutMonotonicity.
      * NULL can be passed as one of the arguments. This means that the corresponding range is unlimited on the left or on the right.
      */
    virtual Monotonicity getMonotonicityForRange(const IDataType & /*type*/, const Field & /*left*/, const Field & /*right*/) const
    {
        throw Exception("Function " + getName() + " has no information about its monotonicity.", ErrorCodes::NOT_IMPLEMENTED);
    }

    /// For non-variadic functions, return number of arguments; otherwise return zero (that should be ignored).
    virtual size_t getNumberOfArguments() const = 0;

    virtual DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const
    {
        throw Exception("getReturnType is not implemented for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    /// Get the result type by argument type. If the function does not apply to these arguments, throw an exception.
    virtual DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const
    {
        DataTypes data_types(arguments.size());
        for (size_t i = 0; i < arguments.size(); ++i)
            data_types[i] = arguments[i].type;

        return getReturnTypeImpl(data_types);
    }

    virtual bool isVariadic() const { return false; }

    virtual void checkNumberOfArgumentsIfVariadic(size_t /*number_of_arguments*/) const
    {
        throw Exception("checkNumberOfArgumentsIfVariadic is not implemented for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    virtual void getLambdaArgumentTypes(DataTypes & /*arguments*/) const
    {
        throw Exception("Function " + getName() + " can't have lambda-expressions as arguments", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    virtual ColumnNumbers getArgumentsThatDontImplyNullableReturnType(size_t /*number_of_arguments*/) const { return {}; }


#if USE_EMBEDDED_COMPILER

    bool isCompilable(const DataTypes & arguments) const;

    llvm::Value * compile(llvm::IRBuilderBase &, const DataTypes & arguments, ValuePlaceholders values) const;

#endif

protected:

#if USE_EMBEDDED_COMPILER

    virtual bool isCompilableImpl(const DataTypes &) const { return false; }

    virtual llvm::Value * compileImpl(llvm::IRBuilderBase &, const DataTypes &, ValuePlaceholders) const
    {
        throw Exception(getName() + " is not JIT-compilable", ErrorCodes::NOT_IMPLEMENTED);
    }

#endif
};


class ExecutableFunctionAdaptor final : public IExecutableFunction
{
public:
    explicit ExecutableFunctionAdaptor(ExecutableFunctionImplPtr impl_) : impl(std::move(impl_)) {}

    String getName() const final { return impl->getName(); }

    void execute(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count, bool dry_run) final;

    void createLowCardinalityResultCache(size_t cache_size) override;

private:
    ExecutableFunctionImplPtr impl;

    /// Cache is created by function createLowCardinalityResultCache()
    PreparedFunctionLowCardinalityResultCachePtr low_cardinality_result_cache;

    bool defaultImplementationForConstantArguments(
            Block & block, const ColumnNumbers & args, size_t result, size_t input_rows_count, bool dry_run);

    bool defaultImplementationForNulls(
            Block & block, const ColumnNumbers & args, size_t result, size_t input_rows_count, bool dry_run);

    void executeWithoutLowCardinalityColumns(
            Block & block, const ColumnNumbers & args, size_t result, size_t input_rows_count, bool dry_run);
};

class FunctionBaseAdaptor final : public IFunctionBase
{
public:
    explicit FunctionBaseAdaptor(FunctionBaseImplPtr impl_) : impl(std::move(impl_)) {}

    String getName() const final { return impl->getName(); }

    const DataTypes & getArgumentTypes() const final { return impl->getArgumentTypes(); }
    const DataTypePtr & getReturnType() const final { return impl->getReturnType(); }

    ExecutableFunctionPtr prepare(const Block & sample_block, const ColumnNumbers & arguments, size_t result) const final
    {
        return std::make_shared<ExecutableFunctionAdaptor>(impl->prepare(sample_block, arguments, result));
    }

#if USE_EMBEDDED_COMPILER

    bool isCompilable() const final { return impl->isCompilable(); }

    llvm::Value * compile(llvm::IRBuilderBase & builder, ValuePlaceholders values) const override
    {
        return impl->compile(builder, std::move(values));
    }

#endif

    bool isStateful() const final { return impl->isStateful(); }
    bool isSuitableForConstantFolding() const final { return impl->isSuitableForConstantFolding(); }

    ColumnPtr getResultIfAlwaysReturnsConstantAndHasArguments(const Block & block, const ColumnNumbers & arguments) const final
    {
        return impl->getResultIfAlwaysReturnsConstantAndHasArguments(block, arguments);
    }

    bool isInjective(const Block & sample_block) final { return impl->isInjective(sample_block); }
    bool isDeterministic() const final { return impl->isDeterministic(); }
    bool isDeterministicInScopeOfQuery() const final { return impl->isDeterministicInScopeOfQuery(); }
    bool hasInformationAboutMonotonicity() const final { return impl->hasInformationAboutMonotonicity(); }

    Monotonicity getMonotonicityForRange(const IDataType & type, const Field & left, const Field & right) const final
    {
        return impl->getMonotonicityForRange(type, left, right);
    }

private:
    FunctionBaseImplPtr impl;
};


class FunctionOverloadResolverAdaptor : public IFunctionOverloadResolver
{
public:
    explicit FunctionOverloadResolverAdaptor(FunctionOverloadResolverImplPtr impl_) : impl(std::move(impl_)) {}

    String getName() const final { return impl->getName(); }

    bool isDeterministic() const final { return impl->isDeterministic(); }

    bool isDeterministicInScopeOfQuery() const final { return impl->isDeterministicInScopeOfQuery(); }

    bool isStateful() const final { return impl->isStateful(); }

    bool isVariadic() const final { return impl->isVariadic(); }

    size_t getNumberOfArguments() const final { return impl->getNumberOfArguments(); }

    void checkNumberOfArguments(size_t number_of_arguments) const final;

    FunctionBaseImplPtr buildImpl(const ColumnsWithTypeAndName & arguments) const
    {
        return impl->build(arguments, getReturnType(arguments));
    }

    FunctionBasePtr build(const ColumnsWithTypeAndName & arguments) const final
    {
        return std::make_shared<FunctionBaseAdaptor>(buildImpl(arguments));
    }

    void getLambdaArgumentTypes(DataTypes & arguments) const final
    {
        checkNumberOfArguments(arguments.size());
        impl->getLambdaArgumentTypes(arguments);
    }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const final { return impl->getArgumentsThatAreAlwaysConstant(); }

    ColumnNumbers getArgumentsThatDontImplyNullableReturnType(size_t number_of_arguments) const final
    {
        return impl->getArgumentsThatDontImplyNullableReturnType(number_of_arguments);
    }

private:
    FunctionOverloadResolverImplPtr impl;

    DataTypePtr getReturnTypeWithoutLowCardinality(const ColumnsWithTypeAndName & arguments) const;
    DataTypePtr getReturnType(const ColumnsWithTypeAndName & arguments) const;
};

/// Wrappers over IFunction.

class DefaultExecutable final : public IExecutableFunctionImpl
{
public:
    explicit DefaultExecutable(std::shared_ptr<IFunction> function_) : function(std::move(function_)) {}

    String getName() const override { return function->getName(); }

protected:
    void execute(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) final
    {
        return function->executeImpl(block, arguments, result, input_rows_count);
    }
    void executeDryRun(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) final
    {
        return function->executeImplDryRun(block, arguments, result, input_rows_count);
    }
    bool useDefaultImplementationForNulls() const final { return function->useDefaultImplementationForNulls(); }
    bool useDefaultImplementationForConstants() const final { return function->useDefaultImplementationForConstants(); }
    bool useDefaultImplementationForLowCardinalityColumns() const final { return function->useDefaultImplementationForLowCardinalityColumns(); }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const final { return function->getArgumentsThatAreAlwaysConstant(); }
    bool canBeExecutedOnDefaultArguments() const override { return function->canBeExecutedOnDefaultArguments(); }

private:
    std::shared_ptr<IFunction> function;
};

class DefaultFunction final : public IFunctionBaseImpl
{
public:
    DefaultFunction(std::shared_ptr<IFunction> function_, DataTypes arguments_, DataTypePtr return_type_)
            : function(std::move(function_)), arguments(std::move(arguments_)), return_type(std::move(return_type_)) {}

    String getName() const override { return function->getName(); }

    const DataTypes & getArgumentTypes() const override { return arguments; }
    const DataTypePtr & getReturnType() const override { return return_type; }

#if USE_EMBEDDED_COMPILER

    bool isCompilable() const override { return function->isCompilable(arguments); }

    llvm::Value * compile(llvm::IRBuilderBase & builder, ValuePlaceholders values) const override { return function->compile(builder, arguments, std::move(values)); }

#endif

    ExecutableFunctionImplPtr prepare(const Block & /*sample_block*/, const ColumnNumbers & /*arguments*/, size_t /*result*/) const override
    {
        return std::make_unique<DefaultExecutable>(function);
    }

    bool isSuitableForConstantFolding() const override { return function->isSuitableForConstantFolding(); }
    ColumnPtr getResultIfAlwaysReturnsConstantAndHasArguments(const Block & block, const ColumnNumbers & arguments_) const override
    {
        return function->getResultIfAlwaysReturnsConstantAndHasArguments(block, arguments_);
    }

    bool isStateful() const override { return function->isStateful(); }

    bool isInjective(const Block & sample_block) override { return function->isInjective(sample_block); }

    bool isDeterministic() const override { return function->isDeterministic(); }

    bool isDeterministicInScopeOfQuery() const override { return function->isDeterministicInScopeOfQuery(); }

    bool hasInformationAboutMonotonicity() const override { return function->hasInformationAboutMonotonicity(); }

    using Monotonicity = IFunctionBase::Monotonicity;
    Monotonicity getMonotonicityForRange(const IDataType & type, const Field & left, const Field & right) const override
    {
        return function->getMonotonicityForRange(type, left, right);
    }
private:
    std::shared_ptr<IFunction> function;
    DataTypes arguments;
    DataTypePtr return_type;
};

class DefaultFunctionBuilder : public IFunctionOverloadResolverImpl
{
public:
    explicit DefaultFunctionBuilder(std::shared_ptr<IFunction> function_) : function(std::move(function_)) {}

    void checkNumberOfArgumentsIfVariadic(size_t number_of_arguments) const override
    {
        return function->checkNumberOfArgumentsIfVariadic(number_of_arguments);
    }

    bool isDeterministic() const override { return function->isDeterministic(); }
    bool isDeterministicInScopeOfQuery() const override { return function->isDeterministicInScopeOfQuery(); }

    String getName() const override { return function->getName(); }
    bool isStateful() const override { return function->isStateful(); }
    bool isVariadic() const override { return function->isVariadic(); }
    size_t getNumberOfArguments() const override { return function->getNumberOfArguments(); }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return function->getArgumentsThatAreAlwaysConstant(); }
    ColumnNumbers getArgumentsThatDontImplyNullableReturnType(size_t number_of_arguments) const override
    {
        return function->getArgumentsThatDontImplyNullableReturnType(number_of_arguments);
    }

    DataTypePtr getReturnType(const DataTypes & arguments) const override { return function->getReturnTypeImpl(arguments); }
    DataTypePtr getReturnType(const ColumnsWithTypeAndName & arguments) const override { return function->getReturnTypeImpl(arguments); }

    bool useDefaultImplementationForNulls() const override { return function->useDefaultImplementationForNulls(); }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return function->useDefaultImplementationForLowCardinalityColumns(); }
    bool canBeExecutedOnLowCardinalityDictionary() const override { return function->canBeExecutedOnLowCardinalityDictionary(); }

    FunctionBaseImplPtr build(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type) const override
    {
        DataTypes data_types(arguments.size());
        for (size_t i = 0; i < arguments.size(); ++i)
            data_types[i] = arguments[i].type;
        return std::make_unique<DefaultFunction>(function, data_types, return_type);
    }

    void getLambdaArgumentTypes(DataTypes & arguments) const override { function->getLambdaArgumentTypes(arguments); }

private:
    std::shared_ptr<IFunction> function;
};

using FunctionPtr = std::shared_ptr<IFunction>;

}
