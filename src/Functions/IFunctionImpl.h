#pragma once
#include <Functions/IFunction.h>

/// This file contains developer interface for functions.
/// In order to implement a new function you can choose one of two options:
///  * Implement interface for IFunction (old function interface, which is planned to be removed sometimes)
///  * Implement three interfaces for IExecutableFunctionImpl, IFunctionBaseImpl and IFunctionOverloadResolverImpl
/// Generally saying, IFunction represents a union of three new interfaces. However, it can't be used for all cases.
/// Examples:
///  * Function properties may depend on arguments type (e.g. toUInt32(UInt8) is globally monotonic, toUInt32(UInt64) - only on intervals)
///  * In implementation of lambda functions DataTypeFunction needs an functional object with known arguments and return type
///  * Function CAST prepares specific implementation based on argument types
///
/// Interfaces for IFunction, IExecutableFunctionImpl, IFunctionBaseImpl and IFunctionOverloadResolverImpl are pure.
/// Default implementations are in adaptors classes (IFunctionAdaptors.h), which are implement user interfaces via developer ones.
/// Interfaces IExecutableFunctionImpl, IFunctionBaseImpl and IFunctionOverloadResolverImpl are implemented via IFunction
///  in DefaultExecutable, DefaultFunction and DefaultOverloadResolver classes (IFunctionAdaptors.h).

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NOT_IMPLEMENTED;
}

/// Cache for functions result if it was executed on low cardinality column.
class ExecutableFunctionLowCardinalityResultCache;
using ExecutableFunctionLowCardinalityResultCachePtr = std::shared_ptr<ExecutableFunctionLowCardinalityResultCache>;

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


/// This class generally has the same methods as in IFunctionBase.
/// See comments for IFunctionBase in IFunction.h
/// The main purpose is to implement `prepare` which returns IExecutableFunctionImpl, not IExecutableFunction
/// Inheritance is not used for better readability.
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

    virtual bool isInjective(const Block & /*sample_block*/) const { return false; }
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

    /// This function will be called in default implementation. You can overload it or the previous one.
    virtual DataTypePtr getReturnType(const ColumnsWithTypeAndName & arguments) const
    {
        DataTypes data_types(arguments.size());
        for (size_t i = 0; i < arguments.size(); ++i)
            data_types[i] = arguments[i].type;

        return getReturnType(data_types);
    }

    /// For non-variadic functions, return number of arguments; otherwise return zero (that should be ignored).
    virtual size_t getNumberOfArguments() const = 0;

    /// Properties from IFunctionOverloadResolver. See comments in IFunction.h
    virtual bool isDeterministic() const { return true; }
    virtual bool isDeterministicInScopeOfQuery() const { return true; }
    virtual bool isInjective(const Block &) const { return false; }
    virtual bool isStateful() const { return false; }
    virtual bool isVariadic() const { return false; }

    virtual void getLambdaArgumentTypes(DataTypes & /*arguments*/) const
    {
        throw Exception("Function " + getName() + " can't have lambda-expressions as arguments", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    virtual ColumnNumbers getArgumentsThatAreAlwaysConstant() const { return {}; }
    virtual ColumnNumbers getArgumentsThatDontImplyNullableReturnType(size_t /*number_of_arguments*/) const { return {}; }

    /** If useDefaultImplementationForNulls() is true, than change arguments for getReturnType() and build():
      *  if some of arguments are Nullable(Nothing) then don't call getReturnType(), call build() with return_type = Nullable(Nothing),
      *  if some of arguments are Nullable, then:
      *   - Nullable types are substituted with nested types for getReturnType() function
      *   - wrap getReturnType() result in Nullable type and pass to build
      *
      * Otherwise build returns build(arguments, getReturnType(arguments));
      */
    virtual bool useDefaultImplementationForNulls() const { return true; }

    /** If useDefaultImplementationForNulls() is true, than change arguments for getReturnType() and build().
      * If function arguments has low cardinality types, convert them to ordinary types.
      * getReturnType returns ColumnLowCardinality if at least one argument type is ColumnLowCardinality.
      */
    virtual bool useDefaultImplementationForLowCardinalityColumns() const { return true; }

    /// If it isn't, will convert all ColumnLowCardinality arguments to full columns.
    virtual bool canBeExecutedOnLowCardinalityDictionary() const { return true; }
};

using FunctionOverloadResolverImplPtr = std::unique_ptr<IFunctionOverloadResolverImpl>;


/// Previous function interface.
class IFunction
{
public:
    virtual ~IFunction() = default;

    virtual String getName() const = 0;

    virtual void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) const = 0;
    virtual void executeImplDryRun(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) const
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

    /// Properties from IFunctionBase (see IFunction.h)
    virtual bool isSuitableForConstantFolding() const { return true; }
    virtual ColumnPtr getResultIfAlwaysReturnsConstantAndHasArguments(const Block & /*block*/, const ColumnNumbers & /*arguments*/) const { return nullptr; }
    virtual bool isInjective(const Block & /*sample_block*/) const { return false; }
    virtual bool isDeterministic() const { return true; }
    virtual bool isDeterministicInScopeOfQuery() const { return true; }
    virtual bool isStateful() const { return false; }
    virtual bool hasInformationAboutMonotonicity() const { return false; }

    using Monotonicity = IFunctionBase::Monotonicity;
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

using FunctionPtr = std::shared_ptr<IFunction>;

}
