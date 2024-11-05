#pragma once

#include <Core/ColumnNumbers.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Field.h>
#include <Core/IResolvedFunction.h>
#include <Core/Names.h>
#include <Core/ValuesWithType.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionHelpers.h>
#include <Common/Exception.h>

#include "config.h"

#include <memory>

/// This file contains user interface for functions.

namespace llvm
{
    class LLVMContext;
    class Value;
    class IRBuilderBase;
}


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

/// A left-closed and right-open interval representing the preimage of a function.
using FieldInterval = std::pair<Field, Field>;
using OptionalFieldInterval = std::optional<FieldInterval>;

/// The simplest executable object.
/// Motivation:
///  * Prepare something heavy once before main execution loop instead of doing it for each columns.
///  * Provide const interface for IFunctionBase (later).
///  * Create one executable function per thread to use caches without synchronization (later).
class IExecutableFunction
{
public:

    virtual ~IExecutableFunction() = default;

    /// Get the main function name.
    virtual String getName() const = 0;

    ColumnPtr execute(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count, bool dry_run) const;

protected:

    virtual ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const = 0;

    virtual ColumnPtr executeDryRunImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const
    {
        return executeImpl(arguments, result_type, input_rows_count);
    }

    /** Default implementation in presence of Nullable arguments or NULL constants as arguments is the following:
      *  if some of arguments are NULL constants then return NULL constant,
      *  if some of arguments are Nullable, then execute function as usual for columns,
      *   where Nullable columns are substituted with nested columns (they have arbitrary values in rows corresponding to NULL value)
      *   and wrap result in Nullable column where NULLs are in all rows where any of arguments are NULL.
      */
    virtual bool useDefaultImplementationForNulls() const { return true; }

    /** Default implementation in presence of arguments with type Nothing is the following:
      *  If some of arguments have type Nothing then default implementation is to return constant column with type Nothing
      */
    virtual bool useDefaultImplementationForNothing() const { return true; }

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

    /** If function arguments has single sparse column and all other arguments are constants, call function on nested column.
      * Otherwise, convert all sparse columns to ordinary columns.
      * If default value doesn't change after function execution, returns sparse column as a result.
      * Otherwise, result column is converted to full.
      */
    virtual bool useDefaultImplementationForSparseColumns() const { return true; }

    /** Some arguments could remain constant during this implementation.
      */
    virtual ColumnNumbers getArgumentsThatAreAlwaysConstant() const { return {}; }

    /** True if function can be called on default arguments (include Nullable's) and won't throw.
      * Counterexample: modulo(0, 0)
      */
    virtual bool canBeExecutedOnDefaultArguments() const { return true; }

private:

    ColumnPtr defaultImplementationForConstantArguments(
            const ColumnsWithTypeAndName & args, const DataTypePtr & result_type, size_t input_rows_count, bool dry_run) const;

    ColumnPtr defaultImplementationForNulls(
            const ColumnsWithTypeAndName & args, const DataTypePtr & result_type, size_t input_rows_count, bool dry_run) const;

    ColumnPtr defaultImplementationForNothing(
            const ColumnsWithTypeAndName & args, const DataTypePtr & result_type, size_t input_rows_count) const;

    ColumnPtr executeWithoutLowCardinalityColumns(
            const ColumnsWithTypeAndName & args, const DataTypePtr & result_type, size_t input_rows_count, bool dry_run) const;

    ColumnPtr executeWithoutSparseColumns(
            const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count, bool dry_run) const;
};

using ExecutableFunctionPtr = std::shared_ptr<IExecutableFunction>;

/** Function with known arguments and return type (when the specific overload was chosen).
  * It is also the point where all function-specific properties are known.
  */
class IFunctionBase : public IResolvedFunction
{
public:

    ~IFunctionBase() override = default;

    virtual ColumnPtr execute( /// NOLINT
        const ColumnsWithTypeAndName & arguments,
        const DataTypePtr & result_type,
        size_t input_rows_count,
        bool dry_run = false) const
    {
        checkFunctionArgumentSizes(arguments, input_rows_count);
        return prepare(arguments)->execute(arguments, result_type, input_rows_count, dry_run);
    }

    /// Get the main function name.
    virtual String getName() const = 0;

    const Array & getParameters() const final
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "IFunctionBase doesn't support getParameters method");
    }

    /// Do preparations and return executable.
    /// sample_columns should contain data types of arguments and values of constants, if relevant.
    virtual ExecutableFunctionPtr prepare(const ColumnsWithTypeAndName & arguments) const = 0;

#if USE_EMBEDDED_COMPILER

    virtual bool isCompilable() const { return false; }

    /** Produce LLVM IR code that operates on scalar values. See `toNativeType` in DataTypes/Native.h
      * for supported value types and how they map to LLVM types.
      *
      * NOTE: the builder is actually guaranteed to be exactly `llvm::IRBuilder<>`, so you may safely
      *       downcast it to that type. This method is specified with `IRBuilderBase` because forward-declaring
      *       templates with default arguments is impossible and including LLVM in such a generic header
      *       as this one is a major pain.
      */
    virtual llvm::Value * compile(llvm::IRBuilderBase & /*builder*/, const ValuesWithType & /*arguments*/) const
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} is not JIT-compilable", getName());
    }

#endif

    virtual bool isStateful() const { return false; }

    /** Should we evaluate this function while constant folding, if arguments are constants?
      * Usually this is true. Notable counterexample is function 'sleep'.
      * If we will call it during query analysis, we will sleep extra amount of time.
      */
    virtual bool isSuitableForConstantFolding() const { return true; }

    /** If function isSuitableForConstantFolding then, this method will be called during query analysis
      * if some arguments are constants. For example logical functions (AndFunction, OrFunction) can
      * return they result based on some constant arguments.
      * Arguments are passed without modifications, useDefaultImplementationForNulls, useDefaultImplementationForNothing,
      * useDefaultImplementationForConstants, useDefaultImplementationForLowCardinality are not applied.
      */
    virtual ColumnPtr getConstantResultForNonConstArguments(
        const ColumnsWithTypeAndName & /* arguments */, const DataTypePtr & /* result_type */) const { return nullptr; }

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
      * sample_columns should contain data types of arguments and values of constants, if relevant.
      * NOTE: to check is function injective with any arguments, you can pass
      *       empty columns as sample_columns (since most of the time function will
      *       ignore it anyway, and creating arguments just for checking is
      *       function injective or not is overkill).
      */
    virtual bool isInjective(const ColumnsWithTypeAndName & /*sample_columns*/) const { return false; }

    /** Function is called "deterministic", if it returns same result for same values of arguments.
      * Most of functions are deterministic. Notable counterexample is rand().
      * Sometimes, functions are "deterministic" in scope of single query
      *  (even for distributed query), but not deterministic it general.
      * Example: now(). Another example: functions that work with periodically updated dictionaries.
      */

    virtual bool isDeterministic() const { return true; }

    virtual bool isDeterministicInScopeOfQuery() const { return true; }

    /** This is a special flags for functions which return constant value for the server,
      * but the result could be different for different servers in distributed query.
      *
      * This functions can't support constant folding on the initiator, but can on the follower.
      * We can't apply some optimizations as well (e.g. can't remove constant result from GROUP BY key).
      * So, it is convenient to have a special flag for them.
      *
      * Examples are: "__getScalar" and every function from serverConstants.cpp
      */
    virtual bool isServerConstant() const { return false; }

    /** Lets you know if the function is monotonic in a range of values.
      * This is used to work with the index in a sorted chunk of data.
      * And allows to use the index not only when it is written, for example `date >= const`, but also, for example, `toMonth(date) >= 11`.
      * All this is considered only for functions of one argument.
      */
    virtual bool hasInformationAboutMonotonicity() const { return false; }

    /** Lets you know if the function has its definition of preimage.
      * This is used to work with predicate optimizations, where the comparison between
      * f(x) and a constant c could be converted to the comparison between x and f's preimage [b, e).
      */
    virtual bool hasInformationAboutPreimage() const { return false; }

    struct ShortCircuitSettings
    {
        /// Should we enable lazy execution for the nth argument of short-circuit function?
        /// Example 1st argument: if(cond, then, else), we don't need to execute cond lazily.
        /// Example other arguments: 1st, 2nd, 3rd argument of dictGetOrDefault should always be calculated.
        std::unordered_set<size_t> arguments_with_disabled_lazy_execution;

        /// Should we enable lazy execution for functions, that are common descendants of
        /// different short-circuit function arguments?
        /// Example 1: if (cond, expr1(..., expr, ...), expr2(..., expr, ...)), we don't need
        /// to execute expr lazily, because it's used in both branches.
        /// Example 2: and(expr1, expr2(..., expr, ...), expr3(..., expr, ...)), here we
        /// should enable lazy execution for expr, because it must be filtered by expr1.
        bool enable_lazy_execution_for_common_descendants_of_arguments;
        /// Should we enable lazy execution without checking isSuitableForShortCircuitArgumentsExecution?
        /// Example: toTypeName(expr), even if expr contains functions that are not suitable for
        /// lazy execution (because of their simplicity), we shouldn't execute them at all.
        bool force_enable_lazy_execution;
    };

    /** Function is called "short-circuit" if it's arguments can be evaluated lazily
      * (examples: and, or, if, multiIf). If function is short circuit, it should be
      *  able to work with lazy executed arguments,
      *  this method will be called before function execution.
      *  If function is short circuit, it must define all fields in settings for
      *  appropriate preparations. Number of arguments is provided because some settings might depend on it.
      *  Example: multiIf(cond, else, then) and multiIf(cond1, else1, cond2, else2, ...), the first
      *  version can enable enable_lazy_execution_for_common_descendants_of_arguments setting, the second - not.
      */
    virtual bool isShortCircuit(ShortCircuitSettings & /*settings*/, size_t /*number_of_arguments*/) const { return false; }

    /** Should we evaluate this function lazily in short-circuit function arguments?
      * If function can throw an exception or it's computationally heavy, then
      * it's suitable, otherwise it's not (due to the overhead of lazy execution).
      * Suitability may depend on function arguments.
      */
    virtual bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const = 0;

    /// The property of monotonicity for a certain range.
    struct Monotonicity
    {
        bool is_monotonic = false;   /// Is the function monotonous (non-decreasing or non-increasing).
        bool is_positive = true;     /// true if the function is non-decreasing, false if non-increasing. If is_monotonic = false, then it does not matter.
        bool is_always_monotonic = false; /// Is true if function is monotonic on the whole input range I
        bool is_strict = false;      /// true if the function is strictly decreasing or increasing.
    };

    /** Get information about monotonicity on a range of values. Call only if hasInformationAboutMonotonicity.
      * NULL can be passed as one of the arguments. This means that the corresponding range is unlimited on the left or on the right.
      */
    virtual Monotonicity getMonotonicityForRange(const IDataType & /*type*/, const Field & /*left*/, const Field & /*right*/) const
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Function {} has no information about its monotonicity", getName());
    }

    /** Get the preimage of a function in the form of a left-closed and right-open interval. Call only if hasInformationAboutPreimage.
      * std::nullopt might be returned if the point (a single value) is invalid for this function.
      */
    virtual OptionalFieldInterval getPreimage(const IDataType & /*type*/, const Field & /*point*/) const
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Function {} has no information about its preimage", getName());
    }

};

using FunctionBasePtr = std::shared_ptr<const IFunctionBase>;


/** Creates IFunctionBase from argument types list (chooses one function overload).
  */
class IFunctionOverloadResolver : public std::enable_shared_from_this<IFunctionOverloadResolver>
{
public:
    virtual ~IFunctionOverloadResolver() = default;

    virtual FunctionBasePtr build(const ColumnsWithTypeAndName & arguments) const;

    void getLambdaArgumentTypes(DataTypes & arguments) const;

    void checkNumberOfArguments(size_t number_of_arguments) const;

    /// Get the main function name.
    virtual String getName() const = 0;

    /// For non-variadic functions, return number of arguments; otherwise return zero (that should be ignored).
    virtual size_t getNumberOfArguments() const = 0;

    /// TODO: This method should not be duplicated here and in IFunctionBase
    /// See the comment for the same method in IFunctionBase
    virtual bool isDeterministic() const { return true; }
    virtual bool isDeterministicInScopeOfQuery() const { return true; }
    virtual bool isInjective(const ColumnsWithTypeAndName &) const { return false; }
    virtual bool isServerConstant() const { return false; }
    virtual bool isShortCircuit(IFunctionBase::ShortCircuitSettings & /*settings*/, size_t /*number_of_arguments*/) const { return false; }

    /// Override and return true if function needs to depend on the state of the data.
    virtual bool isStateful() const { return false; }

    /// Override and return true if function could take different number of arguments.
    virtual bool isVariadic() const { return false; }

    /// For non-variadic functions, return number of arguments; otherwise return zero (that should be ignored).
    /// For higher-order functions (functions, that have lambda expression as at least one argument).
    /// You pass data types with empty DataTypeFunction for lambda arguments.
    /// This function will replace it with DataTypeFunction containing actual types.
    virtual void getLambdaArgumentTypesImpl(DataTypes & arguments [[maybe_unused]]) const
    {
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function {} can't have lambda-expressions as arguments", getName());
    }

    /// Returns indexes of arguments, that must be ColumnConst
    virtual ColumnNumbers getArgumentsThatAreAlwaysConstant() const { return {}; }

    /// Returns indexes if arguments, that can be Nullable without making result of function Nullable
    /// (for functions like isNull(x))
    virtual ColumnNumbers getArgumentsThatDontImplyNullableReturnType(size_t number_of_arguments [[maybe_unused]]) const { return {}; }

    /// Returns type that should be used as the result type in default implementation for Dynamic.
    /// Function should implement this method if its result type doesn't depend on the arguments types.
    virtual DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const { return nullptr; }

protected:

    virtual FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & /* arguments */, const DataTypePtr & /* result_type */) const
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "buildImpl is not implemented for {}", getName());
    }

    virtual DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "getReturnType is not implemented for {}", getName());
    }

    /// This function will be called in default implementation. You can overload it or the previous one.
    virtual DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const
    {
        DataTypes data_types(arguments.size());
        for (size_t i = 0; i < arguments.size(); ++i)
            data_types[i] = arguments[i].type;

        return getReturnTypeImpl(data_types);
    }

    /** If useDefaultImplementationForNulls() is true, then change arguments for getReturnType() and build():
      *  if some of arguments are Nullable(Nothing) then don't call getReturnType(), call build() with return_type = Nullable(Nothing),
      *  if some of arguments are Nullable, then:
      *   - Nullable types are substituted with nested types for getReturnType() function
      *   - wrap getReturnType() result in Nullable type and pass to build
      *
      * Otherwise build returns build(arguments, getReturnType(arguments));
      */
    virtual bool useDefaultImplementationForNulls() const { return true; }

    /** If useDefaultImplementationForNothing() is true, then change arguments for getReturnType() and build():
      *  if some of arguments are Nothing then don't call getReturnType(), call build() with return_type = Nothing,
      * Otherwise build returns build(arguments, getReturnType(arguments));
      */
    virtual bool useDefaultImplementationForNothing() const { return true; }

    /** If useDefaultImplementationForLowCardinalityColumns() is true, then change arguments for getReturnType() and build().
      * If function arguments has low cardinality types, convert them to ordinary types.
      * getReturnType returns ColumnLowCardinality if at least one argument type is ColumnLowCardinality.
      */
    virtual bool useDefaultImplementationForLowCardinalityColumns() const { return true; }

    /** If function arguments has single sparse column and all other arguments are constants, call function on nested column.
      * Otherwise, convert all sparse columns to ordinary columns.
      * If default value doesn't change after function execution, returns sparse column as a result.
      * Otherwise, result column is converted to full.
      */
    virtual bool useDefaultImplementationForSparseColumns() const { return true; }

    /// If it isn't, will convert all ColumnLowCardinality arguments to full columns.
    virtual bool canBeExecutedOnLowCardinalityDictionary() const { return true; }

    /** If useDefaultImplementationForDynamic() is true, then special FunctionBaseDynamicAdaptor will be used
     *  if function arguments has Dynamic column. This adaptor will build and execute this function for all
     *  internal types inside Dynamic column separately and construct result based on results for these types.
     *  If getReturnTypeForDefaultImplementationForDynamic() returns T, then result of such function
     *  will be Nullable(T), otherwise the result will be Dynamic.
     *
     *  We cannot use default implementation for Dynamic if function doesn't use default implementation for NULLs,
     *  because Dynamic column can contain NULLs and we should know how to process them.
      */
    virtual bool useDefaultImplementationForDynamic() const { return useDefaultImplementationForNulls(); }

private:

    DataTypePtr getReturnType(const ColumnsWithTypeAndName & arguments) const;

    DataTypePtr getReturnTypeWithoutLowCardinality(const ColumnsWithTypeAndName & arguments) const;
};

using FunctionOverloadResolverPtr = std::shared_ptr<IFunctionOverloadResolver>;

/// Old function interface. Check documentation in IFunction.h.
/// If client do not need stateful properties it can implement this interface.
class IFunction
{
public:

    virtual ~IFunction() = default;

    virtual String getName() const = 0;

    virtual ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const = 0;
    virtual ColumnPtr executeImplDryRun(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const
    {
        return executeImpl(arguments, result_type, input_rows_count);
    }

    /** Default implementation in presence of Nullable arguments or NULL constants as arguments is the following:
      *  if some of arguments are NULL constants then return NULL constant,
      *  if some of arguments are Nullable, then execute function as usual for columns,
      *   where Nullable columns are substituted with nested columns (they have arbitrary values in rows corresponding to NULL value)
      *   and wrap result in Nullable column where NULLs are in all rows where any of arguments are NULL.
      */
    virtual bool useDefaultImplementationForNulls() const { return true; }

    /** Default implementation in presence of arguments with type Nothing is the following:
      *  If some of arguments have type Nothing then default implementation is to return constant column with type Nothing
      */
    virtual bool useDefaultImplementationForNothing() const { return true; }

    /** If the function have non-zero number of arguments,
      *  and if all arguments are constant, that we could automatically provide default implementation:
      *  arguments are converted to ordinary columns with single value, then function is executed as usual,
      *  and then the result is converted to constant column.
      */
    virtual bool useDefaultImplementationForConstants() const { return false; }

    /** Some arguments could remain constant during this implementation.
      */
    virtual ColumnNumbers getArgumentsThatAreAlwaysConstant() const { return {}; }

    /** If function arguments has single low cardinality column and all other arguments are constants, call function on nested column.
      * Otherwise, convert all low cardinality columns to ordinary columns.
      * Returns ColumnLowCardinality if at least one argument is ColumnLowCardinality.
      */
    virtual bool useDefaultImplementationForLowCardinalityColumns() const { return true; }

    /** If function arguments has single sparse column and all other arguments are constants, call function on nested column.
      * Otherwise, convert all sparse columns to ordinary columns.
      * If default value doesn't change after function execution, returns sparse column as a result.
      * Otherwise, result column is converted to full.
      */
    virtual bool useDefaultImplementationForSparseColumns() const { return true; }

    /// If it isn't, will convert all ColumnLowCardinality arguments to full columns.
    virtual bool canBeExecutedOnLowCardinalityDictionary() const { return true; }

    virtual bool useDefaultImplementationForDynamic() const { return useDefaultImplementationForNulls(); }
    virtual DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const { return nullptr; }

    /** True if function can be called on default arguments (include Nullable's) and won't throw.
      * Counterexample: modulo(0, 0)
      */
    virtual bool canBeExecutedOnDefaultArguments() const { return true; }

    /// Properties from IFunctionBase (see IFunction.h)
    virtual bool isSuitableForConstantFolding() const { return true; }
    virtual ColumnPtr getConstantResultForNonConstArguments(const ColumnsWithTypeAndName & /*arguments*/, const DataTypePtr & /*result_type*/) const { return nullptr; }
    virtual bool isInjective(const ColumnsWithTypeAndName & /*sample_columns*/) const { return false; }
    virtual bool isDeterministic() const { return true; }
    virtual bool isDeterministicInScopeOfQuery() const { return true; }
    virtual bool isServerConstant() const { return false; }
    virtual bool isStateful() const { return false; }

    using ShortCircuitSettings = IFunctionBase::ShortCircuitSettings;
    virtual bool isShortCircuit(ShortCircuitSettings & /*settings*/, size_t /*number_of_arguments*/) const { return false; }
    virtual bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const = 0;

    virtual bool hasInformationAboutMonotonicity() const { return false; }
    virtual bool hasInformationAboutPreimage() const { return false; }

    using Monotonicity = IFunctionBase::Monotonicity;
    virtual Monotonicity getMonotonicityForRange(const IDataType & /*type*/, const Field & /*left*/, const Field & /*right*/) const
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Function {} has no information about its monotonicity", getName());
    }
    virtual OptionalFieldInterval getPreimage(const IDataType & /*type*/, const Field & /*point*/) const
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Function {} has no information about its preimage", getName());
    }

    /// For non-variadic functions, return number of arguments; otherwise return zero (that should be ignored).
    virtual size_t getNumberOfArguments() const = 0;

    virtual DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "getReturnType is not implemented for {}", getName());
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
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function {} can't have lambda-expressions as arguments", getName());
    }

    virtual ColumnNumbers getArgumentsThatDontImplyNullableReturnType(size_t /*number_of_arguments*/) const { return {}; }


#if USE_EMBEDDED_COMPILER

    bool isCompilable(const DataTypes & arguments, const DataTypePtr & result_type) const;

    llvm::Value * compile(llvm::IRBuilderBase & builder, const ValuesWithType & arguments, const DataTypePtr & result_type) const;

#endif

protected:

#if USE_EMBEDDED_COMPILER

    virtual bool isCompilableImpl(const DataTypes & /*arguments*/, const DataTypePtr & /*result_type*/) const { return false; }

    virtual llvm::Value * compileImpl(llvm::IRBuilderBase & /*builder*/, const ValuesWithType & /*arguments*/, const DataTypePtr & /*result_type*/) const
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} is not JIT-compilable", getName());
    }

#endif
};

using FunctionPtr = std::shared_ptr<IFunction>;

}
