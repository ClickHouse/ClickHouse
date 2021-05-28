#pragma once

#include <memory>

#include <Core/Names.h>
#include <Core/Block.h>
#include <Core/ColumnNumbers.h>
#include <DataTypes/IDataType.h>

#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

/// This file contains user interface for functions.
/// For developer interface (in case you need to implement a new function) see IFunctionImpl.h

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
}

class Field;

/// The simplest executable object.
/// Motivation:
///  * Prepare something heavy once before main execution loop instead of doing it for each block.
///  * Provide const interface for IFunctionBase (later).
///  * Create one executable function per thread to use caches without synchronization (later).
class IExecutableFunction
{
public:
    virtual ~IExecutableFunction() = default;

    /// Get the main function name.
    virtual String getName() const = 0;

    virtual void execute(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count, bool dry_run) = 0;

    virtual void createLowCardinalityResultCache(size_t cache_size) = 0;
};

using ExecutableFunctionPtr = std::shared_ptr<IExecutableFunction>;


using ValuePlaceholders = std::vector<std::function<llvm::Value * ()>>;

/// Function with known arguments and return type (when the specific overload was chosen).
/// It is also the point where all function-specific properties are known.
class IFunctionBase
{
public:
    virtual ~IFunctionBase() = default;

    /// Get the main function name.
    virtual String getName() const = 0;

    virtual const DataTypes & getArgumentTypes() const = 0;
    virtual const DataTypePtr & getReturnType() const = 0;

    /// Do preparations and return executable.
    /// sample_block should contain data types of arguments and values of constants, if relevant.
    virtual ExecutableFunctionPtr prepare(const Block & sample_block, const ColumnNumbers & arguments, size_t result) const = 0;

    /// TODO: make const
    virtual void execute(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count, bool dry_run = false)
    {
        return prepare(block, arguments, result)->execute(block, arguments, result, input_rows_count, dry_run);
    }

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
    virtual llvm::Value * compile(llvm::IRBuilderBase & /*builder*/, ValuePlaceholders /*values*/) const
    {
        throw Exception(getName() + " is not JIT-compilable", ErrorCodes::NOT_IMPLEMENTED);
    }

#endif

    virtual bool isStateful() const { return false; }

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
      * NOTE: to check is function injective with any arguments, you can pass
      *       empty block as sample_block (since most of the time function will
      *       ignore it anyway, and creating arguments just for checking is
      *       function injective or not is overkill).
      */
    virtual bool isInjective(const Block & /*sample_block*/) const { return false; }

    /** Function is called "deterministic", if it returns same result for same values of arguments.
      * Most of functions are deterministic. Notable counterexample is rand().
      * Sometimes, functions are "deterministic" in scope of single query
      *  (even for distributed query), but not deterministic it general.
      * Example: now(). Another example: functions that work with periodically updated dictionaries.
      */

    virtual bool isDeterministic() const = 0;

    virtual bool isDeterministicInScopeOfQuery() const = 0;

    /** Lets you know if the function is monotonic in a range of values.
      * This is used to work with the index in a sorted chunk of data.
      * And allows to use the index not only when it is written, for example `date >= const`, but also, for example, `toMonth(date) >= 11`.
      * All this is considered only for functions of one argument.
      */
    virtual bool hasInformationAboutMonotonicity() const { return false; }

    /// The property of monotonicity for a certain range.
    struct Monotonicity
    {
        bool is_monotonic = false;    /// Is the function monotonous (nondecreasing or nonincreasing).
        bool is_positive = true;    /// true if the function is nondecreasing, false, if notincreasing. If is_monotonic = false, then it does not matter.
        bool is_always_monotonic = false; /// Is true if function is monotonic on the whole input range I

        Monotonicity(bool is_monotonic_ = false, bool is_positive_ = true, bool is_always_monotonic_ = false)
                : is_monotonic(is_monotonic_), is_positive(is_positive_), is_always_monotonic(is_always_monotonic_) {}
    };

    /** Get information about monotonicity on a range of values. Call only if hasInformationAboutMonotonicity.
      * NULL can be passed as one of the arguments. This means that the corresponding range is unlimited on the left or on the right.
      */
    virtual Monotonicity getMonotonicityForRange(const IDataType & /*type*/, const Field & /*left*/, const Field & /*right*/) const
    {
        throw Exception("Function " + getName() + " has no information about its monotonicity.", ErrorCodes::NOT_IMPLEMENTED);
    }
};

using FunctionBasePtr = std::shared_ptr<IFunctionBase>;


/// Creates IFunctionBase from argument types list (chooses one function overload).
class IFunctionOverloadResolver
{
public:
    virtual ~IFunctionOverloadResolver() = default;

    /// Get the main function name.
    virtual String getName() const = 0;

    /// See the comment for the same method in IFunctionBase
    virtual bool isDeterministic() const = 0;
    virtual bool isDeterministicInScopeOfQuery() const = 0;
    virtual bool isInjective(const Block &) const = 0;

    /// Override and return true if function needs to depend on the state of the data.
    virtual bool isStateful() const = 0;

    /// Override and return true if function could take different number of arguments.
    virtual bool isVariadic() const = 0;

    /// For non-variadic functions, return number of arguments; otherwise return zero (that should be ignored).
    virtual size_t getNumberOfArguments() const = 0;

    /// Throw if number of arguments is incorrect.
    virtual void checkNumberOfArguments(size_t number_of_arguments) const = 0;

    /// Check if arguments are correct and returns IFunctionBase.
    virtual FunctionBasePtr build(const ColumnsWithTypeAndName & arguments) const = 0;

    /// For higher-order functions (functions, that have lambda expression as at least one argument).
    /// You pass data types with empty DataTypeFunction for lambda arguments.
    /// This function will replace it with DataTypeFunction containing actual types.
    virtual void getLambdaArgumentTypes(DataTypes & arguments) const = 0;

    /// Returns indexes of arguments, that must be ColumnConst
    virtual ColumnNumbers getArgumentsThatAreAlwaysConstant() const = 0;
    /// Returns indexes if arguments, that can be Nullable without making result of function Nullable
    /// (for functions like isNull(x))
    virtual ColumnNumbers getArgumentsThatDontImplyNullableReturnType(size_t number_of_arguments) const = 0;
};

using FunctionOverloadResolverPtr = std::shared_ptr<IFunctionOverloadResolver>;


/** Return ColumnNullable of src, with null map as OR-ed null maps of args columns in blocks.
  * Or ColumnConst(ColumnNullable) if the result is always NULL or if the result is constant and always not NULL.
  */
ColumnPtr wrapInNullable(const ColumnPtr & src, const Block & block, const ColumnNumbers & args, size_t result, size_t input_rows_count);

}
