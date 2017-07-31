#pragma once

#include <memory>

#include <Core/Names.h>
#include <Core/Field.h>
#include <Core/Block.h>
#include <Core/ColumnNumbers.h>
#include <DataTypes/IDataType.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NOT_IMPLEMENTED;
}

struct ExpressionAction;


/** Interface for normal functions.
  * Normal functions are functions that do not change the number of rows in the table,
  *  and the result of which for each row does not depend on other rows.
  *
  * A function can take an arbitrary number of arguments; returns exactly one value.
  * The type of the result depends on the type and number of arguments.
  *
  * The function is dispatched for the whole block. This allows you to perform all kinds of checks rarely,
  *  and do the main job as an efficient loop.
  *
  * The function is applied to one or more columns of the block, and writes its result,
  *  adding a new column to the block. The function does not modify its arguments.
  */
class IFunction
{
public:
    /** The successor of IFunction must implement:
      * - getName
      * - either getReturnType, or getReturnTypeAndPrerequisites
      * - one of the overloads of `execute`.
      */

    /// Get the main function name.
    virtual String getName() const = 0;

    /// Override and return true if function could take different number of arguments.
    virtual bool isVariadic() const { return false; }

    /// For non-variadic functions, return number of arguments; otherwise return zero (that should be ignored).
    virtual size_t getNumberOfArguments() const = 0;

    /// Throw if number of arguments is incorrect. Default implementation will check only in non-variadic case.
    /// It is called inside getReturnType.
    virtual void checkNumberOfArguments(size_t number_of_arguments) const;

    /** Should we evaluate this function while constant folding, if arguments are constants?
      * Usually this is true. Notable counterexample is function 'sleep'.
      * If we will call it during query analysis, we will sleep extra amount of time.
      */
    virtual bool isSuitableForConstantFolding() const { return true; }

    /** Function is called "injective" if it returns different result for different values of arguments.
      * Example: hex, negate, tuple...
      *
      * Function could be injective with some arguments fixed to some constant values.
      * Examples:
      *  plus(const, x);
      *  multiply(const, x) where x is an integer and constant is not divisable by two;
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
    virtual bool isInjective(const Block & sample_block) { return false; }

    /** Function is called "deterministic", if it returns same result for same values of arguments.
      * Most of functions are deterministic. Notable counterexample is rand().
      * Sometimes, functions are "deterministic" in scope of single query
      *  (even for distributed query), but not deterministic it general.
      * Example: now(). Another example: functions that work with periodically updated dictionaries.
      */
    virtual bool isDeterministicInScopeOfQuery() { return true; }

    /// Get the result type by argument type. If the function does not apply to these arguments, throw an exception.
    /// Overloading for those who do not need prerequisites and values of constant arguments. Not called from outside.
    DataTypePtr getReturnType(const DataTypes & arguments) const;

    virtual DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const
    {
        throw Exception("getReturnType is not implemented for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    /** Get the result type by argument types and constant argument values.
      * If the function does not apply to these arguments, throw an exception.
      * You can also return a description of the additional columns that are required to perform the function.
      * For non-constant columns `arguments[i].column = nullptr`.
      * Meaningful element types in out_prerequisites: APPLY_FUNCTION, ADD_COLUMN.
      */
    void getReturnTypeAndPrerequisites(
        const ColumnsWithTypeAndName & arguments,
        DataTypePtr & out_return_type,
        std::vector<ExpressionAction> & out_prerequisites);

    virtual void getReturnTypeAndPrerequisitesImpl(
        const ColumnsWithTypeAndName & arguments,
        DataTypePtr & out_return_type,
        std::vector<ExpressionAction> & out_prerequisites)
    {
        DataTypes types(arguments.size());
        for (size_t i = 0; i < arguments.size(); ++i)
            types[i] = arguments[i].type;
        out_return_type = getReturnTypeImpl(types);
    }

    /// For higher-order functions (functions, that have lambda expression as at least one argument).
    /// You pass data types with empty DataTypeExpression for lambda arguments.
    /// This function will replace it with DataTypeExpression containing actual types.
    void getLambdaArgumentTypes(DataTypes & arguments) const;

    virtual void getLambdaArgumentTypesImpl(DataTypes & arguments) const
    {
        throw Exception("Function " + getName() + " can't have lambda-expressions as arguments", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    /// Execute the function on the block. Note: can be called simultaneously from several threads, for one object.
    /// Overloading for those who do not need `prerequisites`. Not called from outside.
    void execute(Block & block, const ColumnNumbers & arguments, size_t result);

    /// Execute the function above the block. Note: can be called simultaneously from several threads, for one object.
    /// `prerequisites` go in the same order as `out_prerequisites` obtained from getReturnTypeAndPrerequisites.
    void execute(Block & block, const ColumnNumbers & arguments, const ColumnNumbers & prerequisites, size_t result);

    virtual void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result)
    {
        throw Exception("executeImpl is not implemented for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    virtual void executeImpl(Block & block, const ColumnNumbers & arguments, const ColumnNumbers & prerequisites, size_t result)
    {
        executeImpl(block, arguments, result);
    }

    /** Default implementation in presense of Nullable arguments or NULL constants as arguments is the following:
      *  if some of arguments are NULL constants then return NULL constant,
      *  if some of arguments are Nullable, then execute function as usual for block,
      *   where Nullable columns are substituted with nested columns (they have arbitary values in rows corresponding to NULL value)
      *   and wrap result in Nullable column where NULLs are in all rows where any of arguments are NULL.
      */
    virtual bool useDefaultImplementationForNulls() const { return true; }

    /** If the function have non-zero number of arguments,
      *  and if all arguments are constant, that we could automatically provide default implementation:
      *  arguments are converted to ordinary columns with single value, then function is executed as usual,
      *  and then the result is converted to constant column.
      */
    virtual bool useDefaultImplementationForConstants() const { return false; }

    /** Some arguments could remain constant during this implementation.
      */
    virtual ColumnNumbers getArgumentsThatAreAlwaysConstant() const { return {}; }


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
    virtual Monotonicity getMonotonicityForRange(const IDataType & type, const Field & left, const Field & right) const
    {
        throw Exception("Function " + getName() + " has no information about its monotonicity.", ErrorCodes::NOT_IMPLEMENTED);
    }

    virtual ~IFunction() {}
};


using FunctionPtr = std::shared_ptr<IFunction>;


}
