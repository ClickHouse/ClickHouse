#pragma once

#include <memory>
#include <Common/VectorWithMemoryTracking.h>
#include <DataTypes/IDataType.h>
#include <AggregateFunctions/IAggregateFunction.h>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

/// Where an aggregate function combinator converts the `Variant` arguments that it passes as is to the nested function,
/// if the nested function does not support them.
enum class VariantArgumentsOfNestedFunction : UInt8
{
    /// The adapter is applied to the nested function, and the combinator sees the original values (example: -Distinct).
    ConvertedByNested,
    /// Same as `ConvertedByNested`, but an outer combinator with `ConvertedAround` converts them around itself as well.
    /// The combinator must pass the arguments to the nested function at the same positions (example: -If, -State).
    Forwarded,
    /// The adapter is applied around the combinator, so that the rows with NULL values are skipped before the combinator
    /// processes them, as the `Null` combinator does for `Nullable` arguments. Otherwise, the result would differ
    /// from the result for the arguments explicitly cast to `Nullable`: for example, -ArgMax would select the row
    /// with a NULL value, and -OrDefault and -Resample would return NULL instead of the default value for empty states.
    /// It is also used by combinators that pass the arguments to the nested function row by row or element by element,
    /// so that the conversion is done once for the whole block (example: -SimpleState, -ForEach, -Map).
    /// The combinator must pass the arguments to the nested function at the same positions (example: -ArgMax, -OrDefault),
    /// or override `convertVariantArgumentsAround` (example: -ForEach).
    ConvertedAround,
};

/** Aggregate function combinator allows to take one aggregate function
  *  and transform it to another aggregate function.
  *
  * In SQL language they are used as suffixes for existing aggregate functions.
  *
  * Example: -If combinator takes an aggregate function and transforms it
  *  to aggregate function with additional argument at end (condition),
  *  that will pass values to original aggregate function when the condition is true.
  *
  * More examples:
  *
  * sum(x) - calculate sum of x
  * sumIf(x, cond) - calculate sum of x for rows where condition is true.
  * sumArray(arr) - calculate sum of all elements of arrays.
  *
  * PS. Please don't mess it with so called "combiner" - totally unrelated notion from Hadoop world.
  * "combining" - merging the states of aggregate functions - is supported naturally in ClickHouse.
  */

class IAggregateFunctionCombinator
{
public:
    virtual String getName() const = 0;

    virtual bool isForInternalUsageOnly() const { return false; }

    /** Does combinator supports nesting (of itself, i.e. ArrayArray or IfIf)
     */
    virtual bool supportsNesting() const { return false; }

    /** Does this combinator transform argument types (like Merge, Array) rather than just adding logic?
      *
      * If true, the combinator expects arguments to be of specific types (e.g., Merge expects AggregateFunction type)
      * and the asterisk in count(*) should NOT be removed because it refers to an actual column.
      *
      * If false (default), the combinator preserves the original aggregate function semantics,
      * and asterisk in count(*) can be safely removed.
      */
    virtual bool transformsArgumentTypes() const { return false; }

    /** The positions of the `Variant` arguments that the combinator itself does not support (ex: the key of -ArgMin).
      *  They are converted by `AggregateFunctionVariantAdapter`, see `VariantArgumentsConversion`.
      */
    virtual VectorWithMemoryTracking<size_t> getVariantArgumentsToConvert(const DataTypes & /*arguments*/) const { return {}; }

    /** How the `Variant` arguments that are passed as is to the nested function are converted
      *  if the nested function does not support them, see `VariantArgumentsOfNestedFunction`.
      */
    virtual VariantArgumentsOfNestedFunction getVariantArgumentsOfNestedFunction() const { return VariantArgumentsOfNestedFunction::ConvertedByNested; }

    /** Converts the arguments of the combined function (ex: `Array(Variant(UInt8, UInt16))` for -ForEach),
      *  so that `transformArguments` returns `converted_nested_arguments` (ex: `Nullable(UInt16)`) instead of `nested_arguments`.
      *  They differ only by the converted `Variant` arguments. By default, the arguments are passed at the same positions.
      */
    virtual void convertVariantArgumentsAround(
        DataTypes & arguments, const DataTypes & nested_arguments, const DataTypes & converted_nested_arguments) const
    {
        for (size_t i = 0; i < nested_arguments.size(); ++i)
        {
            if (converted_nested_arguments[i]->equals(*nested_arguments[i]))
                continue;
            chassert(i < arguments.size() && arguments[i]->equals(*nested_arguments[i]));
            arguments[i] = converted_nested_arguments[i];
        }
    }

    /** From the arguments for combined function (ex: UInt64, UInt8 for sumIf),
      *  get the arguments for nested function (ex: UInt64 for sum).
      * If arguments are not suitable for combined function, throw an exception.
      */
    virtual DataTypes transformArguments(const DataTypes & arguments) const
    {
        return arguments;
    }

    /** From the parameters for combined function,
      *  get the parameters for nested function.
      * If arguments are not suitable for combined function, throw an exception.
      */
    virtual Array transformParameters(const Array & parameters) const
    {
        return parameters;
    }

    /** Create combined aggregate function (ex: sumIf)
      *  from nested function (ex: sum)
      *  and arguments for combined aggregate function (ex: UInt64, UInt8 for sumIf).
      * It's assumed that function transformArguments was called before this function and 'arguments' are validated.
      * Every combinator overrides this method, except combinators with `transformsMultipleNestedFunctions`,
      * which override `transformAggregateFunctionFromMultipleNestedFunctions` instead.
      */
    virtual AggregateFunctionPtr transformAggregateFunction(
        const AggregateFunctionPtr & /*nested_function*/,
        const AggregateFunctionProperties & /*properties*/,
        const DataTypes & /*arguments*/,
        const Array & /*params*/) const
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "Aggregate function combinator '{}' does not wrap a single nested function", getName());
    }

    /** A combinator that wraps one nested function per argument list returned by
      * `transformArgumentsForMultipleNestedFunctions` (for example, -Tuple: one nested function per tuple element).
      */
    virtual bool transformsMultipleNestedFunctions() const { return false; }

    /** From the arguments for the combined function, get one argument list per nested function.
      * If the arguments are not suitable for the combined function, throw an exception.
      */
    virtual VectorWithMemoryTracking<DataTypes> transformArgumentsForMultipleNestedFunctions(const DataTypes & /*arguments*/) const
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "Aggregate function combinator '{}' does not transform multiple nested functions", getName());
    }

    /** Create the combined aggregate function from the resolved nested functions, one per argument
      * list returned by `transformArgumentsForMultipleNestedFunctions`. `nested_name` is the canonical name of the nested
      * function (aliases resolved).
      */
    virtual AggregateFunctionPtr transformAggregateFunctionFromMultipleNestedFunctions(
        const String & /*nested_name*/,
        VectorWithMemoryTracking<AggregateFunctionPtr> /*nested_functions*/,
        const AggregateFunctionProperties & /*properties*/,
        const DataTypes & /*arguments*/,
        const Array & /*params*/) const
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "Aggregate function combinator '{}' does not transform multiple nested functions", getName());
    }

    virtual ~IAggregateFunctionCombinator() = default;
};

using AggregateFunctionCombinatorPtr = std::shared_ptr<const IAggregateFunctionCombinator>;

}
