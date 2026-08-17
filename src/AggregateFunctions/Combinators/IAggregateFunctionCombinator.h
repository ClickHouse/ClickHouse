#pragma once

#include <memory>
#include <DataTypes/IDataType.h>
#include <AggregateFunctions/IAggregateFunction.h>


namespace DB
{
struct Settings;

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
      */
    virtual AggregateFunctionPtr transformAggregateFunction(
        const AggregateFunctionPtr & nested_function,
        const AggregateFunctionProperties & properties,
        const DataTypes & arguments,
        const Array & params) const = 0;

    virtual ~IAggregateFunctionCombinator() = default;
};

using AggregateFunctionCombinatorPtr = std::shared_ptr<const IAggregateFunctionCombinator>;

}
