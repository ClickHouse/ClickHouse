#pragma once

#include <IO/WriteHelpers.h>
#include <AggregateFunctions/IAggregateFunction.h>


namespace DB
{

/** Interface for aggregate functions taking zero number of arguments. For example, this is 'count' aggregate function.
  */
template <typename T, typename Derived>
class INullaryAggregateFunction : public IAggregateFunctionHelper<T>
{
private:
    Derived & getDerived() { return static_cast<Derived &>(*this); }
    const Derived & getDerived() const { return static_cast<const Derived &>(*this); }

public:
    /// By default, checks that number of arguments is zero. You could override if you would like to allow to have ignored arguments.
    void setArguments(const DataTypes & arguments) override
    {
        if (arguments.size() != 0)
            throw Exception("Passed " + toString(arguments.size()) + " arguments to nullary aggregate function " + this->getName(),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    }

    /// Accumulate a value.
    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override final
    {
        getDerived().addImpl(place);
    }

    static void addFree(const IAggregateFunction * that, AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *)
    {
        return static_cast<const Derived &>(*that).addImpl(place);
    }

    IAggregateFunction::AddFunc getAddressOfAddFunction() const override final { return &addFree; }

    /** Implement the following in descendant class:
      * void addImpl(AggregateDataPtr place) const;
      */
};

}
