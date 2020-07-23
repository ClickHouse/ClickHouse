#pragma once

#include <cstddef>
#include <memory>
#include <vector>
#include <type_traits>

#include <Core/Types.h>
#include <Core/ColumnNumbers.h>
#include <Core/Block.h>
#include <Common/Exception.h>
#include <Core/Field.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

class Arena;
class ReadBuffer;
class WriteBuffer;
class IColumn;
class IDataType;

using DataTypePtr = std::shared_ptr<const IDataType>;
using DataTypes = std::vector<DataTypePtr>;

using AggregateDataPtr = char *;
using ConstAggregateDataPtr = const char *;

class IAggregateFunction;
using AggregateFunctionPtr = std::shared_ptr<IAggregateFunction>;

/** Aggregate functions interface.
  * Instances of classes with this interface do not contain the data itself for aggregation,
  *  but contain only metadata (description) of the aggregate function,
  *  as well as methods for creating, deleting and working with data.
  * The data resulting from the aggregation (intermediate computing states) is stored in other objects
  *  (which can be created in some memory pool),
  *  and IAggregateFunction is the external interface for manipulating them.
  */
class IAggregateFunction
{
public:
    IAggregateFunction(const DataTypes & argument_types_, const Array & parameters_)
        : argument_types(argument_types_), parameters(parameters_) {}

    /// Get main function name.
    virtual String getName() const = 0;

    /// Get the result type.
    virtual DataTypePtr getReturnType() const = 0;

    /// Get type which will be used for prediction result in case if function is an ML method.
    virtual DataTypePtr getReturnTypeToPredict() const
    {
        throw Exception("Prediction is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    virtual ~IAggregateFunction() {}

    /** Data manipulating functions. */

    /** Create empty data for aggregation with `placement new` at the specified location.
      * You will have to destroy them using the `destroy` method.
      */
    virtual void create(AggregateDataPtr place) const = 0;

    /// Delete data for aggregation.
    virtual void destroy(AggregateDataPtr place) const noexcept = 0;

    /// It is not necessary to delete data.
    virtual bool hasTrivialDestructor() const = 0;

    /// Get `sizeof` of structure with data.
    virtual size_t sizeOfData() const = 0;

    /// How the data structure should be aligned. NOTE: Currently not used (structures with aggregation state are put without alignment).
    virtual size_t alignOfData() const = 0;

    /** Adds a value into aggregation data on which place points to.
     *  columns points to columns containing arguments of aggregation function.
     *  row_num is number of row which should be added.
     *  Additional parameter arena should be used instead of standard memory allocator if the addition requires memory allocation.
     */
    virtual void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena * arena) const = 0;

    /// Merges state (on which place points to) with other state of current aggregation function.
    virtual void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena * arena) const = 0;

    /// Serializes state (to transmit it over the network, for example).
    virtual void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const = 0;

    /// Deserializes state. This function is called only for empty (just created) states.
    virtual void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena * arena) const = 0;

    /// Returns true if a function requires Arena to handle own states (see add(), merge(), deserialize()).
    virtual bool allocatesMemoryInArena() const
    {
        return false;
    }

    /// Inserts results into a column.
    /// This method must be called once, from single thread.
    /// After this method was called for state, you can't do anything with state but destroy.
    virtual void insertResultInto(AggregateDataPtr place, IColumn & to, Arena * arena) const = 0;

    /// Used for machine learning methods. Predict result from trained model.
    /// Will insert result into `to` column for rows in range [offset, offset + limit).
    virtual void predictValues(
        ConstAggregateDataPtr /* place */,
        IColumn & /*to*/,
        Block & /*block*/,
        size_t /*offset*/,
        size_t /*limit*/,
        const ColumnNumbers & /*arguments*/,
        const Context & /*context*/) const
    {
        throw Exception("Method predictValues is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    /** Returns true for aggregate functions of type -State
      * They are executed as other aggregate functions, but not finalized (return an aggregation state that can be combined with another).
      * Also returns true when the final value of this aggregate function contains State of other aggregate function inside.
      */
    virtual bool isState() const { return false; }

    /** The inner loop that uses the function pointer is better than using the virtual function.
      * The reason is that in the case of virtual functions GCC 5.1.2 generates code,
      *  which, at each iteration of the loop, reloads the function address (the offset value in the virtual function table) from memory to the register.
      * This gives a performance drop on simple queries around 12%.
      * After the appearance of better compilers, the code can be removed.
      */
    using AddFunc = void (*)(const IAggregateFunction *, AggregateDataPtr, const IColumn **, size_t, Arena *);
    virtual AddFunc getAddressOfAddFunction() const = 0;

    /** Contains a loop with calls to "add" function. You can collect arguments into array "places"
      *  and do a single call to "addBatch" for devirtualization and inlining.
      */
    virtual void addBatch(size_t batch_size, AggregateDataPtr * places, size_t place_offset, const IColumn ** columns, Arena * arena) const = 0;

    /** The same for single place.
      */
    virtual void addBatchSinglePlace(size_t batch_size, AggregateDataPtr place, const IColumn ** columns, Arena * arena) const = 0;

    /** The same for single place when need to aggregate only filtered data.
      */
    virtual void addBatchSinglePlaceNotNull(
        size_t batch_size, AggregateDataPtr place, const IColumn ** columns, const UInt8 * null_map, Arena * arena) const = 0;

    virtual void addBatchSinglePlaceFromInterval(size_t batch_begin, size_t batch_end, AggregateDataPtr place, const IColumn ** columns, Arena * arena) const = 0;

    /** In addition to addBatch, this method collects multiple rows of arguments into array "places"
      *  as long as they are between offsets[i-1] and offsets[i]. This is used for arrayReduce and
      *  -Array combinator. It might also be used generally to break data dependency when array
      *  "places" contains a large number of same values consecutively.
      */
    virtual void addBatchArray(
        size_t batch_size, AggregateDataPtr * places, size_t place_offset, const IColumn ** columns, const UInt64 * offsets, Arena * arena) const = 0;

    /** By default all NULLs are skipped during aggregation.
     *  If it returns nullptr, the default one will be used.
     *  If an aggregate function wants to use something instead of the default one, it overrides this function and returns its own null adapter.
     *  nested_function is a smart pointer to this aggregate function itself.
     *  arguments and params are for nested_function.
     */
    virtual AggregateFunctionPtr getOwnNullAdapter(
        const AggregateFunctionPtr & /*nested_function*/, const DataTypes & /*arguments*/, const Array & /*params*/) const
    {
        return nullptr;
    }

    const DataTypes & getArgumentTypes() const { return argument_types; }
    const Array & getParameters() const { return parameters; }

protected:
    DataTypes argument_types;
    Array parameters;
};


/// Implement method to obtain an address of 'add' function.
template <typename Derived>
class IAggregateFunctionHelper : public IAggregateFunction
{
private:
    static void addFree(const IAggregateFunction * that, AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena * arena)
    {
        static_cast<const Derived &>(*that).add(place, columns, row_num, arena);
    }

public:
    IAggregateFunctionHelper(const DataTypes & argument_types_, const Array & parameters_)
        : IAggregateFunction(argument_types_, parameters_) {}

    AddFunc getAddressOfAddFunction() const override { return &addFree; }

    void addBatch(size_t batch_size, AggregateDataPtr * places, size_t place_offset, const IColumn ** columns, Arena * arena) const override
    {
        for (size_t i = 0; i < batch_size; ++i)
            static_cast<const Derived *>(this)->add(places[i] + place_offset, columns, i, arena);
    }

    void addBatchSinglePlace(size_t batch_size, AggregateDataPtr place, const IColumn ** columns, Arena * arena) const override
    {
        for (size_t i = 0; i < batch_size; ++i)
            static_cast<const Derived *>(this)->add(place, columns, i, arena);
    }

    void addBatchSinglePlaceNotNull(
        size_t batch_size, AggregateDataPtr place, const IColumn ** columns, const UInt8 * null_map, Arena * arena) const override
    {
        for (size_t i = 0; i < batch_size; ++i)
            if (!null_map[i])
                static_cast<const Derived *>(this)->add(place, columns, i, arena);
    }

    void addBatchSinglePlaceFromInterval(size_t batch_begin, size_t batch_end, AggregateDataPtr place, const IColumn ** columns, Arena * arena) const override
    {
        for (size_t i = batch_begin; i < batch_end; ++i)
            static_cast<const Derived *>(this)->add(place, columns, i, arena);
    }

    void addBatchArray(
        size_t batch_size, AggregateDataPtr * places, size_t place_offset, const IColumn ** columns, const UInt64 * offsets, Arena * arena)
        const override
    {
        size_t current_offset = 0;
        for (size_t i = 0; i < batch_size; ++i)
        {
            size_t next_offset = offsets[i];
            for (size_t j = current_offset; j < next_offset; ++j)
                static_cast<const Derived *>(this)->add(places[i] + place_offset, columns, j, arena);
            current_offset = next_offset;
        }
    }
};


/// Implements several methods for manipulation with data. T - type of structure with data for aggregation.
template <typename T, typename Derived>
class IAggregateFunctionDataHelper : public IAggregateFunctionHelper<Derived>
{
protected:
    using Data = T;

    static Data & data(AggregateDataPtr place) { return *reinterpret_cast<Data*>(place); }
    static const Data & data(ConstAggregateDataPtr place) { return *reinterpret_cast<const Data*>(place); }

public:
    IAggregateFunctionDataHelper(const DataTypes & argument_types_, const Array & parameters_)
        : IAggregateFunctionHelper<Derived>(argument_types_, parameters_) {}

    void create(AggregateDataPtr place) const override
    {
        new (place) Data;
    }

    void destroy(AggregateDataPtr place) const noexcept override
    {
        data(place).~Data();
    }

    bool hasTrivialDestructor() const override
    {
        return std::is_trivially_destructible_v<Data>;
    }

    size_t sizeOfData() const override
    {
        return sizeof(Data);
    }

    /// NOTE: Currently not used (structures with aggregation state are put without alignment).
    size_t alignOfData() const override
    {
        return alignof(Data);
    }
};


/// Properties of aggregate function that are independent of argument types and parameters.
struct AggregateFunctionProperties
{
    /** When the function is wrapped with Null combinator,
      * should we return Nullable type with NULL when no values were aggregated
      * or we should return non-Nullable type with default value (example: count, countDistinct).
      */
    bool returns_default_when_only_null = false;

    /** Result varies depending on the data order (example: groupArray).
      * Some may also name this property as "non-commutative".
      */
    bool is_order_dependent = false;
};


}
