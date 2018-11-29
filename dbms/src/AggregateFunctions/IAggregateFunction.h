#pragma once

#include <cstddef>
#include <memory>
#include <vector>
#include <type_traits>

#include <Core/Types.h>
#include <Core/Field.h>
#include <Common/Exception.h>


namespace DB
{

class Arena;
class ReadBuffer;
class WriteBuffer;
class IColumn;
class IDataType;

using DataTypePtr = std::shared_ptr<const IDataType>;
using DataTypes = std::vector<DataTypePtr>;

using AggregateDataPtr = char *;
using ConstAggregateDataPtr = const char *;


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
    /// Get main function name.
    virtual String getName() const = 0;

    /// Get the result type.
    virtual DataTypePtr getReturnType() const = 0;

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
    virtual void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const = 0;

    /** Returns true for aggregate functions of type -State.
      * They are executed as other aggregate functions, but not finalized (return an aggregation state that can be combined with another).
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

    /** This is used for runtime code generation to determine, which header files to include in generated source.
      * Always implement it as
      * const char * getHeaderFilePath() const override { return __FILE__; }
      */
    virtual const char * getHeaderFilePath() const = 0;
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
    AddFunc getAddressOfAddFunction() const override { return &addFree; }
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


using AggregateFunctionPtr = std::shared_ptr<IAggregateFunction>;

}
