#pragma once

#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnSparse.h>
#include <Core/Block.h>
#include <Core/ColumnNumbers.h>
#include <Core/Field.h>
#include <Interpreters/Context_fwd.h>
#include <Common/Exception.h>
#include <base/types.h>

#include "config_core.h"

#include <cstddef>
#include <memory>
#include <vector>
#include <type_traits>

namespace llvm
{
    class LLVMContext;
    class Value;
    class IRBuilderBase;
}

namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

class Arena;
class ReadBuffer;
class WriteBuffer;
class IColumn;
class IDataType;
class IWindowFunction;

using DataTypePtr = std::shared_ptr<const IDataType>;
using DataTypes = std::vector<DataTypePtr>;

using AggregateDataPtr = char *;
using ConstAggregateDataPtr = const char *;

class IAggregateFunction;
using AggregateFunctionPtr = std::shared_ptr<const IAggregateFunction>;
struct AggregateFunctionProperties;

/** Aggregate functions interface.
  * Instances of classes with this interface do not contain the data itself for aggregation,
  *  but contain only metadata (description) of the aggregate function,
  *  as well as methods for creating, deleting and working with data.
  * The data resulting from the aggregation (intermediate computing states) is stored in other objects
  *  (which can be created in some memory pool),
  *  and IAggregateFunction is the external interface for manipulating them.
  */
class IAggregateFunction : public std::enable_shared_from_this<IAggregateFunction>
{
public:
    IAggregateFunction(const DataTypes & argument_types_, const Array & parameters_)
        : argument_types(argument_types_), parameters(parameters_) {}

    /// Get main function name.
    virtual String getName() const = 0;

    /// Get the result type.
    virtual DataTypePtr getReturnType() const = 0;

    /// Get the data type of internal state. By default it is AggregateFunction(name(params), argument_types...).
    virtual DataTypePtr getStateType() const;

    /// Returns true if two aggregate functions have the same state representation in memory and the same serialization,
    /// so state of one aggregate function can be safely used with another.
    /// Examples:
    ///  - quantile(x), quantile(a)(x), quantile(b)(x) - parameter doesn't affect state and used for finalization only
    ///  - foo(x) and fooIf(x) - If combinator doesn't affect state
    /// By default returns true only if functions have exactly the same names, combinators and parameters.
    virtual bool haveSameStateRepresentation(const IAggregateFunction & rhs) const;

    bool haveEqualArgumentTypes(const IAggregateFunction & rhs) const;

    /// Get type which will be used for prediction result in case if function is an ML method.
    virtual DataTypePtr getReturnTypeToPredict() const
    {
        throw Exception("Prediction is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    virtual bool isVersioned() const { return false; }

    virtual size_t getVersionFromRevision(size_t /* revision */) const { return 0; }

    virtual size_t getDefaultVersion() const { return 0; }

    virtual ~IAggregateFunction() = default;

    /** Data manipulating functions. */

    /** Create empty data for aggregation with `placement new` at the specified location.
      * You will have to destroy them using the `destroy` method.
      */
    virtual void create(AggregateDataPtr __restrict place) const = 0;

    /// Delete data for aggregation.
    virtual void destroy(AggregateDataPtr __restrict place) const noexcept = 0;

    /// It is not necessary to delete data.
    virtual bool hasTrivialDestructor() const = 0;

    /// Get `sizeof` of structure with data.
    virtual size_t sizeOfData() const = 0;

    /// How the data structure should be aligned.
    virtual size_t alignOfData() const = 0;

    /** Adds a value into aggregation data on which place points to.
     *  columns points to columns containing arguments of aggregation function.
     *  row_num is number of row which should be added.
     *  Additional parameter arena should be used instead of standard memory allocator if the addition requires memory allocation.
     */
    virtual void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const = 0;

    /// Merges state (on which place points to) with other state of current aggregation function.
    virtual void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const = 0;

    /// Serializes state (to transmit it over the network, for example).
    virtual void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> version = std::nullopt) const = 0; /// NOLINT

    /// Deserializes state. This function is called only for empty (just created) states.
    virtual void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> version = std::nullopt, Arena * arena = nullptr) const = 0; /// NOLINT

    /// Returns true if a function requires Arena to handle own states (see add(), merge(), deserialize()).
    virtual bool allocatesMemoryInArena() const = 0;

    /// Inserts results into a column. This method might modify the state (e.g.
    /// sort an array), so must be called once, from single thread. The state
    /// must remain valid though, and the subsequent calls to add/merge/
    /// insertResultInto must work correctly. This kind of call sequence occurs
    /// in `runningAccumulate`, or when calculating an aggregate function as a
    /// window function.
    virtual void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const = 0;

    /// Used for machine learning methods. Predict result from trained model.
    /// Will insert result into `to` column for rows in range [offset, offset + limit).
    virtual void predictValues(
        ConstAggregateDataPtr /* place */,
        IColumn & /*to*/,
        const ColumnsWithTypeAndName & /*arguments*/,
        size_t /*offset*/,
        size_t /*limit*/,
        ContextPtr /*context*/) const
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
    virtual void addBatch( /// NOLINT
        size_t batch_size,
        AggregateDataPtr * places,
        size_t place_offset,
        const IColumn ** columns,
        Arena * arena,
        ssize_t if_argument_pos = -1) const = 0;

    /// The version of "addBatch", that handle sparse columns as arguments.
    virtual void addBatchSparse(
        AggregateDataPtr * places,
        size_t place_offset,
        const IColumn ** columns,
        Arena * arena) const = 0;

    virtual void mergeBatch(
        size_t batch_size,
        AggregateDataPtr * places,
        size_t place_offset,
        const AggregateDataPtr * rhs,
        Arena * arena) const = 0;

    /** The same for single place.
      */
    virtual void addBatchSinglePlace( /// NOLINT
        size_t batch_size, AggregateDataPtr place, const IColumn ** columns, Arena * arena, ssize_t if_argument_pos = -1) const = 0;

    /// The version of "addBatchSinglePlace", that handle sparse columns as arguments.
    virtual void addBatchSparseSinglePlace(
        AggregateDataPtr place, const IColumn ** columns, Arena * arena) const = 0;

    /** The same for single place when need to aggregate only filtered data.
      * Instead of using an if-column, the condition is combined inside the null_map
      */
    virtual void addBatchSinglePlaceNotNull( /// NOLINT
        size_t batch_size,
        AggregateDataPtr place,
        const IColumn ** columns,
        const UInt8 * null_map,
        Arena * arena,
        ssize_t if_argument_pos = -1) const = 0;

    virtual void addBatchSinglePlaceFromInterval( /// NOLINT
        size_t batch_begin, size_t batch_end, AggregateDataPtr place, const IColumn ** columns, Arena * arena, ssize_t if_argument_pos = -1)
        const = 0;

    /** In addition to addBatch, this method collects multiple rows of arguments into array "places"
      *  as long as they are between offsets[i-1] and offsets[i]. This is used for arrayReduce and
      *  -Array combinator. It might also be used generally to break data dependency when array
      *  "places" contains a large number of same values consecutively.
      */
    virtual void addBatchArray(
        size_t batch_size,
        AggregateDataPtr * places,
        size_t place_offset,
        const IColumn ** columns,
        const UInt64 * offsets,
        Arena * arena) const = 0;

    /** The case when the aggregation key is UInt8
      * and pointers to aggregation states are stored in AggregateDataPtr[256] lookup table.
      */
    virtual void addBatchLookupTable8(
        size_t batch_size,
        AggregateDataPtr * places,
        size_t place_offset,
        std::function<void(AggregateDataPtr &)> init,
        const UInt8 * key,
        const IColumn ** columns,
        Arena * arena) const = 0;

    /** Insert result of aggregate function into result column with batch size.
      * If destroy_place_after_insert is true. Then implementation of this method
      * must destroy aggregate place if insert state into result column was successful.
      * All places that were not inserted must be destroyed if there was exception during insert into result column.
      */
    virtual void insertResultIntoBatch(
        size_t batch_size,
        AggregateDataPtr * places,
        size_t place_offset,
        IColumn & to,
        Arena * arena,
        bool destroy_place_after_insert) const = 0;

    /** Destroy batch of aggregate places.
      */
    virtual void destroyBatch(
        size_t batch_size,
        AggregateDataPtr * places,
        size_t place_offset) const noexcept = 0;

    /** By default all NULLs are skipped during aggregation.
     *  If it returns nullptr, the default one will be used.
     *  If an aggregate function wants to use something instead of the default one, it overrides this function and returns its own null adapter.
     *  nested_function is a smart pointer to this aggregate function itself.
     *  arguments and params are for nested_function.
     */
    virtual AggregateFunctionPtr getOwnNullAdapter(
        const AggregateFunctionPtr & /*nested_function*/, const DataTypes & /*arguments*/,
        const Array & /*params*/, const AggregateFunctionProperties & /*properties*/) const
    {
        return nullptr;
    }

    /** Return the nested function if this is an Aggregate Function Combinator.
      * Otherwise return nullptr.
      */
    virtual AggregateFunctionPtr getNestedFunction() const { return {}; }

    const DataTypes & getArgumentTypes() const { return argument_types; }
    const Array & getParameters() const { return parameters; }

    // Any aggregate function can be calculated over a window, but there are some
    // window functions such as rank() that require a different interface, e.g.
    // because they don't respect the window frame, or need to be notified when
    // a new peer group starts. They pretend to be normal aggregate functions,
    // but will fail if you actually try to use them in Aggregator. The
    // WindowTransform recognizes these functions and handles them differently.
    // We could have a separate factory for window functions, and make all
    // aggregate functions implement IWindowFunction interface and so on. This
    // would be more logically correct, but more complex. We only have a handful
    // of true window functions, so this hack-ish interface suffices.
    virtual bool isOnlyWindowFunction() const { return false; }

    /// Description of AggregateFunction in form of name(parameters)(argument_types).
    String getDescription() const;

#if USE_EMBEDDED_COMPILER

    /// Is function JIT compilable
    virtual bool isCompilable() const { return false; }

    /// compileCreate should generate code for initialization of aggregate function state in aggregate_data_ptr
    virtual void compileCreate(llvm::IRBuilderBase & /*builder*/, llvm::Value * /*aggregate_data_ptr*/) const
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} is not JIT-compilable", getName());
    }

    /// compileAdd should generate code for updating aggregate function state stored in aggregate_data_ptr
    virtual void compileAdd(llvm::IRBuilderBase & /*builder*/, llvm::Value * /*aggregate_data_ptr*/, const DataTypes & /*arguments_types*/, const std::vector<llvm::Value *> & /*arguments_values*/) const
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} is not JIT-compilable", getName());
    }

    /// compileMerge should generate code for merging aggregate function states stored in aggregate_data_dst_ptr and aggregate_data_src_ptr
    virtual void compileMerge(llvm::IRBuilderBase & /*builder*/, llvm::Value * /*aggregate_data_dst_ptr*/, llvm::Value * /*aggregate_data_src_ptr*/) const
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} is not JIT-compilable", getName());
    }

    /// compileGetResult should generate code for getting result value from aggregate function state stored in aggregate_data_ptr
    virtual llvm::Value * compileGetResult(llvm::IRBuilderBase & /*builder*/, llvm::Value * /*aggregate_data_ptr*/) const
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} is not JIT-compilable", getName());
    }

#endif

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

    void addBatch( /// NOLINT
        size_t batch_size,
        AggregateDataPtr * places,
        size_t place_offset,
        const IColumn ** columns,
        Arena * arena,
        ssize_t if_argument_pos = -1) const override
    {
        if (if_argument_pos >= 0)
        {
            const auto & flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
            for (size_t i = 0; i < batch_size; ++i)
            {
                if (flags[i] && places[i])
                    static_cast<const Derived *>(this)->add(places[i] + place_offset, columns, i, arena);
            }
        }
        else
        {
            for (size_t i = 0; i < batch_size; ++i)
                if (places[i])
                    static_cast<const Derived *>(this)->add(places[i] + place_offset, columns, i, arena);
        }
    }

    void addBatchSparse(
        AggregateDataPtr * places,
        size_t place_offset,
        const IColumn ** columns,
        Arena * arena) const override
    {
        const auto & column_sparse = assert_cast<const ColumnSparse &>(*columns[0]);
        const auto * values = &column_sparse.getValuesColumn();
        size_t batch_size = column_sparse.size();
        auto offset_it = column_sparse.begin();

        for (size_t i = 0; i < batch_size; ++i, ++offset_it)
            static_cast<const Derived *>(this)->add(places[offset_it.getCurrentRow()] + place_offset,
                                                    &values, offset_it.getValueIndex(), arena);
    }

    void mergeBatch(
        size_t batch_size,
        AggregateDataPtr * places,
        size_t place_offset,
        const AggregateDataPtr * rhs,
        Arena * arena) const override
    {
        for (size_t i = 0; i < batch_size; ++i)
            if (places[i])
                static_cast<const Derived *>(this)->merge(places[i] + place_offset, rhs[i], arena);
    }

    void addBatchSinglePlace( /// NOLINT
        size_t batch_size, AggregateDataPtr place, const IColumn ** columns, Arena * arena, ssize_t if_argument_pos = -1) const override
    {
        if (if_argument_pos >= 0)
        {
            const auto & flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
            for (size_t i = 0; i < batch_size; ++i)
            {
                if (flags[i])
                    static_cast<const Derived *>(this)->add(place, columns, i, arena);
            }
        }
        else
        {
            for (size_t i = 0; i < batch_size; ++i)
                static_cast<const Derived *>(this)->add(place, columns, i, arena);
        }
    }

    void addBatchSparseSinglePlace(
        AggregateDataPtr place, const IColumn ** columns, Arena * arena) const override
    {
        /// TODO: add values and defaults separately if order of adding isn't important.
        const auto & column_sparse = assert_cast<const ColumnSparse &>(*columns[0]);
        const auto * values = &column_sparse.getValuesColumn();
        size_t batch_size = column_sparse.size();
        auto offset_it = column_sparse.begin();

        for (size_t i = 0; i < batch_size; ++i, ++offset_it)
            static_cast<const Derived *>(this)->add(place, &values, offset_it.getValueIndex(), arena);
    }

    void addBatchSinglePlaceNotNull( /// NOLINT
        size_t batch_size,
        AggregateDataPtr place,
        const IColumn ** columns,
        const UInt8 * null_map,
        Arena * arena,
        ssize_t if_argument_pos = -1) const override
    {
        if (if_argument_pos >= 0)
        {
            const auto & flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
            for (size_t i = 0; i < batch_size; ++i)
                if (!null_map[i] && flags[i])
                    static_cast<const Derived *>(this)->add(place, columns, i, arena);
        }
        else
        {
            for (size_t i = 0; i < batch_size; ++i)
                if (!null_map[i])
                    static_cast<const Derived *>(this)->add(place, columns, i, arena);
        }
    }

    void addBatchSinglePlaceFromInterval( /// NOLINT
        size_t batch_begin, size_t batch_end, AggregateDataPtr place, const IColumn ** columns, Arena * arena, ssize_t if_argument_pos = -1)
        const override
    {
        if (if_argument_pos >= 0)
        {
            const auto & flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
            for (size_t i = batch_begin; i < batch_end; ++i)
            {
                if (flags[i])
                    static_cast<const Derived *>(this)->add(place, columns, i, arena);
            }
        }
        else
        {
            for (size_t i = batch_begin; i < batch_end; ++i)
                static_cast<const Derived *>(this)->add(place, columns, i, arena);
        }
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
                if (places[i])
                    static_cast<const Derived *>(this)->add(places[i] + place_offset, columns, j, arena);
            current_offset = next_offset;
        }
    }

    void addBatchLookupTable8(
        size_t batch_size,
        AggregateDataPtr * map,
        size_t place_offset,
        std::function<void(AggregateDataPtr &)> init,
        const UInt8 * key,
        const IColumn ** columns,
        Arena * arena) const override
    {
        static constexpr size_t UNROLL_COUNT = 8;

        size_t i = 0;

        size_t batch_size_unrolled = batch_size / UNROLL_COUNT * UNROLL_COUNT;
        for (; i < batch_size_unrolled; i += UNROLL_COUNT)
        {
            AggregateDataPtr places[UNROLL_COUNT];
            for (size_t j = 0; j < UNROLL_COUNT; ++j)
            {
                AggregateDataPtr & place = map[key[i + j]];
                if (unlikely(!place))
                    init(place);

                places[j] = place;
            }

            for (size_t j = 0; j < UNROLL_COUNT; ++j)
                static_cast<const Derived *>(this)->add(places[j] + place_offset, columns, i + j, arena);
        }

        for (; i < batch_size; ++i)
        {
            AggregateDataPtr & place = map[key[i]];
            if (unlikely(!place))
                init(place);
            static_cast<const Derived *>(this)->add(place + place_offset, columns, i, arena);
        }
    }

    void insertResultIntoBatch(size_t batch_size, AggregateDataPtr * places, size_t place_offset, IColumn & to, Arena * arena, bool destroy_place_after_insert) const override
    {
        size_t batch_index = 0;

        try
        {
            for (; batch_index < batch_size; ++batch_index)
            {
                static_cast<const Derived *>(this)->insertResultInto(places[batch_index] + place_offset, to, arena);

                if (destroy_place_after_insert)
                    static_cast<const Derived *>(this)->destroy(places[batch_index] + place_offset);
            }
        }
        catch (...)
        {
            for (size_t destroy_index = batch_index; destroy_index < batch_size; ++destroy_index)
                static_cast<const Derived *>(this)->destroy(places[destroy_index] + place_offset);

            throw;
        }
    }

    void destroyBatch(size_t batch_size, AggregateDataPtr * places, size_t place_offset) const noexcept override
    {
        for (size_t i = 0; i < batch_size; ++i)
        {
            static_cast<const Derived *>(this)->destroy(places[i] + place_offset);
        }
    }
};


/// Implements several methods for manipulation with data. T - type of structure with data for aggregation.
template <typename T, typename Derived>
class IAggregateFunctionDataHelper : public IAggregateFunctionHelper<Derived>
{
protected:
    using Data = T;

    static Data & data(AggregateDataPtr __restrict place) { return *reinterpret_cast<Data *>(place); }
    static const Data & data(ConstAggregateDataPtr __restrict place) { return *reinterpret_cast<const Data *>(place); }

public:
    // Derived class can `override` this to flag that DateTime64 is not supported.
    static constexpr bool DateTime64Supported = true;

    IAggregateFunctionDataHelper(const DataTypes & argument_types_, const Array & parameters_)
        : IAggregateFunctionHelper<Derived>(argument_types_, parameters_) {}

    void create(AggregateDataPtr place) const override /// NOLINT
    {
        new (place) Data;
    }

    void destroy(AggregateDataPtr __restrict place) const noexcept override
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

    size_t alignOfData() const override
    {
        return alignof(Data);
    }

    void addBatchLookupTable8(
        size_t batch_size,
        AggregateDataPtr * map,
        size_t place_offset,
        std::function<void(AggregateDataPtr &)> init,
        const UInt8 * key,
        const IColumn ** columns,
        Arena * arena) const override
    {
        const Derived & func = *static_cast<const Derived *>(this);

        /// If the function is complex or too large, use more generic algorithm.

        if (func.allocatesMemoryInArena() || sizeof(Data) > 16 || func.sizeOfData() != sizeof(Data))
        {
            IAggregateFunctionHelper<Derived>::addBatchLookupTable8(batch_size, map, place_offset, init, key, columns, arena);
            return;
        }

        /// Will use UNROLL_COUNT number of lookup tables.

        static constexpr size_t UNROLL_COUNT = 4;

        std::unique_ptr<Data[]> places{new Data[256 * UNROLL_COUNT]};
        bool has_data[256 * UNROLL_COUNT]{}; /// Separate flags array to avoid heavy initialization.

        size_t i = 0;

        /// Aggregate data into different lookup tables.

        size_t batch_size_unrolled = batch_size / UNROLL_COUNT * UNROLL_COUNT;
        for (; i < batch_size_unrolled; i += UNROLL_COUNT)
        {
            for (size_t j = 0; j < UNROLL_COUNT; ++j)
            {
                size_t idx = j * 256 + key[i + j];
                if (unlikely(!has_data[idx]))
                {
                    new (&places[idx]) Data;
                    has_data[idx] = true;
                }
                func.add(reinterpret_cast<char *>(&places[idx]), columns, i + j, nullptr);
            }
        }

        /// Merge data from every lookup table to the final destination.

        for (size_t k = 0; k < 256; ++k)
        {
            for (size_t j = 0; j < UNROLL_COUNT; ++j)
            {
                size_t idx = j * 256 + k;
                if (has_data[idx])
                {
                    AggregateDataPtr & place = map[k];
                    if (unlikely(!place))
                        init(place);

                    func.merge(place + place_offset, reinterpret_cast<const char *>(&places[idx]), nullptr);
                }
            }
        }

        /// Process tails and add directly to the final destination.

        for (; i < batch_size; ++i)
        {
            size_t k = key[i];
            AggregateDataPtr & place = map[k];
            if (unlikely(!place))
                init(place);

            func.add(place + place_offset, columns, i, nullptr);
        }
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
