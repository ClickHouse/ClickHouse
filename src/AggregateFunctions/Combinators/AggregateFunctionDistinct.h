#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/Combinators/AggregateFunctionNull.h>
#include <AggregateFunctions/KeyHolderHelpers.h>
#include <IO/ReadHelpersArena.h>
#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/HashSet.h>
#include <Common/assert_cast.h>
#include <Common/VectorWithMemoryTracking.h>


namespace DB
{
struct Settings;


template <typename T>
struct AggregateFunctionDistinctSingleNumericData
{
    /// When creating, the hash table must be small.
    using Set = HashSetWithStackMemory<T, DefaultHash<T>, 4>;
    using Self = AggregateFunctionDistinctSingleNumericData<T>;

    /// history will hold all values added so far
    Set history;

    bool add(const IColumn ** __restrict columns, size_t /* columns_num */, size_t row_num, Arena *)
    {
        return add(*columns[0], row_num);
    }

    bool add(const IColumn & column, size_t row_num)
    {
        const auto & vec = assert_cast<const ColumnVector<T> &>(column).getData();
        return history.insert(vec[row_num]).second;
    }

    /// Pass the new values from rhs to the nested function via argument_columns
    void merge(const Self & rhs, MutableColumns & argument_columns, Arena *)
    {
        for (const auto & elem : rhs.history)
        {
            const auto & value = elem.getValue();
            bool inserted = history.insert(value).second;

            if (inserted)
                argument_columns[0]->insert(value);
        }
    }

    void serialize(WriteBuffer & buf) const
    {
        history.write(buf);
    }

    void deserialize(ReadBuffer & buf, Arena *)
    {
        history.read(buf);
    }
};


/** Ordered variant for productDistinct. Its on-disk format is the same as the
  * regular numeric Distinct set: a count followed by binary-encoded keys.
  * Recording insertion order also lets us reconstruct the nested aggregate
  * after deserialization without changing the order of multiplication.
  */
template <typename T>
struct AggregateFunctionDistinctSingleNumericOrderedData : AggregateFunctionDistinctSingleNumericData<T>
{
    VectorWithMemoryTracking<T> insertion_order;

    bool add(const IColumn ** columns, size_t, size_t row_num, Arena *)
    {
        return add(*columns[0], row_num);
    }

    bool add(const IColumn & column, size_t row_num)
    {
        const auto & values = assert_cast<const ColumnVector<T> &>(column).getData();
        const T value = values[row_num];
        if (!this->history.insert(value).second)
            return false;

        insertion_order.push_back(value);
        return true;
    }

    void merge(const AggregateFunctionDistinctSingleNumericOrderedData & rhs, MutableColumns & argument_columns, Arena *)
    {
        for (const T & value : rhs.insertion_order)
        {
            if (this->history.insert(value).second)
            {
                insertion_order.push_back(value);
                argument_columns[0]->insert(value);
            }
        }
    }

    void serialize(WriteBuffer & buf) const
    {
        writeVarUInt(insertion_order.size(), buf);
        for (const T & value : insertion_order)
            writeBinaryLittleEndian(value, buf);
    }

    void deserialize(ReadBuffer & buf, Arena *)
    {
        size_t size = 0;
        readVarUInt(size, buf);
        for (size_t i = 0; i < size; ++i)
        {
            T value{};
            readBinaryLittleEndian(value, buf);
            if (this->history.insert(value).second)
                insertion_order.push_back(value);
        }
    }

    void insertOrderedValues(MutableColumns & argument_columns) const
    {
        for (const T & value : insertion_order)
            argument_columns[0]->insert(value);
    }
};

struct AggregateFunctionDistinctGenericData
{
    /// When creating, the hash table must be small.
    using Set = HashSetWithSavedHashWithStackMemory<std::string_view, StringViewHash, 4>;
    using Self = AggregateFunctionDistinctGenericData;

    Set history;

    void serialize(WriteBuffer & buf) const
    {
        writeVarUInt(history.size(), buf);
        for (const auto & elem : history)
            writeStringBinary(elem.getValue(), buf);
    }

    void deserialize(ReadBuffer & buf, Arena * arena)
    {
        size_t size = 0;
        readVarUInt(size, buf);
        for (size_t i = 0; i < size; ++i)
            history.insert(readStringBinaryInto(*arena, buf));
    }
};

template <bool is_plain_column>
struct AggregateFunctionDistinctSingleGenericData : public AggregateFunctionDistinctGenericData
{
    bool add(const IColumn ** columns, size_t /* columns_num */, size_t row_num, Arena * arena)
    {
        auto key_holder = getKeyHolder<is_plain_column>(*columns[0], row_num, *arena);
        Set::LookupResult it = nullptr;
        bool inserted = false;
        history.emplace(key_holder, it, inserted);

        return inserted;
    }

    void merge(const Self & rhs, MutableColumns & argument_columns, Arena * arena)
    {
        for (const auto & elem : rhs.history)
        {
            const auto & value = elem.getValue();
            Set::LookupResult it = nullptr;
            bool inserted = false;
            history.emplace(ArenaKeyHolder{value, *arena}, it, inserted);

            if (inserted)
                deserializeAndInsert<is_plain_column>(it->getValue(), *argument_columns[0]);
        }
    }
};


/** Keep the first-seen order for non-numeric arguments of productDistinct,
  * including Decimal and Nullable. The serialized representation remains a
  * count followed by serialized keys, as in the regular generic Distinct set.
  */
template <bool is_plain_column>
struct AggregateFunctionDistinctSingleGenericOrderedData : AggregateFunctionDistinctSingleGenericData<is_plain_column>
{
    using Base = AggregateFunctionDistinctSingleGenericData<is_plain_column>;
    using Set = typename Base::Set;
    VectorWithMemoryTracking<std::string_view> insertion_order;

    bool add(const IColumn ** columns, size_t, size_t row_num, Arena * arena)
    {
        auto key_holder = getKeyHolder<is_plain_column>(*columns[0], row_num, *arena);
        typename Set::LookupResult it = nullptr;
        bool inserted = false;
        this->history.emplace(key_holder, it, inserted);
        if (inserted)
            insertion_order.push_back(it->getValue());
        return inserted;
    }

    void merge(const AggregateFunctionDistinctSingleGenericOrderedData & rhs, MutableColumns & argument_columns, Arena * arena)
    {
        for (const auto & key : rhs.insertion_order)
        {
            typename Set::LookupResult it = nullptr;
            bool inserted = false;
            this->history.emplace(ArenaKeyHolder{key, *arena}, it, inserted);
            if (inserted)
            {
                insertion_order.push_back(it->getValue());
                deserializeAndInsert<is_plain_column>(it->getValue(), *argument_columns[0]);
            }
        }
    }

    void serialize(WriteBuffer & buf) const
    {
        writeVarUInt(insertion_order.size(), buf);
        for (const auto & key : insertion_order)
            writeStringBinary(key, buf);
    }

    void deserialize(ReadBuffer & buf, Arena * arena)
    {
        size_t size = 0;
        readVarUInt(size, buf);
        for (size_t i = 0; i < size; ++i)
        {
            const auto [it, inserted] = this->history.insert(readStringBinaryInto(*arena, buf));
            if (inserted)
                insertion_order.push_back(it->getValue());
        }
    }

    void insertOrderedValues(MutableColumns & argument_columns) const
    {
        for (const auto & key : insertion_order)
            deserializeAndInsert<is_plain_column>(key, *argument_columns[0]);
    }
};

struct AggregateFunctionDistinctMultipleGenericData : public AggregateFunctionDistinctGenericData
{
    bool add(const IColumn ** columns, size_t columns_num, size_t row_num, Arena * arena)
    {
        const char * begin = nullptr;
        std::string_view value;
        for (size_t i = 0; i < columns_num; ++i)
        {
            auto settings = IColumn::SerializationSettings::createForAggregationState();
            auto cur_ref = columns[i]->serializeValueIntoArena(row_num, *arena, begin, &settings);
            value = std::string_view{cur_ref.data() - value.size(), value.size() + cur_ref.size()};
        }

        Set::LookupResult it = nullptr;
        bool inserted = false;
        history.emplace(SerializedKeyHolder{value, *arena}, it, inserted);

        return inserted;
    }

    void merge(const Self & rhs, MutableColumns & argument_columns, Arena * arena)
    {
        for (const auto & elem : rhs.history)
        {
            const auto & value = elem.getValue();
            if (!history.contains(value))
            {
                Set::LookupResult it = nullptr;
                bool inserted = false;
                history.emplace(ArenaKeyHolder{value, *arena}, it, inserted);
                ReadBufferFromString in(it->getValue());
                /// Multiple columns are serialized one by one
                auto settings = IColumn::SerializationSettings::createForAggregationState();
                for (auto & column : argument_columns)
                    column->deserializeAndInsertFromArena(in, &settings);
            }
        }
    }
};


/** Ordered multi-argument Distinct history for product combinators such as
  * productIfDistinct and productArgMaxDistinct. Keys use the same serialized
  * representation as AggregateFunctionDistinctMultipleGenericData, but are
  * replayed in the order in which their distinct tuples were first seen.
  */
struct AggregateFunctionDistinctMultipleGenericOrderedData : AggregateFunctionDistinctMultipleGenericData
{
    VectorWithMemoryTracking<std::string_view> insertion_order;

    static void insertColumns(std::string_view key, MutableColumns & argument_columns)
    {
        ReadBufferFromString in(key);
        auto settings = IColumn::SerializationSettings::createForAggregationState();
        for (auto & column : argument_columns)
            column->deserializeAndInsertFromArena(in, &settings);
    }

    bool add(const IColumn ** columns, size_t columns_num, size_t row_num, Arena * arena)
    {
        const char * begin = nullptr;
        std::string_view value;
        for (size_t i = 0; i < columns_num; ++i)
        {
            auto settings = IColumn::SerializationSettings::createForAggregationState();
            auto cur_ref = columns[i]->serializeValueIntoArena(row_num, *arena, begin, &settings);
            value = std::string_view{cur_ref.data() - value.size(), value.size() + cur_ref.size()};
        }

        Set::LookupResult it = nullptr;
        bool inserted = false;
        history.emplace(SerializedKeyHolder{value, *arena}, it, inserted);
        if (inserted)
            insertion_order.push_back(it->getValue());
        return inserted;
    }

    void merge(const AggregateFunctionDistinctMultipleGenericOrderedData & rhs, MutableColumns & argument_columns, Arena * arena)
    {
        for (const auto & key : rhs.insertion_order)
        {
            Set::LookupResult it = nullptr;
            bool inserted = false;
            history.emplace(ArenaKeyHolder{key, *arena}, it, inserted);
            if (inserted)
            {
                insertion_order.push_back(it->getValue());
                insertColumns(it->getValue(), argument_columns);
            }
        }
    }

    void serialize(WriteBuffer & buf) const
    {
        writeVarUInt(insertion_order.size(), buf);
        for (const auto & key : insertion_order)
            writeStringBinary(key, buf);
    }

    void deserialize(ReadBuffer & buf, Arena * arena)
    {
        size_t size = 0;
        readVarUInt(size, buf);
        for (size_t i = 0; i < size; ++i)
        {
            /// readStringBinaryInto already owns the key in the Arena.
            /// Avoid copying every persisted key into the same Arena again.
            const auto [it, inserted] = history.insert(readStringBinaryInto(*arena, buf));
            if (inserted)
                insertion_order.push_back(it->getValue());
        }
    }

    void insertOrderedValues(MutableColumns & argument_columns) const
    {
        for (const auto & key : insertion_order)
            insertColumns(key, argument_columns);
    }
};

/** Adaptor for aggregate functions.
  * Adding -Distinct suffix to aggregate function
**/
template <typename Data>
class AggregateFunctionDistinct final : public IAggregateFunctionDataHelper<Data, AggregateFunctionDistinct<Data>>
{
private:
    AggregateFunctionPtr nested_func;
    size_t prefix_size;
    size_t arguments_num;
    bool preserve_input_order;

    AggregateDataPtr getNestedPlace(AggregateDataPtr __restrict place) const noexcept
    {
        return place + prefix_size;
    }

    ConstAggregateDataPtr getNestedPlace(ConstAggregateDataPtr __restrict place) const noexcept
    {
        return place + prefix_size;
    }

    MutableColumns prepareArgumentColumns() const
    {
        MutableColumns argument_columns;
        argument_columns.reserve(this->argument_types.size());
        for (const auto & type : this->argument_types)
            argument_columns.emplace_back(type->createColumn());

        return argument_columns;
    }

    void addToNested(size_t row_begin, size_t row_end, AggregateDataPtr __restrict place, const IColumn ** columns, Arena * arena) const
    {
        nested_func->addBatchSinglePlace(row_begin, row_end, getNestedPlace(place), columns, arena);
    }

    void addToNested(AggregateDataPtr __restrict place, const MutableColumns & argument_columns, Arena * arena) const
    {
        ColumnRawPtrs arguments_raw(argument_columns.size());
        for (size_t i = 0; i < argument_columns.size(); ++i)
            arguments_raw[i] = argument_columns[i].get();

        chassert(!argument_columns.empty());
        addToNested(0, argument_columns[0]->size(), place, arguments_raw.data(), arena);
    }

public:
    AggregateFunctionDistinct(
        AggregateFunctionPtr nested_func_, const DataTypes & arguments, const Array & params_, bool preserve_input_order_)
    : IAggregateFunctionDataHelper<Data, AggregateFunctionDistinct>(arguments, params_, nested_func_->getResultType())
    , nested_func(nested_func_)
    , arguments_num(arguments.size())
    , preserve_input_order(preserve_input_order_)
    {
        size_t nested_size = nested_func->alignOfData();
        prefix_size = (sizeof(Data) + nested_size - 1) / nested_size * nested_size;
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        bool added = this->data(place).add(columns, arguments_num, row_num, arena);
        if (added)
            addToNested(row_num, row_num + 1, place, columns, arena);
    }

    void addBatchSinglePlace(
        size_t row_begin, size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** __restrict columns,
        Arena * arena,
        ssize_t if_argument_pos) const override
    {
        auto & data = this->data(place);

        /// Detect whether Data provides the single-column `add(IColumn &, row)` overload.
        constexpr bool is_single_numeric = requires(Data & d, const IColumn & c, size_t r)
        {
            { d.add(c, r) } -> std::same_as<bool>;
        };

        if (if_argument_pos >= 0)
        {
            const auto & flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
            if constexpr (is_single_numeric)
            {
                const IColumn & column = *columns[0];
                for (size_t row = row_begin; row < row_end; ++row)
                    if (flags[row] && data.add(column, row))
                        addToNested(row, row + 1, place, columns, arena);
            }
            else
            {
                for (size_t row = row_begin; row < row_end; ++row)
                    if (flags[row] && data.add(columns, arguments_num, row, arena))
                        addToNested(row, row + 1, place, columns, arena);
            }
        }
        else
        {
            if constexpr (is_single_numeric)
            {
                const IColumn & column = *columns[0];
                for (size_t row = row_begin; row < row_end; ++row)
                    if (data.add(column, row))
                        addToNested(row, row + 1, place, columns, arena);
            }
            else
            {
                for (size_t row = row_begin; row < row_end; ++row)
                    if (data.add(columns, arguments_num, row, arena))
                        addToNested(row, row + 1, place, columns, arena);
            }
        }
    }


    /// Keep sparse rows in input order for order-dependent aggregates wrapped in DISTINCT.
    void addBatchSparseSinglePlace(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        Arena * arena) const override
    {
        if (!preserve_input_order)
        {
            IAggregateFunctionHelper<AggregateFunctionDistinct<Data>>::addBatchSparseSinglePlace(
                row_begin, row_end, place, columns, arena);
            return;
        }

        const auto & column_sparse = assert_cast<const ColumnSparse &>(*columns[0]);
        const auto * values = &column_sparse.getValuesColumn();
        auto offset_it = column_sparse.getIterator(row_begin);

        for (size_t row = row_begin; row < row_end; ++row, ++offset_it)
            add(place, &values, offset_it.getValueIndex(), arena);
    }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        auto argument_columns = prepareArgumentColumns();
        this->data(place).merge(this->data(rhs), argument_columns, arena);
        addToNested(place, argument_columns, arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * arena) const override
    {
        this->data(place).deserialize(buf, arena);
        /// Ordered Distinct states serialize only their unique keys. Rebuild the
        /// nested product in the same order when loading a persisted state.
        if constexpr (requires(const Data & data, MutableColumns & columns) { data.insertOrderedValues(columns); })
        {
            auto argument_columns = prepareArgumentColumns();
            this->data(place).insertOrderedValues(argument_columns);
            addToNested(place, argument_columns, arena);
        }
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        nested_func->insertResultInto(getNestedPlace(place), to, arena);
    }

    void insertMergeResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        nested_func->insertMergeResultInto(getNestedPlace(place), to, arena);
    }

    void rollbackInsertResult(ConstAggregateDataPtr __restrict place, IColumn & to) const noexcept override
    {
        nested_func->rollbackInsertResult(getNestedPlace(place), to);
    }

    size_t sizeOfData() const override
    {
        return prefix_size + nested_func->sizeOfData();
    }

    size_t alignOfData() const override
    {
        return std::max(alignof(Data), nested_func->alignOfData());
    }

    void create(AggregateDataPtr __restrict place) const override
    {
        new (place) Data;
        nested_func->create(getNestedPlace(place));
    }

    void destroy(AggregateDataPtr __restrict place) const noexcept override
    {
        this->data(place).~Data();
        nested_func->destroy(getNestedPlace(place));
    }

    bool hasTrivialDestructor() const override
    {
        return std::is_trivially_destructible_v<Data> && nested_func->hasTrivialDestructor();
    }

    void destroyUpToState(AggregateDataPtr __restrict place) const noexcept override
    {
        this->data(place).~Data();
        nested_func->destroyUpToState(getNestedPlace(place));
    }

    String getName() const override
    {
        return nested_func->getName() + "Distinct";
    }

    bool allocatesMemoryInArena() const override
    {
        return true;
    }

    bool isState() const override
    {
        return nested_func->isState();
    }

    bool isVersioned() const override
    {
        return nested_func->isVersioned();
    }

    size_t getVersionFromRevision(size_t revision) const override
    {
        return nested_func->getVersionFromRevision(revision);
    }

    size_t getDefaultVersion() const override
    {
        return nested_func->getDefaultVersion();
    }

    DataTypePtr getStateType() const override
    {
        return this->getStateTypeWithVersionOf(*nested_func);
    }

    bool canMergeStateFromDifferentVariant(const IAggregateFunction & rhs) const override
    {
        /// Distinct state contains a history of unique values and can be merged across
        /// variants without reading the nested function state from rhs
        return this->haveSameDefinition(rhs);
    }

    void mergeStateFromDifferentVariant(
        AggregateDataPtr __restrict place,
        const IAggregateFunction & /*rhs*/,
        ConstAggregateDataPtr rhs_place,
        Arena * arena) const override
    {
        this->merge(place, rhs_place, arena);
    }

    AggregateFunctionPtr getNestedFunction() const override { return nested_func; }

    AggregateFunctionPtr getOwnNullAdapter(
        const AggregateFunctionPtr & nested_function,
        const DataTypes & arguments,
        const Array & params,
        const AggregateFunctionProperties & /*properties*/) const override
    {
        /// After `Nullable(Tuple)` was introduced, Tuple's `canBeInsideNullable` now returns true,
        /// which changed the default null adapter for Tuple-returning functions:
        ///   - single-arg: from `<false, false>` to `<true, true>` (flag byte added to serialization).
        ///   - multi-arg: from `<false, true>` to `<true, true>` (flag byte was already present).
        /// Only single-arg functions are affected because the multi-arg (variadic) Null combinator
        /// always serialized the flag byte unconditionally, so its serialization format did not change.
        /// Currently, the only single-arg Tuple-returning aggregate function is `sumCount`.
        /// We hardcode the check for `sumCount` rather than matching all single-arg Tuple-returning
        /// functions, so that future functions with the same shape get the correct new behavior
        /// (`<true, true>`) by default and are not silently forced into the legacy adapter.
        if (nested_func->getName() == "sumCount")
            return std::make_shared<AggregateFunctionNullUnary<false, false>>(nested_function, arguments, params);
        return nullptr;
    }
};

}
