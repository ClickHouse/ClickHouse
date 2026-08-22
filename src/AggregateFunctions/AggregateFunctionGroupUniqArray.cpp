#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Combinators/AggregateFunctionNull.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeIPv4andIPv6.h>
#include <DataTypes/DataTypeTuple.h>

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpersArena.h>

#include <DataTypes/DataTypeArray.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>

#include <Common/Allocator.h>
#include <Common/HashTable/HashSet.h>
#include <Common/HashTable/HashTableKeyHolder.h>
#include <Common/assert_cast.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/KeyHolderHelpers.h>

#include <new>


namespace DB
{

struct Settings;

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_DATA;
}

namespace
{

DataTypePtr createGroupUniqArrayResultType(const DataTypePtr & element_type, bool with_overflow_marker)
{
    auto values_type = std::make_shared<DataTypeArray>(element_type);
    if (!with_overflow_marker)
        return values_type;

    return std::make_shared<DataTypeTuple>(
        DataTypes{std::move(values_type), DataTypeFactory::instance().get("Bool")},
        Names{"values", "overflowed"});
}

template <typename T, bool discard_on_overflow = false>
struct AggregateFunctionGroupUniqArrayData
{
    /// CRC32 for integer keys, like uniqExact.
    using Hash = std::conditional_t<is_integer<T>, HashCRC32<T>, DefaultHash<T>>;

    /// When creating, the hash table must be small.
    using Set = HashSetWithStackMemory<T, Hash, 4>;

    Set value;
};

template <typename T>
struct AggregateFunctionGroupUniqArrayData<T, true>
{
    /// CRC32 for integer keys, like uniqExact.
    using Hash = std::conditional_t<is_integer<T>, HashCRC32<T>, DefaultHash<T>>;

    /// When creating, the hash table must be small.
    using Set = HashSetWithStackMemory<T, Hash, 4>;

    Set value;
    bool overflowed = false;
};


/// Puts all values to the hash set. Returns an array of unique values. Implemented for numeric types.
template <typename T, typename LimitNumElems, bool discard_on_overflow = false>
class AggregateFunctionGroupUniqArray
    : public IAggregateFunctionDataHelper<
          AggregateFunctionGroupUniqArrayData<T, discard_on_overflow>,
          AggregateFunctionGroupUniqArray<T, LimitNumElems, discard_on_overflow>>
{
    static constexpr bool limit_num_elems = LimitNumElems::value;
    UInt64 max_elems;

private:
    using State = AggregateFunctionGroupUniqArrayData<T, discard_on_overflow>;

    static void setOverflowed(State & state)
    {
        if constexpr (discard_on_overflow)
        {
            state.value.clearAndShrink();
            state.overflowed = true;
        }
    }

public:
    AggregateFunctionGroupUniqArray(
        const DataTypePtr & argument_type,
        const Array & parameters_,
        UInt64 max_elems_ = std::numeric_limits<UInt64>::max())
        : IAggregateFunctionDataHelper<AggregateFunctionGroupUniqArrayData<T, discard_on_overflow>,
              AggregateFunctionGroupUniqArray<T, LimitNumElems, discard_on_overflow>>(
              {argument_type}, parameters_, createGroupUniqArrayResultType(argument_type, discard_on_overflow))
        , max_elems(max_elems_)
    {
    }

    AggregateFunctionGroupUniqArray(
        const DataTypePtr & argument_type,
        const Array & parameters_,
        const DataTypePtr & result_type_,
        UInt64 max_elems_ = std::numeric_limits<UInt64>::max())
        : IAggregateFunctionDataHelper<AggregateFunctionGroupUniqArrayData<T, discard_on_overflow>,
              AggregateFunctionGroupUniqArray<T, LimitNumElems, discard_on_overflow>>({argument_type}, parameters_, result_type_)
        , max_elems(max_elems_)
    {
    }

    String getName() const override { return discard_on_overflow ? "groupUniqArrayUpTo" : "groupUniqArray"; }

    bool allocatesMemoryInArena() const override { return false; }

    AggregateFunctionPtr getOwnNullAdapter(
        const AggregateFunctionPtr & nested_function,
        const DataTypes & arguments,
        const Array & params,
        const AggregateFunctionProperties &) const final
    {
        if constexpr (discard_on_overflow)
            return std::make_shared<AggregateFunctionNullUnary<false, false>>(nested_function, arguments, params);
        return nullptr;
    }

    /// `final` devirtualizes the per-row call in addBatchSinglePlace (this class has subclasses).
    void ALWAYS_INLINE add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const final
    {
        auto & state = this->data(place);

        if constexpr (discard_on_overflow)
        {
            if (state.overflowed)
                return;

            const auto value = assert_cast<const ColumnVector<T> &>(*columns[0]).getData()[row_num];
            if (state.value.size() >= max_elems)
            {
                if (state.value.find(value) == nullptr)
                    setOverflowed(state);
                return;
            }

            state.value.insert(value);
        }
        else
        {
            if (limit_num_elems && state.value.size() >= max_elems)
                return;
            state.value.insert(assert_cast<const ColumnVector<T> &>(*columns[0]).getData()[row_num]);
        }
    }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        auto & state = this->data(place);
        const auto & rhs_state = this->data(rhs);

        if constexpr (discard_on_overflow)
        {
            if (state.overflowed)
                return;

            if (rhs_state.overflowed)
            {
                setOverflowed(state);
                return;
            }

            for (const auto & rhs_elem : rhs_state.value)
            {
                const auto value = rhs_elem.getValue();
                if (state.value.size() >= max_elems)
                {
                    if (state.value.find(value) == nullptr)
                    {
                        setOverflowed(state);
                        return;
                    }
                    continue;
                }

                state.value.insert(value);
            }
        }
        else if (!limit_num_elems)
            state.value.merge(rhs_state.value);
        else
        {
            for (const auto & rhs_elem : rhs_state.value)
            {
                if (state.value.size() >= max_elems)
                    return;
                state.value.insert(rhs_elem.getValue());
            }
        }
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        const auto & state = this->data(place);
        if constexpr (discard_on_overflow)
        {
            writeBinary(static_cast<UInt8>(state.overflowed), buf);
            if (state.overflowed)
                return;
        }

        const auto & set = state.value;
        size_t size = set.size();
        writeVarUInt(size, buf);
        for (const auto & elem : set)
            writeBinaryLittleEndian(elem.key, buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        auto & state = this->data(place);
        if constexpr (discard_on_overflow)
        {
            UInt8 overflowed = 0;
            readBinary(overflowed, buf);
            if (overflowed > 1)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid overflow flag in aggregate function state for {}", getName());

            state.overflowed = overflowed;
            if (state.overflowed)
            {
                state.value.clearAndShrink();
                return;
            }

            size_t size = 0;
            readVarUInt(size, buf);
            if (size > max_elems)
                throw Exception(
                    ErrorCodes::INCORRECT_DATA,
                    "Too many elements ({}) in aggregate function state for {}, maximum: {}",
                    size,
                    getName(),
                    max_elems);

            for (size_t i = 0; i < size; ++i)
            {
                T value{};
                readBinaryLittleEndian(value, buf);
                state.value.insert(value);
            }

            return;
        }

        state.value.read(buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        IColumn * values_to = &to;
        if constexpr (discard_on_overflow)
        {
            auto & tuple_to = assert_cast<ColumnTuple &>(to);
            values_to = &tuple_to.getColumn(0);
        }

        ColumnArray & arr_to = assert_cast<ColumnArray &>(*values_to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();

        const auto & state = this->data(place);
        const typename State::Set & set = state.value;
        size_t size = set.size();

        offsets_to.push_back(offsets_to.back() + size);

        typename ColumnVector<T>::Container & data_to = assert_cast<ColumnVector<T> &>(arr_to.getData()).getData();
        size_t old_size = data_to.size();
        data_to.resize(old_size + size);

        size_t i = 0;
        for (auto it = set.begin(); it != set.end(); ++it, ++i)
            data_to[old_size + i] = it->getValue();

        if constexpr (discard_on_overflow)
        {
            auto & tuple_to = assert_cast<ColumnTuple &>(to);
            auto & overflowed_to = assert_cast<ColumnUInt8 &>(tuple_to.getColumn(1)).getData();
            overflowed_to.push_back(state.overflowed);
        }
    }
};


/// Generic implementation, it uses serialized representation as object descriptor.
/// The overflow-discarding variant owns serialized keys instead of keeping them in the shared aggregation Arena,
/// so all key memory can be released as soon as the state overflows.
class GroupUniqArrayReleasableKeys
{
    static constexpr size_t inline_size = 16;

    char inline_data[inline_size]{};
    char * heap_data = nullptr;
    size_t heap_capacity = 0;
    size_t data_size = 0;

    char * data() { return heap_data ? heap_data : inline_data; }
    size_t capacity() const { return heap_data ? heap_capacity : inline_size; }

    template <typename Set>
    void grow(size_t required_capacity, Set & set)
    {
        size_t new_capacity = capacity();
        while (new_capacity < required_capacity)
        {
            if (new_capacity > std::numeric_limits<size_t>::max() / 2)
            {
                new_capacity = required_capacity;
                break;
            }
            new_capacity *= 2;
        }

        Allocator<false> allocator;
        auto * new_data = static_cast<char *>(allocator.alloc(new_capacity));
        char * old_data = data();
        memcpy(new_data, old_data, data_size);

        /// Hash table cells store views into this buffer. Their saved hashes remain valid after rebasing.
        for (auto & elem : set)
        {
            const auto key = elem.getValue();
            if (key.empty())
                continue;

            chassert(key.data() >= old_data && key.data() + key.size() <= old_data + data_size);
            elem.key = std::string_view(new_data + (key.data() - old_data), key.size());
        }

        if (heap_data)
            allocator.free(heap_data, heap_capacity);

        heap_data = new_data;
        heap_capacity = new_capacity;
    }

public:
    GroupUniqArrayReleasableKeys() = default;
    GroupUniqArrayReleasableKeys(const GroupUniqArrayReleasableKeys &) = delete;
    GroupUniqArrayReleasableKeys & operator=(const GroupUniqArrayReleasableKeys &) = delete;

    ~GroupUniqArrayReleasableKeys()
    {
        clear();
    }

    template <typename Set>
    std::string_view store(std::string_view key, Set & set)
    {
        if (key.empty())
            return {};

        size_t required_capacity = 0;
        if (__builtin_add_overflow(data_size, key.size(), &required_capacity))
            throw std::bad_alloc();

        if (required_capacity > capacity())
            grow(required_capacity, set);

        char * destination = data() + data_size;
        memcpy(destination, key.data(), key.size());
        data_size = required_capacity;
        return std::string_view(destination, key.size());
    }

    void clear()
    {
        if (heap_data)
        {
            Allocator<false> allocator;
            allocator.free(heap_data, heap_capacity);
            heap_data = nullptr;
            heap_capacity = 0;
        }
        data_size = 0;
    }
};

template <bool discard_on_overflow = false>
struct AggregateFunctionGroupUniqArrayGenericData
{
    static constexpr size_t INITIAL_SIZE_DEGREE = 3; /// adjustable

    using Set = HashSetWithSavedHashWithStackMemory<std::string_view, StringViewHash,
        INITIAL_SIZE_DEGREE>;

    Set value;
};

template <>
struct AggregateFunctionGroupUniqArrayGenericData<true>
{
    static constexpr size_t INITIAL_SIZE_DEGREE = 3; /// adjustable

    using Set = HashSetWithSavedHashWithStackMemory<std::string_view, StringViewHash,
        INITIAL_SIZE_DEGREE>;

    GroupUniqArrayReleasableKeys keys;
    Set value;
    bool overflowed = false;

    std::string_view storeKey(std::string_view key)
    {
        return keys.store(key, value);
    }

    void clearAndShrink()
    {
        value.clearAndShrink();
        keys.clear();
    }
};

template <bool is_plain_column>
static void deserializeAndInsertImpl(std::string_view str, IColumn & data_to);

/** Template parameter with true value should be used for columns that store their elements in memory continuously.
 *  For such columns groupUniqArray() can be implemented more efficiently (especially for small numeric arrays).
 */
template <bool is_plain_column = false, typename LimitNumElems = std::false_type, bool discard_on_overflow = false>
class AggregateFunctionGroupUniqArrayGeneric final
    : public IAggregateFunctionDataHelper<AggregateFunctionGroupUniqArrayGenericData<discard_on_overflow>,
        AggregateFunctionGroupUniqArrayGeneric<is_plain_column, LimitNumElems, discard_on_overflow>>
{
    DataTypePtr & input_data_type;

    static constexpr bool limit_num_elems = LimitNumElems::value;
    UInt64 max_elems;

    using State = AggregateFunctionGroupUniqArrayGenericData<discard_on_overflow>;

    static void setOverflowed(State & state)
    {
        if constexpr (discard_on_overflow)
        {
            state.clearAndShrink();
            state.overflowed = true;
        }
    }

public:
    AggregateFunctionGroupUniqArrayGeneric(
        const DataTypePtr & input_data_type_,
        const Array & parameters_,
        UInt64 max_elems_ = std::numeric_limits<UInt64>::max())
        : IAggregateFunctionDataHelper<
              AggregateFunctionGroupUniqArrayGenericData<discard_on_overflow>,
              AggregateFunctionGroupUniqArrayGeneric<is_plain_column, LimitNumElems, discard_on_overflow>>(
              {input_data_type_}, parameters_, createGroupUniqArrayResultType(input_data_type_, discard_on_overflow))
        , input_data_type(this->argument_types[0])
        , max_elems(max_elems_)
    {
    }

    String getName() const override { return discard_on_overflow ? "groupUniqArrayUpTo" : "groupUniqArray"; }

    bool allocatesMemoryInArena() const override
    {
        return true;
    }

    AggregateFunctionPtr getOwnNullAdapter(
        const AggregateFunctionPtr & nested_function,
        const DataTypes & arguments,
        const Array & params,
        const AggregateFunctionProperties &) const final
    {
        if constexpr (discard_on_overflow)
            return std::make_shared<AggregateFunctionNullUnary<false, false>>(nested_function, arguments, params);
        return nullptr;
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        const auto & state = this->data(place);
        if constexpr (discard_on_overflow)
        {
            writeBinary(static_cast<UInt8>(state.overflowed), buf);
            if (state.overflowed)
                return;
        }

        const auto & set = state.value;
        writeVarUInt(set.size(), buf);

        for (const auto & elem : set)
        {
            writeStringBinary(elem.getValue(), buf);
        }
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * arena) const override
    {
        auto & state = this->data(place);
        if constexpr (discard_on_overflow)
        {
            UInt8 overflowed = 0;
            readBinary(overflowed, buf);
            if (overflowed > 1)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid overflow flag in aggregate function state for {}", getName());

            state.overflowed = overflowed;
            if (state.overflowed)
            {
                state.clearAndShrink();
                return;
            }
        }

        auto & set = state.value;
        size_t size = 0;
        readVarUInt(size, buf);
        if constexpr (discard_on_overflow)
        {
            if (size > max_elems)
                throw Exception(
                    ErrorCodes::INCORRECT_DATA,
                    "Too many elements ({}) in aggregate function state for {}, maximum: {}",
                    size,
                    getName(),
                    max_elems);
        }

        if constexpr (discard_on_overflow)
        {
            for (size_t i = 0; i < size; ++i)
            {
                String key;
                readStringBinary(key, buf);

                const std::string_view key_view = key;
                const size_t hash = StringViewHash{}(key_view);
                if (set.find(key_view, hash) != nullptr)
                    continue;

                auto stored_key = state.storeKey(key_view);
                typename State::Set::LookupResult it = nullptr;
                bool inserted = false;
                set.emplace(stored_key, it, inserted, hash);
                chassert(inserted);
            }
        }
        else
        {
            for (size_t i = 0; i < size; ++i)
                set.insert(readStringBinaryInto(*arena, buf));
        }
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        auto & state = this->data(place);
        if constexpr (discard_on_overflow)
        {
            if (state.overflowed)
                return;

            auto key_holder = getKeyHolder<is_plain_column>(*columns[0], row_num, *arena);
            const auto key = keyHolderGetKey(key_holder);
            const size_t hash = StringViewHash{}(key);

            if (state.value.find(key, hash) != nullptr)
            {
                keyHolderDiscardKey(key_holder);
                return;
            }

            if (state.value.size() >= max_elems)
            {
                keyHolderDiscardKey(key_holder);
                setOverflowed(state);
                return;
            }

            auto stored_key = state.storeKey(key);
            keyHolderDiscardKey(key_holder);

            bool inserted = false;
            typename State::Set::LookupResult it = nullptr;
            state.value.emplace(stored_key, it, inserted, hash);
            chassert(inserted);
            return;
        }
        else
        {
            if (limit_num_elems && state.value.size() >= max_elems)
                return;

            bool inserted = false;
            typename State::Set::LookupResult it = nullptr;
            auto key_holder = getKeyHolder<is_plain_column>(*columns[0], row_num, *arena);
            state.value.emplace(key_holder, it, inserted);
        }
    }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        auto & state = this->data(place);
        const auto & rhs_state = this->data(rhs);

        if constexpr (discard_on_overflow)
        {
            if (state.overflowed)
                return;

            if (rhs_state.overflowed)
            {
                setOverflowed(state);
                return;
            }

            for (const auto & rhs_elem : rhs_state.value)
            {
                const auto key = rhs_elem.getValue();
                const size_t hash = StringViewHash{}(key);
                if (state.value.find(key, hash) != nullptr)
                    continue;

                if (state.value.size() >= max_elems)
                {
                    setOverflowed(state);
                    return;
                }

                auto stored_key = state.storeKey(key);
                bool inserted = false;
                typename State::Set::LookupResult it = nullptr;
                state.value.emplace(stored_key, it, inserted, hash);
                chassert(inserted);
            }
            return;
        }

        bool inserted = false;
        typename State::Set::LookupResult it = nullptr;
        for (const auto & rhs_elem : rhs_state.value)
        {
            if (limit_num_elems && state.value.size() >= max_elems)
                return;

            // We have to copy the keys to our arena.
            chassert(arena != nullptr);
            state.value.emplace(ArenaKeyHolder{rhs_elem.getValue(), *arena}, it, inserted);
        }
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        IColumn * values_to = &to;
        if constexpr (discard_on_overflow)
        {
            auto & tuple_to = assert_cast<ColumnTuple &>(to);
            values_to = &tuple_to.getColumn(0);
        }

        ColumnArray & arr_to = assert_cast<ColumnArray &>(*values_to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();
        IColumn & data_to = arr_to.getData();

        const auto & state = this->data(place);
        const auto & set = state.value;
        offsets_to.push_back(offsets_to.back() + set.size());

        for (auto & elem : set)
            deserializeAndInsert<is_plain_column>(elem.getValue(), data_to);

        if constexpr (discard_on_overflow)
        {
            auto & tuple_to = assert_cast<ColumnTuple &>(to);
            auto & overflowed_to = assert_cast<ColumnUInt8 &>(tuple_to.getColumn(1)).getData();
            overflowed_to.push_back(state.overflowed);
        }
    }
};


/// Substitute return type for Date and DateTime
template <typename HasLimit, bool discard_on_overflow = false>
class AggregateFunctionGroupUniqArrayDate final
    : public AggregateFunctionGroupUniqArray<DataTypeDate::FieldType, HasLimit, discard_on_overflow>
{
public:
    explicit AggregateFunctionGroupUniqArrayDate(
        const DataTypePtr & argument_type,
        const Array & parameters_,
        UInt64 max_elems_ = std::numeric_limits<UInt64>::max())
        : AggregateFunctionGroupUniqArray<DataTypeDate::FieldType, HasLimit, discard_on_overflow>(
              argument_type, parameters_, createResultType(), max_elems_)
    {
    }
    static DataTypePtr createResultType()
    {
        return createGroupUniqArrayResultType(std::make_shared<DataTypeDate>(), discard_on_overflow);
    }
};

template <typename HasLimit, bool discard_on_overflow = false>
class AggregateFunctionGroupUniqArrayDateTime final
    : public AggregateFunctionGroupUniqArray<DataTypeDateTime::FieldType, HasLimit, discard_on_overflow>
{
public:
    explicit AggregateFunctionGroupUniqArrayDateTime(
        const DataTypePtr & argument_type,
        const Array & parameters_,
        UInt64 max_elems_ = std::numeric_limits<UInt64>::max())
        : AggregateFunctionGroupUniqArray<DataTypeDateTime::FieldType, HasLimit, discard_on_overflow>(
              argument_type, parameters_, createResultType(), max_elems_)
    {
    }
    static DataTypePtr createResultType()
    {
        return createGroupUniqArrayResultType(std::make_shared<DataTypeDateTime>(), discard_on_overflow);
    }
};

template <typename HasLimit, bool discard_on_overflow = false>
class AggregateFunctionGroupUniqArrayIPv4 final
    : public AggregateFunctionGroupUniqArray<DataTypeIPv4::FieldType, HasLimit, discard_on_overflow>
{
public:
    explicit AggregateFunctionGroupUniqArrayIPv4(
        const DataTypePtr & argument_type,
        const Array & parameters_,
        UInt64 max_elems_ = std::numeric_limits<UInt64>::max())
        : AggregateFunctionGroupUniqArray<DataTypeIPv4::FieldType, HasLimit, discard_on_overflow>(
              argument_type, parameters_, createResultType(), max_elems_)
    {
    }
    static DataTypePtr createResultType()
    {
        return createGroupUniqArrayResultType(std::make_shared<DataTypeIPv4>(), discard_on_overflow);
    }
};

template <typename HasLimit, bool discard_on_overflow = false, typename ... TArgs>
IAggregateFunction * createWithExtraTypes(const DataTypePtr & argument_type, TArgs && ... args)
{
    WhichDataType which(argument_type);
    if (which.idx == TypeIndex::Date)
        return new AggregateFunctionGroupUniqArrayDate<HasLimit, discard_on_overflow>(argument_type, args...);
    if (which.idx == TypeIndex::DateTime)
        return new AggregateFunctionGroupUniqArrayDateTime<HasLimit, discard_on_overflow>(argument_type, args...);
    if (which.idx == TypeIndex::IPv4)
        return new AggregateFunctionGroupUniqArrayIPv4<HasLimit, discard_on_overflow>(argument_type, args...);

    /// Check that we can use plain version of AggregateFunctionGroupUniqArrayGeneric
    if (argument_type->isValueUnambiguouslyRepresentedInContiguousMemoryRegion())
        return new AggregateFunctionGroupUniqArrayGeneric<true, HasLimit, discard_on_overflow>(argument_type, args...);
    return new AggregateFunctionGroupUniqArrayGeneric<false, HasLimit, discard_on_overflow>(argument_type, args...);
}

template <typename T, typename LimitNumElems>
using AggregateFunctionGroupUniqArrayUpTo = AggregateFunctionGroupUniqArray<T, LimitNumElems, true>;

template <typename HasLimit, bool discard_on_overflow = false, typename ... TArgs>
inline AggregateFunctionPtr createAggregateFunctionGroupUniqArrayImpl(
    const std::string & name, const DataTypePtr & argument_type, TArgs ... args)
{
    AggregateFunctionPtr res;
    if constexpr (discard_on_overflow)
        res = AggregateFunctionPtr(createWithNumericType<AggregateFunctionGroupUniqArrayUpTo, HasLimit, const DataTypePtr &>(
            *argument_type, argument_type, args...));
    else
        res = AggregateFunctionPtr(createWithNumericType<AggregateFunctionGroupUniqArray, HasLimit, const DataTypePtr &>(
            *argument_type, argument_type, args...));

    if (!res)
        res = AggregateFunctionPtr(createWithExtraTypes<HasLimit, discard_on_overflow>(argument_type, args...));

    if (!res)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument for aggregate function {}",
                        argument_type->getName(), name);

    return res;

}

AggregateFunctionPtr createAggregateFunctionGroupUniqArray(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertUnary(name, argument_types);

    bool limit_size = false;
    UInt64 max_elems = std::numeric_limits<UInt64>::max();

    if (parameters.empty())
    {
        // no limit
    }
    else if (parameters.size() == 1)
    {
        auto type = parameters[0].getType();
        if (type != Field::Types::Int64 && type != Field::Types::UInt64)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Parameter for aggregate function {} should be positive number", name);

        if ((type == Field::Types::Int64 && parameters[0].safeGet<Int64>() <= 0) ||
            (type == Field::Types::UInt64 && parameters[0].safeGet<UInt64>() == 0))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Parameter for aggregate function {} should be positive number", name);

        limit_size = true;
        max_elems = parameters[0].safeGet<UInt64>();
    }
    else
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                        "Incorrect number of parameters for aggregate function {}, should be 0 or 1", name);

    if (!limit_size)
        return createAggregateFunctionGroupUniqArrayImpl<std::false_type>(name, argument_types[0], parameters);
    return createAggregateFunctionGroupUniqArrayImpl<std::true_type>(name, argument_types[0], parameters, max_elems);
}

AggregateFunctionPtr createAggregateFunctionGroupUniqArrayUpTo(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertUnary(name, argument_types);

    if (parameters.size() != 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                        "Incorrect number of parameters for aggregate function {}, should be 1", name);

    auto type = parameters[0].getType();
    if (type != Field::Types::Int64 && type != Field::Types::UInt64)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Parameter for aggregate function {} should be positive number", name);

    if ((type == Field::Types::Int64 && parameters[0].safeGet<Int64>() <= 0)
        || (type == Field::Types::UInt64 && parameters[0].safeGet<UInt64>() == 0))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Parameter for aggregate function {} should be positive number", name);

    UInt64 max_elems = parameters[0].safeGet<UInt64>();
    return createAggregateFunctionGroupUniqArrayImpl<std::true_type, true>(name, argument_types[0], parameters, max_elems);
}

}

void registerAggregateFunctionGroupUniqArray(AggregateFunctionFactory & factory);
void registerAggregateFunctionGroupUniqArray(AggregateFunctionFactory & factory)
{
    FunctionDocumentation::Description description = R"(
Creates an array from different argument values.
The memory consumption of this function is the same as for the [`uniqExact`](/reference/functions/aggregate-functions/uniqExact) function.
    )";
    FunctionDocumentation::Syntax syntax = R"(
groupUniqArray(x)
groupUniqArray(max_size)(x)
    )";
    FunctionDocumentation::Arguments arguments = {
        {"x", "Expression.", {"Any"}}
    };
    FunctionDocumentation::Parameters parameters = {
        {"max_size", "Limits the size of the resulting array to `max_size` elements. `groupUniqArray(1)(x)` is equivalent to `[any(x)]`.", {"UInt64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns an array of unique values.", {"Array"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        R"(
CREATE TABLE t (x UInt8) ENGINE = Memory;
INSERT INTO t VALUES (1), (2), (1), (3), (2), (4);

SELECT groupUniqArray(x) FROM t;
        )",
        R"(
┌─groupUniqArray(x)─┐
│ [1,2,3,4]         │
└───────────────────┘
        )"
    },
    {
        "With max_size parameter",
        R"(
SELECT groupUniqArray(2)(x) FROM t;
        )",
        R"(
┌─groupUniqArray(2)(x)─┐
│ [1,2]                │
└──────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation = {description, syntax, arguments, parameters, returned_value, examples, introduced_in, category};

    AggregateFunctionProperties properties = { .returns_default_when_only_null = false, .is_order_dependent = true };

    factory.registerFunction("groupUniqArray", { createAggregateFunctionGroupUniqArray, documentation, properties });

    FunctionDocumentation::Description up_to_description = R"(
Collects distinct argument values when their number does not exceed `max_size`.
If more than `max_size` distinct values are encountered, the function discards its accumulated values.
The overflow state is preserved when aggregate states are merged.
    )";
    FunctionDocumentation::Syntax up_to_syntax = R"(
groupUniqArrayUpTo(max_size)(x)
    )";
    FunctionDocumentation::Arguments up_to_arguments = {
        {"x", "Expression.", {"Any"}}
    };
    FunctionDocumentation::Parameters up_to_parameters = {
        {"max_size", "Maximum number of distinct values allowed in the resulting array.", {"UInt64"}}
    };
    FunctionDocumentation::ReturnedValue up_to_returned_value = {
        "Returns a named tuple `(values, overflowed)`. If the number of distinct values does not exceed `max_size`, `values` "
        "contains all distinct values and `overflowed` is false. On overflow, `values` is empty and `overflowed` is true.",
        {"Tuple(values Array(T), overflowed Bool)"}};
    FunctionDocumentation::Examples up_to_examples = {
    {
        "Values within the limit",
        R"(
SELECT groupUniqArrayUpTo(3)(x)
FROM values('x UInt8', 1, 2, 1, 3);
        )",
        R"(
┌─groupUniqArrayUpTo(3)(x)─┐
│ ([1,2,3],false)          │
└───────────────────────────┘
        )"
    },
    {
        "Too many distinct values",
        R"(
SELECT groupUniqArrayUpTo(3)(x)
FROM values('x UInt8', 1, 2, 3, 4);
        )",
        R"(
┌─groupUniqArrayUpTo(3)(x)─┐
│ ([],true)                │
└──────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn up_to_introduced_in = {26, 9};
    FunctionDocumentation up_to_documentation = {
        up_to_description,
        up_to_syntax,
        up_to_arguments,
        up_to_parameters,
        up_to_returned_value,
        up_to_examples,
        up_to_introduced_in,
        category};

    AggregateFunctionProperties up_to_properties = { .returns_default_when_only_null = false, .is_order_dependent = true };
    factory.registerFunction("groupUniqArrayUpTo", { createAggregateFunctionGroupUniqArrayUpTo, up_to_documentation, up_to_properties });
}

}
