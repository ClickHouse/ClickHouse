#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/KeyHolderHelpers.h>

#include <Columns/ColumnDecimal.h>
#include <DataTypes/DataTypeDate32.h>
#include <Common/HashTable/HashMap.h>
#include <Common/assert_cast.h>

#include <IO/ReadHelpers.h>
#include <IO/ReadHelpersArena.h>
#include <IO/WriteHelpers.h>

#include <new>
#include <string_view>
#include <type_traits>
#include <utility>


namespace DB
{

struct Settings;

namespace
{

template <typename T>
ALWAYS_INLINE T normalizeModeKey(T value)
{
    if constexpr (is_floating_point<T>)
    {
        if (value == T{0})
            return T{0};
    }

    return value;
}


template <typename T>
struct AggregateFunctionModeData
{
    /// CRC32 for integer keys, like uniqExact and groupUniqArray.
    using Hash = std::conditional_t<is_integer<T>, HashCRC32<T>, DefaultHash<T>>;

    /// Mode is most useful for low-cardinality values, so keep small states inline.
    using Map = HashMapWithStackMemory<T, UInt64, Hash, 4>;

    Map counts;
};


template <typename T>
class AggregateFunctionModeNumeric final
    : public IAggregateFunctionDataHelper<AggregateFunctionModeData<T>, AggregateFunctionModeNumeric<T>>
{
    using Data = AggregateFunctionModeData<T>;
    using Base = IAggregateFunctionDataHelper<Data, AggregateFunctionModeNumeric<T>>;
    using ColumnType = ColumnVectorOrDecimal<T>;

public:
    explicit AggregateFunctionModeNumeric(const DataTypes & argument_types_)
        : Base(argument_types_, {}, argument_types_[0])
    {
    }

    String getName() const override { return "mode"; }

    bool allocatesMemoryInArena() const override { return false; }

    void ALWAYS_INLINE add(AggregateDataPtr __restrict place, const IColumn ** __restrict columns, size_t row_num, Arena *) const override
    {
        const auto & values = assert_cast<const ColumnType &>(*columns[0]).getData();
        ++this->data(place).counts[normalizeModeKey(values[row_num])];
    }

    void ALWAYS_INLINE addBatchSinglePlace(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** __restrict columns,
        Arena *,
        ssize_t if_argument_pos) const override
    {
        const auto & values = assert_cast<const ColumnType &>(*columns[0]).getData();
        auto & counts = this->data(place).counts;

        if (if_argument_pos >= 0)
        {
            const auto & flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
            for (size_t i = row_begin; i < row_end; ++i)
            {
                if (flags[i])
                    ++counts[normalizeModeKey(values[i])];
            }
        }
        else
        {
            for (size_t i = row_begin; i < row_end; ++i)
                ++counts[normalizeModeKey(values[i])];
        }
    }

    void addBatchSinglePlaceNotNull(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** __restrict columns,
        const UInt8 * __restrict null_map,
        Arena *,
        ssize_t if_argument_pos) const override
    {
        const auto & values = assert_cast<const ColumnType &>(*columns[0]).getData();
        auto & counts = this->data(place).counts;

        if (if_argument_pos >= 0)
        {
            const auto & flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
            for (size_t i = row_begin; i < row_end; ++i)
            {
                if (!null_map[i] && flags[i])
                    ++counts[normalizeModeKey(values[i])];
            }
        }
        else
        {
            for (size_t i = row_begin; i < row_end; ++i)
            {
                if (!null_map[i])
                    ++counts[normalizeModeKey(values[i])];
            }
        }
    }

    void addManyDefaults(AggregateDataPtr __restrict place, const IColumn ** columns, size_t length, Arena *) const override
    {
        if (length == 0)
            return;

        const auto & values = assert_cast<const ColumnType &>(*columns[0]).getData();
        this->data(place).counts[normalizeModeKey(values[0])] += length;
    }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        auto & counts = this->data(place).counts;
        for (const auto & pair : this->data(rhs).counts)
            counts[normalizeModeKey(pair.getKey())] += pair.getMapped();
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).counts.write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        typename Data::Map::Reader reader(buf);
        auto & counts = this->data(place).counts;
        while (reader.next())
        {
            const auto & pair = reader.get();
            counts[normalizeModeKey(pair.first)] += pair.second;
        }
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        const auto & counts = this->data(place).counts;
        if (counts.empty())
        {
            this->result_type->insertDefaultInto(to);
            return;
        }

        UInt64 best_count = 0;
        T best{};

        for (const auto & pair : counts)
        {
            if (pair.getMapped() > best_count)
            {
                best = pair.getKey();
                best_count = pair.getMapped();
            }
        }

        assert_cast<ColumnType &>(to).getData().push_back(best);
    }
};


struct AggregateFunctionModeGenericData
{
    using Map = HashMapWithStackMemory<std::string_view, UInt64, StringViewHash, 4>;

    Map counts;
};


template <typename KeyHolder>
void incrementModeCount(AggregateFunctionModeGenericData::Map & counts, KeyHolder && key_holder, UInt64 amount)
{
    AggregateFunctionModeGenericData::Map::LookupResult it = nullptr;
    bool inserted = false;
    counts.emplace(std::forward<KeyHolder>(key_holder), it, inserted);

    if (inserted)
        new (&it->getMapped()) UInt64(amount);
    else
        it->getMapped() += amount;
}


template <bool is_plain_column>
class AggregateFunctionModeGeneric final
    : public IAggregateFunctionDataHelper<AggregateFunctionModeGenericData, AggregateFunctionModeGeneric<is_plain_column>>
{
    using Data = AggregateFunctionModeGenericData;
    using Base = IAggregateFunctionDataHelper<Data, AggregateFunctionModeGeneric<is_plain_column>>;

public:
    explicit AggregateFunctionModeGeneric(const DataTypes & argument_types_)
        : Base(argument_types_, {}, argument_types_[0])
    {
    }

    String getName() const override { return "mode"; }

    bool allocatesMemoryInArena() const override { return true; }

    void add(AggregateDataPtr __restrict place, const IColumn ** __restrict columns, size_t row_num, Arena * arena) const override
    {
        auto key_holder = getKeyHolder<is_plain_column>(*columns[0], row_num, *arena);
        incrementModeCount(this->data(place).counts, std::move(key_holder), 1);
    }

    void addBatchSinglePlace(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** __restrict columns,
        Arena * arena,
        ssize_t if_argument_pos) const override
    {
        if (if_argument_pos >= 0)
        {
            const auto & flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
            for (size_t i = row_begin; i < row_end; ++i)
            {
                if (flags[i])
                    add(place, columns, i, arena);
            }
        }
        else
        {
            for (size_t i = row_begin; i < row_end; ++i)
                add(place, columns, i, arena);
        }
    }

    void addBatchSinglePlaceNotNull(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** __restrict columns,
        const UInt8 * __restrict null_map,
        Arena * arena,
        ssize_t if_argument_pos) const override
    {
        if (if_argument_pos >= 0)
        {
            const auto & flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
            for (size_t i = row_begin; i < row_end; ++i)
            {
                if (!null_map[i] && flags[i])
                    add(place, columns, i, arena);
            }
        }
        else
        {
            for (size_t i = row_begin; i < row_end; ++i)
            {
                if (!null_map[i])
                    add(place, columns, i, arena);
            }
        }
    }

    void addManyDefaults(AggregateDataPtr __restrict place, const IColumn ** columns, size_t length, Arena * arena) const override
    {
        if (length == 0)
            return;

        auto key_holder = getKeyHolder<is_plain_column>(*columns[0], 0, *arena);
        incrementModeCount(this->data(place).counts, std::move(key_holder), length);
    }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        auto & counts = this->data(place).counts;
        for (const auto & pair : this->data(rhs).counts)
            incrementModeCount(counts, ArenaKeyHolder{pair.getKey(), *arena}, pair.getMapped());
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        const auto & counts = this->data(place).counts;
        writeVarUInt(counts.size(), buf);
        for (const auto & pair : counts)
        {
            writeStringBinary(pair.getKey(), buf);
            writeBinaryLittleEndian(pair.getMapped(), buf);
        }
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * arena) const override
    {
        auto & counts = this->data(place).counts;
        size_t size = 0;
        readVarUInt(size, buf);

        for (size_t i = 0; i < size; ++i)
        {
            auto key = readStringBinaryInto(*arena, buf);
            UInt64 count = 0;
            readBinaryLittleEndian(count, buf);
            incrementModeCount(counts, SerializedKeyHolder{key, *arena}, count);
        }
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        const auto & counts = this->data(place).counts;
        if (counts.empty())
        {
            this->result_type->insertDefaultInto(to);
            return;
        }

        UInt64 best_count = 0;
        std::string_view best{};

        for (const auto & pair : counts)
        {
            if (pair.getMapped() > best_count)
            {
                best = pair.getKey();
                best_count = pair.getMapped();
            }
        }

        deserializeAndInsert<is_plain_column>(best, to);
    }
};


template <typename T>
AggregateFunctionPtr createAggregateFunctionModeNumeric(const DataTypes & argument_types)
{
    return std::make_shared<AggregateFunctionModeNumeric<T>>(argument_types);
}


AggregateFunctionPtr
createAggregateFunctionMode(const String & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    assertUnary(name, argument_types);

    const auto & argument_type = *argument_types[0];

    AggregateFunctionPtr result(createWithNumericBasedType<AggregateFunctionModeNumeric>(argument_type, argument_types));
    if (!result)
        result.reset(createWithDecimalType<AggregateFunctionModeNumeric>(argument_type, argument_types));

    if (!result && WhichDataType(argument_type).isDate32())
        result = createAggregateFunctionModeNumeric<DataTypeDate32::FieldType>(argument_types);

    if (result)
        return result;

    if (argument_type.isValueUnambiguouslyRepresentedInContiguousMemoryRegion())
        return std::make_shared<AggregateFunctionModeGeneric<true>>(argument_types);

    return std::make_shared<AggregateFunctionModeGeneric<false>>(argument_types);
}

}

void registerAggregateFunctionMode(AggregateFunctionFactory & factory);
void registerAggregateFunctionMode(AggregateFunctionFactory & factory)
{
    FunctionDocumentation::Description description = R"(
Returns the most frequent non-NULL value in a group.

If multiple values have the same maximum frequency, any of them may be returned. The result is non-deterministic in case of ties.
    )";
    FunctionDocumentation::Syntax syntax = R"(
mode(x)
    )";
    FunctionDocumentation::Arguments arguments = {{"x", "Expression.", {"Any"}}};
    FunctionDocumentation::ReturnedValue returned_value
        = {"Returns the most frequent value. The return type matches the input type, except that `LowCardinality` is removed.", {"Any"}};
    FunctionDocumentation::Examples examples
        = {{"Usage example",
            R"(
SELECT mode(x)
FROM values('x UInt8', (1), (2), (2), (3));
        )",
            R"(
┌─mode(x)─┐
│       2 │
└─────────┘
        )"},
           {"Tied values",
            R"(
SELECT mode(x) IN (1, 2)
FROM values('x UInt8', (1), (1), (2), (2));
        )",
            R"(
┌─mode(x) IN (1, 2)─┐
│                 1 │
└───────────────────┘
        )"}};
    FunctionDocumentation::IntroducedIn introduced_in = {26, 10};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    AggregateFunctionProperties properties = {.returns_default_when_only_null = false, .is_order_dependent = true};

    factory.registerFunction("mode", {createAggregateFunctionMode, documentation, properties});
}

}
