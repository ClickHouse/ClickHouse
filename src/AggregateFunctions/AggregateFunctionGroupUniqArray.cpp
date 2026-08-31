#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeIPv4andIPv6.h>

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpersArena.h>

#include <DataTypes/DataTypeArray.h>

#include <Columns/ColumnArray.h>

#include <Common/HashTable/HashSet.h>
#include <Common/HashTable/HashTableKeyHolder.h>
#include <Common/assert_cast.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/KeyHolderHelpers.h>

#include <bit>


namespace DB
{

struct Settings;

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
    extern const int TOO_LARGE_ARRAY_SIZE;
}

namespace
{

template <typename T>
struct AggregateFunctionGroupUniqArrayData
{
    /// CRC32 for integer keys, like uniqExact.
    using Hash = std::conditional_t<is_integer<T>, HashCRC32<T>, DefaultHash<T>>;

    /// When creating, the hash table must be small.
    using Set = HashSetWithStackMemory<T, Hash, 4>;

    Set value;
};


/// Puts all values to the hash set. Returns an array of unique values. Implemented for numeric types.
template <typename T, typename LimitNumElems>
class AggregateFunctionGroupUniqArray
    : public IAggregateFunctionDataHelper<AggregateFunctionGroupUniqArrayData<T>, AggregateFunctionGroupUniqArray<T, LimitNumElems>>
{
    static constexpr bool limit_num_elems = LimitNumElems::value;
    UInt64 max_elems;

private:
    using State = AggregateFunctionGroupUniqArrayData<T>;

public:
    AggregateFunctionGroupUniqArray(const DataTypePtr & argument_type, const Array & parameters_, UInt64 max_elems_ = std::numeric_limits<UInt64>::max())
        : IAggregateFunctionDataHelper<AggregateFunctionGroupUniqArrayData<T>,
          AggregateFunctionGroupUniqArray<T, LimitNumElems>>({argument_type}, parameters_, std::make_shared<DataTypeArray>(argument_type)),
          max_elems(max_elems_) {}

    AggregateFunctionGroupUniqArray(const DataTypePtr & argument_type, const Array & parameters_, const DataTypePtr & result_type_, UInt64 max_elems_ = std::numeric_limits<UInt64>::max())
        : IAggregateFunctionDataHelper<AggregateFunctionGroupUniqArrayData<T>,
          AggregateFunctionGroupUniqArray<T, LimitNumElems>>({argument_type}, parameters_, result_type_),
          max_elems(max_elems_) {}

    String getName() const override { return "groupUniqArray"; }

    bool allocatesMemoryInArena() const override { return false; }

    /// `final` devirtualizes the per-row call in addBatchSinglePlace (this class has subclasses).
    void ALWAYS_INLINE add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const final
    {
        if (limit_num_elems && this->data(place).value.size() >= max_elems)
            return;
        this->data(place).value.insert(assert_cast<const ColumnVector<T> &>(*columns[0]).getData()[row_num]);
    }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        if (!limit_num_elems)
            this->data(place).value.merge(this->data(rhs).value);
        else
        {
            auto & cur_set = this->data(place).value;
            auto & rhs_set = this->data(rhs).value;

            for (auto & rhs_elem : rhs_set)
            {
                if (cur_set.size() >= max_elems)
                    return;
                cur_set.insert(rhs_elem.getValue());
            }
        }
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        auto & set = this->data(place).value;
        size_t size = set.size();
        writeVarUInt(size, buf);
        for (const auto & elem : set)
            writeBinaryLittleEndian(elem.key, buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        this->data(place).value.read(buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        ColumnArray & arr_to = assert_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();

        const typename State::Set & set = this->data(place).value;
        size_t size = set.size();

        offsets_to.push_back(offsets_to.back() + size);

        typename ColumnVector<T>::Container & data_to = assert_cast<ColumnVector<T> &>(arr_to.getData()).getData();
        size_t old_size = data_to.size();
        data_to.resize(old_size + size);

        size_t i = 0;
        for (auto it = set.begin(); it != set.end(); ++it, ++i)
            data_to[old_size + i] = it->getValue();
    }
};


/// The serialized format (size, then the elements) is shared with the hash set implementation above:
/// the reader does not depend on the order of the elements.
struct AggregateFunctionGroupUniqArrayBitmapData
{
    UInt64 bits[4] = {};

    size_t size() const
    {
        return std::popcount(bits[0]) + std::popcount(bits[1]) + std::popcount(bits[2]) + std::popcount(bits[3]);
    }

    bool contains(UInt8 idx) const { return bits[idx / 64] & (1ULL << (idx % 64)); }
    void set(UInt8 idx) { bits[idx / 64] |= 1ULL << (idx % 64); }

    template <typename F>
    void forEach(F && f) const
    {
        for (size_t word = 0; word < 4; ++word)
        {
            UInt64 w = bits[word];
            while (w)
            {
                f(static_cast<UInt8>(word * 64 + std::countr_zero(w)));
                w &= w - 1;
            }
        }
    }
};


/// Same as AggregateFunctionGroupUniqArray, but for 8-bit types the set of values is a 256-bit bitmap.
template <typename T, typename LimitNumElems>
class AggregateFunctionGroupUniqArrayBitmap final
    : public IAggregateFunctionDataHelper<AggregateFunctionGroupUniqArrayBitmapData, AggregateFunctionGroupUniqArrayBitmap<T, LimitNumElems>>
{
    static_assert(sizeof(T) == 1);

    static constexpr bool limit_num_elems = LimitNumElems::value;
    UInt64 max_elems;

    using Data = AggregateFunctionGroupUniqArrayBitmapData;

public:
    AggregateFunctionGroupUniqArrayBitmap(const DataTypePtr & argument_type, const Array & parameters_, UInt64 max_elems_ = std::numeric_limits<UInt64>::max())
        : IAggregateFunctionDataHelper<Data, AggregateFunctionGroupUniqArrayBitmap<T, LimitNumElems>>({argument_type}, parameters_, std::make_shared<DataTypeArray>(argument_type))
        , max_elems(max_elems_) {}

    String getName() const override { return "groupUniqArray"; }

    bool allocatesMemoryInArena() const override { return false; }

    void ALWAYS_INLINE add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        auto & data = this->data(place);
        UInt8 idx = static_cast<UInt8>(assert_cast<const ColumnVector<T> &>(*columns[0]).getData()[row_num]);
        /// Not just `set`: a store on every row would make consecutive rows serialize on the same memory.
        if (data.contains(idx))
            return;
        if (limit_num_elems && data.size() >= max_elems)
            return;
        data.set(idx);
    }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        auto & cur = this->data(place);
        const auto & other = this->data(rhs);

        if constexpr (!limit_num_elems)
        {
            for (size_t word = 0; word < 4; ++word)
                cur.bits[word] |= other.bits[word];
        }
        else
        {
            size_t cur_size = cur.size();
            for (size_t word = 0; word < 4; ++word)
            {
                UInt64 w = other.bits[word] & ~cur.bits[word];
                while (w)
                {
                    if (cur_size >= max_elems)
                        return;
                    cur.bits[word] |= 1ULL << std::countr_zero(w);
                    ++cur_size;
                    w &= w - 1;
                }
            }
        }
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        const auto & data = this->data(place);
        writeVarUInt(data.size(), buf);
        data.forEach([&](UInt8 idx) { writeBinaryLittleEndian(static_cast<T>(idx), buf); });
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        size_t size = 0;
        readVarUInt(size, buf);
        if (size > 256)
            throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Too many elements ({}) in serialized state of groupUniqArray over an 8-bit type", size);

        auto & data = this->data(place);
        for (size_t i = 0; i < size; ++i)
        {
            T value;
            readBinaryLittleEndian(value, buf);
            data.set(static_cast<UInt8>(value));
        }
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        ColumnArray & arr_to = assert_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();

        const auto & data = this->data(place);
        size_t size = data.size();

        offsets_to.push_back(offsets_to.back() + size);

        typename ColumnVector<T>::Container & data_to = assert_cast<ColumnVector<T> &>(arr_to.getData()).getData();
        size_t old_size = data_to.size();
        data_to.resize(old_size + size);
        T * out = data_to.data() + old_size;
        data.forEach([&](UInt8 idx) { *out++ = static_cast<T>(idx); });
    }
};


/// Generic implementation, it uses serialized representation as object descriptor.
struct AggregateFunctionGroupUniqArrayGenericData
{
    static constexpr size_t INITIAL_SIZE_DEGREE = 3; /// adjustable

    using Set = HashSetWithSavedHashWithStackMemory<std::string_view, StringViewHash,
        INITIAL_SIZE_DEGREE>;

    Set value;
};

template <bool is_plain_column>
static void deserializeAndInsertImpl(std::string_view str, IColumn & data_to);

/** Template parameter with true value should be used for columns that store their elements in memory continuously.
 *  For such columns groupUniqArray() can be implemented more efficiently (especially for small numeric arrays).
 */
template <bool is_plain_column = false, typename LimitNumElems = std::false_type>
class AggregateFunctionGroupUniqArrayGeneric final
    : public IAggregateFunctionDataHelper<AggregateFunctionGroupUniqArrayGenericData,
        AggregateFunctionGroupUniqArrayGeneric<is_plain_column, LimitNumElems>>
{
    DataTypePtr & input_data_type;

    static constexpr bool limit_num_elems = LimitNumElems::value;
    UInt64 max_elems;

    using State = AggregateFunctionGroupUniqArrayGenericData;

public:
    AggregateFunctionGroupUniqArrayGeneric(const DataTypePtr & input_data_type_, const Array & parameters_, UInt64 max_elems_ = std::numeric_limits<UInt64>::max())
        : IAggregateFunctionDataHelper<AggregateFunctionGroupUniqArrayGenericData, AggregateFunctionGroupUniqArrayGeneric<is_plain_column, LimitNumElems>>({input_data_type_}, parameters_, std::make_shared<DataTypeArray>(input_data_type_))
        , input_data_type(this->argument_types[0])
        , max_elems(max_elems_) {}

    String getName() const override { return "groupUniqArray"; }

    bool allocatesMemoryInArena() const override
    {
        return true;
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        auto & set = this->data(place).value;
        writeVarUInt(set.size(), buf);

        for (const auto & elem : set)
        {
            writeStringBinary(elem.getValue(), buf);
        }
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * arena) const override
    {
        auto & set = this->data(place).value;
        size_t size = 0;
        readVarUInt(size, buf);

        for (size_t i = 0; i < size; ++i)
            set.insert(readStringBinaryInto(*arena, buf));
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        auto & set = this->data(place).value;
        if (limit_num_elems && set.size() >= max_elems)
            return;

        bool inserted = false;
        State::Set::LookupResult it = nullptr;
        auto key_holder = getKeyHolder<is_plain_column>(*columns[0], row_num, *arena);
        set.emplace(key_holder, it, inserted);
    }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        auto & cur_set = this->data(place).value;
        auto & rhs_set = this->data(rhs).value;

        bool inserted = false;
        State::Set::LookupResult it = nullptr;
        for (auto & rhs_elem : rhs_set)
        {
            if (limit_num_elems && cur_set.size() >= max_elems)
                return;

            // We have to copy the keys to our arena.
            chassert(arena != nullptr);
            cur_set.emplace(ArenaKeyHolder{rhs_elem.getValue(), *arena}, it, inserted);
        }
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        ColumnArray & arr_to = assert_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();
        IColumn & data_to = arr_to.getData();

        auto & set = this->data(place).value;
        offsets_to.push_back(offsets_to.back() + set.size());

        for (auto & elem : set)
            deserializeAndInsert<is_plain_column>(elem.getValue(), data_to);
    }
};


/// Substitute return type for Date and DateTime
template <typename HasLimit>
class AggregateFunctionGroupUniqArrayDate final : public AggregateFunctionGroupUniqArray<DataTypeDate::FieldType, HasLimit>
{
public:
    explicit AggregateFunctionGroupUniqArrayDate(const DataTypePtr & argument_type, const Array & parameters_, UInt64 max_elems_ = std::numeric_limits<UInt64>::max())
        : AggregateFunctionGroupUniqArray<DataTypeDate::FieldType, HasLimit>(argument_type, parameters_, createResultType(), max_elems_) {}
    static DataTypePtr createResultType() { return std::make_shared<DataTypeArray>(std::make_shared<DataTypeDate>()); }
};

template <typename HasLimit>
class AggregateFunctionGroupUniqArrayDateTime final : public AggregateFunctionGroupUniqArray<DataTypeDateTime::FieldType, HasLimit>
{
public:
    explicit AggregateFunctionGroupUniqArrayDateTime(const DataTypePtr & argument_type, const Array & parameters_, UInt64 max_elems_ = std::numeric_limits<UInt64>::max())
        : AggregateFunctionGroupUniqArray<DataTypeDateTime::FieldType, HasLimit>(argument_type, parameters_, createResultType(), max_elems_) {}
    static DataTypePtr createResultType() { return std::make_shared<DataTypeArray>(std::make_shared<DataTypeDateTime>()); }
};

template <typename HasLimit>
class AggregateFunctionGroupUniqArrayIPv4 final : public AggregateFunctionGroupUniqArray<DataTypeIPv4::FieldType, HasLimit>
{
public:
    explicit AggregateFunctionGroupUniqArrayIPv4(const DataTypePtr & argument_type, const Array & parameters_, UInt64 max_elems_ = std::numeric_limits<UInt64>::max())
        : AggregateFunctionGroupUniqArray<DataTypeIPv4::FieldType, HasLimit>(argument_type, parameters_, createResultType(), max_elems_) {}
    static DataTypePtr createResultType() { return std::make_shared<DataTypeArray>(std::make_shared<DataTypeIPv4>()); }
};

template <typename HasLimit, typename ... TArgs>
IAggregateFunction * createWithExtraTypes(const DataTypePtr & argument_type, TArgs && ... args)
{
    WhichDataType which(argument_type);
    if (which.idx == TypeIndex::Date) return new AggregateFunctionGroupUniqArrayDate<HasLimit>(argument_type, args...);
    if (which.idx == TypeIndex::DateTime)
        return new AggregateFunctionGroupUniqArrayDateTime<HasLimit>(argument_type, args...);
    if (which.idx == TypeIndex::IPv4)
        return new AggregateFunctionGroupUniqArrayIPv4<HasLimit>(argument_type, args...);

    /// Check that we can use plain version of AggregateFunctionGroupUniqArrayGeneric
    if (argument_type->isValueUnambiguouslyRepresentedInContiguousMemoryRegion())
        return new AggregateFunctionGroupUniqArrayGeneric<true, HasLimit>(argument_type, args...);
    return new AggregateFunctionGroupUniqArrayGeneric<false, HasLimit>(argument_type, args...);
}

template <typename HasLimit, typename ... TArgs>
inline AggregateFunctionPtr createAggregateFunctionGroupUniqArrayImpl(const std::string & name, const DataTypePtr & argument_type, TArgs ... args)
{
    /// Must precede createWithNumericType, which also matches these types.
    WhichDataType which(argument_type);
    if (which.isUInt8())
        return std::make_shared<AggregateFunctionGroupUniqArrayBitmap<UInt8, HasLimit>>(argument_type, args...);
    if (which.isInt8() || which.isEnum8())
        return std::make_shared<AggregateFunctionGroupUniqArrayBitmap<Int8, HasLimit>>(argument_type, args...);

    AggregateFunctionPtr res(createWithNumericType<AggregateFunctionGroupUniqArray, HasLimit, const DataTypePtr &>(*argument_type, argument_type, args...));

    if (!res)
        res = AggregateFunctionPtr(createWithExtraTypes<HasLimit>(argument_type, args...));

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
}

}
