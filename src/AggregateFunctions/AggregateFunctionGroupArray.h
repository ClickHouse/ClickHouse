#pragma once

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>

#include <Common/ArenaAllocator.h>
#include <Common/assert_cast.h>

#include <AggregateFunctions/IAggregateFunction.h>

#include <type_traits>

#define AGGREGATE_FUNCTION_GROUP_ARRAY_MAX_ARRAY_SIZE 0xFFFFFF


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int TOO_LARGE_ARRAY_SIZE;
}

enum class Sampler
{
    NONE,
    RNG,
};

template <bool Thas_limit, bool Tlast, Sampler Tsampler>
struct GroupArrayTrait
{
    static constexpr bool has_limit = Thas_limit;
    static constexpr bool last = Tlast;
    static constexpr Sampler sampler = Tsampler;
};

template <typename Trait>
static constexpr const char * getNameByTrait()
{
    if (Trait::last)
        return "groupArrayLast";
    if (Trait::sampler == Sampler::NONE)
        return "groupArray";
    else if (Trait::sampler == Sampler::RNG)
        return "groupArraySample";

    UNREACHABLE();
}

template <typename T>
struct GroupArraySamplerData
{
    /// For easy serialization.
    static_assert(std::has_unique_object_representations_v<T> || std::is_floating_point_v<T>);

    // Switch to ordinary Allocator after 4096 bytes to avoid fragmentation and trash in Arena
    using Allocator = MixedAlignedArenaAllocator<alignof(T), 4096>;
    using Array = PODArray<T, 32, Allocator>;

    Array value;
    size_t total_values = 0;
    pcg32_fast rng;

    UInt64 genRandom(size_t lim)
    {
        /// With a large number of values, we will generate random numbers several times slower.
        if (lim <= static_cast<UInt64>(rng.max()))
            return static_cast<UInt32>(rng()) % static_cast<UInt32>(lim);
        else
            return (static_cast<UInt64>(rng()) * (static_cast<UInt64>(rng.max()) + 1ULL) + static_cast<UInt64>(rng())) % lim;
    }

    void randomShuffle()
    {
        for (size_t i = 1; i < value.size(); ++i)
        {
            size_t j = genRandom(i + 1);
            std::swap(value[i], value[j]);
        }
    }
};

/// A particular case is an implementation for numeric types.
template <typename T, bool has_sampler>
struct GroupArrayNumericData;

template <typename T>
struct GroupArrayNumericData<T, false>
{
    /// For easy serialization.
    static_assert(std::has_unique_object_representations_v<T> || std::is_floating_point_v<T>);

    // Switch to ordinary Allocator after 4096 bytes to avoid fragmentation and trash in Arena
    using Allocator = MixedAlignedArenaAllocator<alignof(T), 4096>;
    using Array = PODArray<T, 32, Allocator>;

    // For groupArrayLast()
    size_t total_values = 0;
    Array value;
};

template <typename T>
struct GroupArrayNumericData<T, true> : public GroupArraySamplerData<T>
{
};

template <typename T, typename Trait>
class GroupArrayNumericImpl final
    : public IAggregateFunctionDataHelper<GroupArrayNumericData<T, Trait::sampler != Sampler::NONE>, GroupArrayNumericImpl<T, Trait>>
{
    using Data = GroupArrayNumericData<T, Trait::sampler != Sampler::NONE>;
    static constexpr bool limit_num_elems = Trait::has_limit;
    UInt64 max_elems;
    UInt64 seed;

public:
    explicit GroupArrayNumericImpl(
        const DataTypePtr & data_type_, const Array & parameters_, UInt64 max_elems_ = std::numeric_limits<UInt64>::max(), UInt64 seed_ = 123456)
        : IAggregateFunctionDataHelper<GroupArrayNumericData<T, Trait::sampler != Sampler::NONE>, GroupArrayNumericImpl<T, Trait>>(
            {data_type_}, parameters_, std::make_shared<DataTypeArray>(data_type_))
        , max_elems(max_elems_)
        , seed(seed_)
    {
    }

    String getName() const override { return getNameByTrait<Trait>(); }

    void insertWithSampler(Data & a, const T & v, Arena * arena) const
    {
        ++a.total_values;
        if (a.value.size() < max_elems)
            a.value.push_back(v, arena);
        else
        {
            UInt64 rnd = a.genRandom(a.total_values);
            if (rnd < max_elems)
                a.value[rnd] = v;
        }
    }

    void create(AggregateDataPtr __restrict place) const override /// NOLINT
    {
        [[maybe_unused]] auto a = new (place) Data;
        if constexpr (Trait::sampler == Sampler::RNG)
            a->rng.seed(seed);
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        const auto & row_value = assert_cast<const ColumnVector<T> &>(*columns[0]).getData()[row_num];
        auto & cur_elems = this->data(place);

        ++cur_elems.total_values;

        if constexpr (Trait::sampler == Sampler::NONE)
        {
            if (limit_num_elems && cur_elems.value.size() >= max_elems)
            {
                if constexpr (Trait::last)
                    cur_elems.value[(cur_elems.total_values - 1) % max_elems] = row_value;
                return;
            }

            cur_elems.value.push_back(row_value, arena);
        }

        if constexpr (Trait::sampler == Sampler::RNG)
        {
            if (cur_elems.value.size() < max_elems)
                cur_elems.value.push_back(row_value, arena);
            else
            {
                UInt64 rnd = cur_elems.genRandom(cur_elems.total_values);
                if (rnd < max_elems)
                    cur_elems.value[rnd] = row_value;
            }
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        auto & cur_elems = this->data(place);
        auto & rhs_elems = this->data(rhs);

        if (rhs_elems.value.empty())
            return;

        if constexpr (Trait::last)
            mergeNoSamplerLast(cur_elems, rhs_elems, arena);
        else if constexpr (Trait::sampler == Sampler::NONE)
            mergeNoSampler(cur_elems, rhs_elems, arena);
        else if constexpr (Trait::sampler == Sampler::RNG)
            mergeWithRNGSampler(cur_elems, rhs_elems, arena);
    }

    void mergeNoSamplerLast(Data & cur_elems, const Data & rhs_elems, Arena * arena) const
    {
        UInt64 new_elements = std::min(static_cast<size_t>(max_elems), cur_elems.value.size() + rhs_elems.value.size());
        cur_elems.value.resize_exact(new_elements, arena);
        for (auto & value : rhs_elems.value)
        {
            cur_elems.value[cur_elems.total_values % max_elems] = value;
            ++cur_elems.total_values;
        }
        assert(rhs_elems.total_values >= rhs_elems.value.size());
        cur_elems.total_values += rhs_elems.total_values - rhs_elems.value.size();
    }

    void mergeNoSampler(Data & cur_elems, const Data & rhs_elems, Arena * arena) const
    {
        if (!limit_num_elems)
        {
            if (rhs_elems.value.size())
                cur_elems.value.insertByOffsets(rhs_elems.value, 0, rhs_elems.value.size(), arena);
        }
        else
        {
            UInt64 elems_to_insert = std::min(static_cast<size_t>(max_elems) - cur_elems.value.size(), rhs_elems.value.size());
            if (elems_to_insert)
                cur_elems.value.insertByOffsets(rhs_elems.value, 0, elems_to_insert, arena);
        }
    }

    void mergeWithRNGSampler(Data & cur_elems, const Data & rhs_elems, Arena * arena) const
    {
        if (rhs_elems.total_values <= max_elems)
        {
            for (size_t i = 0; i < rhs_elems.value.size(); ++i)
                insertWithSampler(cur_elems, rhs_elems.value[i], arena);
        }
        else if (cur_elems.total_values <= max_elems)
        {
            decltype(cur_elems.value) from;
            from.swap(cur_elems.value, arena);
            cur_elems.value.assign(rhs_elems.value.begin(), rhs_elems.value.end(), arena);
            cur_elems.total_values = rhs_elems.total_values;
            for (size_t i = 0; i < from.size(); ++i)
                insertWithSampler(cur_elems, from[i], arena);
        }
        else
        {
            cur_elems.randomShuffle();
            cur_elems.total_values += rhs_elems.total_values;
            for (size_t i = 0; i < max_elems; ++i)
            {
                UInt64 rnd = cur_elems.genRandom(cur_elems.total_values);
                if (rnd < rhs_elems.total_values)
                    cur_elems.value[i] = rhs_elems.value[i];
            }
        }
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        const auto & value = this->data(place).value;
        const size_t size = value.size();
        writeVarUInt(size, buf);
        for (const auto & element : value)
            writeBinaryLittleEndian(element, buf);

        if constexpr (Trait::last)
            writeBinaryLittleEndian(this->data(place).total_values, buf);

        if constexpr (Trait::sampler == Sampler::RNG)
        {
            writeBinaryLittleEndian(this->data(place).total_values, buf);
            WriteBufferFromOwnString rng_buf;
            rng_buf << this->data(place).rng;
            writeStringBinary(rng_buf.str(), buf);
        }
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * arena) const override
    {
        size_t size = 0;
        readVarUInt(size, buf);

        if (unlikely(size > AGGREGATE_FUNCTION_GROUP_ARRAY_MAX_ARRAY_SIZE))
            throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE,
                            "Too large array size (maximum: {})", AGGREGATE_FUNCTION_GROUP_ARRAY_MAX_ARRAY_SIZE);

        if (limit_num_elems && unlikely(size > max_elems))
            throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Too large array size, it should not exceed {}", max_elems);

        auto & value = this->data(place).value;

        value.resize_exact(size, arena);
        for (auto & element : value)
            readBinaryLittleEndian(element, buf);

        if constexpr (Trait::last)
            readBinaryLittleEndian(this->data(place).total_values, buf);

        if constexpr (Trait::sampler == Sampler::RNG)
        {
            readBinaryLittleEndian(this->data(place).total_values, buf);
            std::string rng_string;
            readStringBinary(rng_string, buf);
            ReadBufferFromString rng_buf(rng_string);
            rng_buf >> this->data(place).rng;
        }
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        const auto & value = this->data(place).value;
        size_t size = value.size();

        ColumnArray & arr_to = assert_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();

        offsets_to.push_back(offsets_to.back() + size);

        if (size)
        {
            typename ColumnVector<T>::Container & data_to = assert_cast<ColumnVector<T> &>(arr_to.getData()).getData();
            data_to.insert(this->data(place).value.begin(), this->data(place).value.end());
        }
    }

    bool allocatesMemoryInArena() const override { return true; }
};


/// General case


/// Nodes used to implement a linked list for storage of groupArray states

template <typename Node>
struct GroupArrayNodeBase
{
    UInt64 size; // size of payload

    /// Returns pointer to actual payload
    char * data() { return reinterpret_cast<char *>(this) + sizeof(Node); }

    const char * data() const { return reinterpret_cast<const char *>(this) + sizeof(Node); }

    /// Clones existing node (does not modify next field)
    Node * clone(Arena * arena) const
    {
        return reinterpret_cast<Node *>(
            const_cast<char *>(arena->alignedInsert(reinterpret_cast<const char *>(this), sizeof(Node) + size, alignof(Node))));
    }

    /// Write node to buffer
    void write(WriteBuffer & buf) const
    {
        writeVarUInt(size, buf);
        buf.write(data(), size);
    }

    /// Reads and allocates node from ReadBuffer's data (doesn't set next)
    static Node * read(ReadBuffer & buf, Arena * arena)
    {
        UInt64 size;
        readVarUInt(size, buf);
        if (unlikely(size > AGGREGATE_FUNCTION_GROUP_ARRAY_MAX_ARRAY_SIZE))
            throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE,
                            "Too large array size (maximum: {})", AGGREGATE_FUNCTION_GROUP_ARRAY_MAX_ARRAY_SIZE);

        Node * node = reinterpret_cast<Node *>(arena->alignedAlloc(sizeof(Node) + size, alignof(Node)));
        node->size = size;
        buf.readStrict(node->data(), size);
        return node;
    }
};

struct GroupArrayNodeString : public GroupArrayNodeBase<GroupArrayNodeString>
{
    using Node = GroupArrayNodeString;

    /// Create node from string
    static Node * allocate(const IColumn & column, size_t row_num, Arena * arena)
    {
        StringRef string = assert_cast<const ColumnString &>(column).getDataAt(row_num);

        Node * node = reinterpret_cast<Node *>(arena->alignedAlloc(sizeof(Node) + string.size, alignof(Node)));
        node->size = string.size;
        memcpy(node->data(), string.data, string.size);

        return node;
    }

    void insertInto(IColumn & column)
    {
        assert_cast<ColumnString &>(column).insertData(data(), size);
    }
};

struct GroupArrayNodeGeneral : public GroupArrayNodeBase<GroupArrayNodeGeneral>
{
    using Node = GroupArrayNodeGeneral;

    static Node * allocate(const IColumn & column, size_t row_num, Arena * arena)
    {
        const char * begin = arena->alignedAlloc(sizeof(Node), alignof(Node));
        StringRef value = column.serializeValueIntoArena(row_num, *arena, begin);

        Node * node = reinterpret_cast<Node *>(const_cast<char *>(begin));
        node->size = value.size;

        return node;
    }

    void insertInto(IColumn & column) { column.deserializeAndInsertFromArena(data()); }
};

template <typename Node, bool has_sampler>
struct GroupArrayGeneralData;

template <typename Node>
struct GroupArrayGeneralData<Node, false>
{
    // Switch to ordinary Allocator after 4096 bytes to avoid fragmentation and trash in Arena
    using Allocator = MixedAlignedArenaAllocator<alignof(Node *), 4096>;
    using Array = PODArray<Node *, 32, Allocator>;

    // For groupArrayLast()
    size_t total_values = 0;
    Array value;
};

template <typename Node>
struct GroupArrayGeneralData<Node, true> : public GroupArraySamplerData<Node *>
{
};

/// Implementation of groupArray for String or any ComplexObject via Array
template <typename Node, typename Trait>
class GroupArrayGeneralImpl final
    : public IAggregateFunctionDataHelper<GroupArrayGeneralData<Node, Trait::sampler != Sampler::NONE>, GroupArrayGeneralImpl<Node, Trait>>
{
    static constexpr bool limit_num_elems = Trait::has_limit;
    using Data = GroupArrayGeneralData<Node, Trait::sampler != Sampler::NONE>;
    static Data & data(AggregateDataPtr __restrict place) { return *reinterpret_cast<Data *>(place); }
    static const Data & data(ConstAggregateDataPtr __restrict place) { return *reinterpret_cast<const Data *>(place); }

    DataTypePtr & data_type;
    UInt64 max_elems;
    UInt64 seed;

public:
    GroupArrayGeneralImpl(const DataTypePtr & data_type_, const Array & parameters_, UInt64 max_elems_ = std::numeric_limits<UInt64>::max(), UInt64 seed_ = 123456)
        : IAggregateFunctionDataHelper<GroupArrayGeneralData<Node, Trait::sampler != Sampler::NONE>, GroupArrayGeneralImpl<Node, Trait>>(
            {data_type_}, parameters_, std::make_shared<DataTypeArray>(data_type_))
        , data_type(this->argument_types[0])
        , max_elems(max_elems_)
        , seed(seed_)
    {
    }

    String getName() const override { return getNameByTrait<Trait>(); }

    void insertWithSampler(Data & a, const Node * v, Arena * arena) const
    {
        ++a.total_values;
        if (a.value.size() < max_elems)
            a.value.push_back(v->clone(arena), arena);
        else
        {
            UInt64 rnd = a.genRandom(a.total_values);
            if (rnd < max_elems)
                a.value[rnd] = v->clone(arena);
        }
    }

    void create(AggregateDataPtr __restrict place) const override /// NOLINT
    {
        [[maybe_unused]] auto a = new (place) Data;
        if constexpr (Trait::sampler == Sampler::RNG)
            a->rng.seed(seed);
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        auto & cur_elems = data(place);

        ++cur_elems.total_values;

        if constexpr (Trait::sampler == Sampler::NONE)
        {
            if (limit_num_elems && cur_elems.value.size() >= max_elems)
            {
                if (Trait::last)
                {
                    Node * node = Node::allocate(*columns[0], row_num, arena);
                    cur_elems.value[(cur_elems.total_values - 1) % max_elems] = node;
                }
                return;
            }

            Node * node = Node::allocate(*columns[0], row_num, arena);
            cur_elems.value.push_back(node, arena);
        }

        if constexpr (Trait::sampler == Sampler::RNG)
        {
            if (cur_elems.value.size() < max_elems)
                cur_elems.value.push_back(Node::allocate(*columns[0], row_num, arena), arena);
            else
            {
                UInt64 rnd = cur_elems.genRandom(cur_elems.total_values);
                if (rnd < max_elems)
                    cur_elems.value[rnd] = Node::allocate(*columns[0], row_num, arena);
            }
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        auto & cur_elems = data(place);
        auto & rhs_elems = data(rhs);

        if (rhs_elems.value.empty())
            return;

        if constexpr (Trait::last)
            mergeNoSamplerLast(cur_elems, rhs_elems, arena);
        else if constexpr (Trait::sampler == Sampler::NONE)
            mergeNoSampler(cur_elems, rhs_elems, arena);
        else if constexpr (Trait::sampler == Sampler::RNG)
            mergeWithRNGSampler(cur_elems, rhs_elems, arena);
    }

    void ALWAYS_INLINE mergeNoSamplerLast(Data & cur_elems, const Data & rhs_elems, Arena * arena) const
    {
        UInt64 new_elements = std::min(static_cast<size_t>(max_elems), cur_elems.value.size() + rhs_elems.value.size());
        cur_elems.value.resize_exact(new_elements, arena);
        for (auto & value : rhs_elems.value)
        {
            cur_elems.value[cur_elems.total_values % max_elems] = value->clone(arena);
            ++cur_elems.total_values;
        }
        assert(rhs_elems.total_values >= rhs_elems.value.size());
        cur_elems.total_values += rhs_elems.total_values - rhs_elems.value.size();
    }

    void ALWAYS_INLINE mergeNoSampler(Data & cur_elems, const Data & rhs_elems, Arena * arena) const
    {
        UInt64 new_elems;
        if (limit_num_elems)
        {
            if (cur_elems.value.size() >= max_elems)
                return;
            new_elems = std::min(rhs_elems.value.size(), static_cast<size_t>(max_elems) - cur_elems.value.size());
        }
        else
            new_elems = rhs_elems.value.size();

        for (UInt64 i = 0; i < new_elems; ++i)
            cur_elems.value.push_back(rhs_elems.value[i]->clone(arena), arena);
    }

    void ALWAYS_INLINE mergeWithRNGSampler(Data & cur_elems, const Data & rhs_elems, Arena * arena) const
    {
        if (rhs_elems.total_values <= max_elems)
        {
            for (size_t i = 0; i < rhs_elems.value.size(); ++i)
                insertWithSampler(cur_elems, rhs_elems.value[i], arena);
        }
        else if (cur_elems.total_values <= max_elems)
        {
            decltype(cur_elems.value) from;
            from.swap(cur_elems.value, arena);
            for (auto & node : rhs_elems.value)
                cur_elems.value.push_back(node->clone(arena), arena);
            cur_elems.total_values = rhs_elems.total_values;
            for (size_t i = 0; i < from.size(); ++i)
                insertWithSampler(cur_elems, from[i], arena);
        }
        else
        {
            cur_elems.randomShuffle();
            cur_elems.total_values += rhs_elems.total_values;
            for (size_t i = 0; i < max_elems; ++i)
            {
                UInt64 rnd = cur_elems.genRandom(cur_elems.total_values);
                if (rnd < rhs_elems.total_values)
                    cur_elems.value[i] = rhs_elems.value[i]->clone(arena);
            }
        }
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        writeVarUInt(data(place).value.size(), buf);

        auto & value = data(place).value;
        for (auto & node : value)
            node->write(buf);

        if constexpr (Trait::last)
            writeBinaryLittleEndian(data(place).total_values, buf);

        if constexpr (Trait::sampler == Sampler::RNG)
        {
            writeBinaryLittleEndian(data(place).total_values, buf);
            WriteBufferFromOwnString rng_buf;
            rng_buf << data(place).rng;
            writeStringBinary(rng_buf.str(), buf);
        }
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * arena) const override
    {
        UInt64 elems;
        readVarUInt(elems, buf);

        if (unlikely(elems == 0))
            return;

        if (unlikely(elems > AGGREGATE_FUNCTION_GROUP_ARRAY_MAX_ARRAY_SIZE))
            throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE,
                            "Too large array size (maximum: {})", AGGREGATE_FUNCTION_GROUP_ARRAY_MAX_ARRAY_SIZE);

        if (limit_num_elems && unlikely(elems > max_elems))
            throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Too large array size, it should not exceed {}", max_elems);

        auto & value = data(place).value;

        value.resize_exact(elems, arena);
        for (UInt64 i = 0; i < elems; ++i)
            value[i] = Node::read(buf, arena);

        if constexpr (Trait::last)
            readBinaryLittleEndian(data(place).total_values, buf);

        if constexpr (Trait::sampler == Sampler::RNG)
        {
            readBinaryLittleEndian(data(place).total_values, buf);
            std::string rng_string;
            readStringBinary(rng_string, buf);
            ReadBufferFromString rng_buf(rng_string);
            rng_buf >> data(place).rng;
        }
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto & column_array = assert_cast<ColumnArray &>(to);

        auto & offsets = column_array.getOffsets();
        offsets.push_back(offsets.back() + data(place).value.size());

        auto & column_data = column_array.getData();

        if (std::is_same_v<Node, GroupArrayNodeString>)
        {
            auto & string_offsets = assert_cast<ColumnString &>(column_data).getOffsets();
            string_offsets.reserve(string_offsets.size() + data(place).value.size());
        }

        auto & value = data(place).value;
        for (auto & node : value)
            node->insertInto(column_data);
    }

    bool allocatesMemoryInArena() const override { return true; }
};

#undef AGGREGATE_FUNCTION_GROUP_ARRAY_MAX_ARRAY_SIZE

}
