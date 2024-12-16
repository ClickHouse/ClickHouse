#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <Interpreters/Context.h>
#include <Core/ServerSettings.h>

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
#include <Common/thread_local_rng.h>

#include <AggregateFunctions/IAggregateFunction.h>

#include <type_traits>

#define AGGREGATE_FUNCTION_GROUP_ARRAY_MAX_ELEMENT_SIZE 0xFFFFFF


namespace DB
{
struct Settings;

namespace ServerSetting
{
    extern const ServerSettingsGroupArrayActionWhenLimitReached aggregate_function_group_array_action_when_limit_is_reached;
    extern const ServerSettingsUInt64 aggregate_function_group_array_max_element_size;
}

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
    extern const int TOO_LARGE_ARRAY_SIZE;
}

namespace
{

enum class Sampler : uint8_t
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
constexpr const char * getNameByTrait()
{
    if constexpr (Trait::last)
        return "groupArrayLast";
    switch (Trait::sampler)
    {
        case Sampler::NONE: return "groupArray";
        case Sampler::RNG: return "groupArraySample";
    }
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
        chassert(lim != 0);

        /// With a large number of values, we will generate random numbers several times slower.
        if (lim <= static_cast<UInt64>(pcg32_fast::max()))
            return rng() % lim;
        return (static_cast<UInt64>(rng()) * (static_cast<UInt64>(pcg32::max()) + 1ULL) + static_cast<UInt64>(rng())) % lim;
    }

    void randomShuffle()
    {
        size_t size = value.size();
        chassert(size < std::numeric_limits<size_t>::max());

        for (size_t i = 1; i < size; ++i)
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
    std::optional<UInt64> seed;

public:
    explicit GroupArrayNumericImpl(
        const DataTypePtr & data_type_, const Array & parameters_, UInt64 max_elems_, std::optional<UInt64> seed_)
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
            a->rng.seed(seed.value_or(thread_local_rng()));
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        const auto & row_value = assert_cast<const ColumnVector<T> &>(*columns[0]).getData()[row_num];
        auto & cur_elems = this->data(place);

        ++cur_elems.total_values;

        if constexpr (Trait::sampler == Sampler::NONE)
        {
            if constexpr (limit_num_elems)
            {
                if (cur_elems.value.size() >= max_elems)
                {
                    if constexpr (Trait::last)
                        cur_elems.value[(cur_elems.total_values - 1) % max_elems] = row_value;
                    return;
                }
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
        chassert(rhs_elems.total_values >= rhs_elems.value.size());
        cur_elems.total_values += rhs_elems.total_values - rhs_elems.value.size();
    }

    void mergeNoSampler(Data & cur_elems, const Data & rhs_elems, Arena * arena) const
    {
        if constexpr (!limit_num_elems)
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

    static void checkArraySize(size_t elems, size_t max_elems)
    {
        if (unlikely(elems > max_elems))
            throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE,
                            "Too large array size {} (maximum: {})", elems, max_elems);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        const auto & value = this->data(place).value;
        const UInt64 size = value.size();
        checkArraySize(size, max_elems);
        writeVarUInt(size, buf);


        if constexpr (std::endian::native == std::endian::little)
        {
            buf.write(reinterpret_cast<const char *>(value.data()), size * sizeof(value[0]));
        }
        else
        {
            for (const auto & element : value)
                writeBinaryLittleEndian(element, buf);
        }

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
        checkArraySize(size, max_elems);

        auto & value = this->data(place).value;

        value.resize_exact(size, arena);

        if constexpr (std::endian::native == std::endian::little)
        {
            buf.readStrict(reinterpret_cast<char *>(value.data()), size * sizeof(value[0]));
        }
        else
        {
            for (auto & element : value)
                readBinaryLittleEndian(element, buf);
        }

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

    static void checkElementSize(size_t size, size_t max_size)
    {
        if (unlikely(size > max_size))
            throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE,
                            "Too large array element size {} (maximum: {})", size, max_size);
    }

    /// Write node to buffer
    void write(WriteBuffer & buf) const
    {
        checkElementSize(size, AGGREGATE_FUNCTION_GROUP_ARRAY_MAX_ELEMENT_SIZE);
        writeVarUInt(size, buf);
        buf.write(data(), size);
    }

    /// Reads and allocates node from ReadBuffer's data (doesn't set next)
    static Node * read(ReadBuffer & buf, Arena * arena)
    {
        UInt64 size;
        readVarUInt(size, buf);
        checkElementSize(size, AGGREGATE_FUNCTION_GROUP_ARRAY_MAX_ELEMENT_SIZE);

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

    void insertInto(IColumn & column) { std::ignore = column.deserializeAndInsertFromArena(data()); }
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
    std::optional<UInt64> seed;

public:
    GroupArrayGeneralImpl(const DataTypePtr & data_type_, const Array & parameters_, UInt64 max_elems_, std::optional<UInt64> seed_)
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
            a->rng.seed(seed.value_or(thread_local_rng()));
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
        chassert(rhs_elems.total_values >= rhs_elems.value.size());
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

    static void checkArraySize(size_t elems, size_t max_elems)
    {
        if (unlikely(elems > max_elems))
            throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE,
                            "Too large array size {} (maximum: {})", elems, max_elems);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        UInt64 elems = data(place).value.size();
        checkArraySize(elems, max_elems);
        writeVarUInt(elems, buf);

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

        checkArraySize(elems, max_elems);

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


template <template <typename, typename> class AggregateFunctionTemplate, typename Data, typename ... TArgs>
IAggregateFunction * createWithNumericOrTimeType(const IDataType & argument_type, TArgs && ... args)
{
    WhichDataType which(argument_type);
    if (which.idx == TypeIndex::Date) return new AggregateFunctionTemplate<UInt16, Data>(std::forward<TArgs>(args)...);
    if (which.idx == TypeIndex::DateTime) return new AggregateFunctionTemplate<UInt32, Data>(std::forward<TArgs>(args)...);
    if (which.idx == TypeIndex::IPv4) return new AggregateFunctionTemplate<IPv4, Data>(std::forward<TArgs>(args)...);
    return createWithNumericType<AggregateFunctionTemplate, Data, TArgs...>(argument_type, std::forward<TArgs>(args)...);
}


template <typename Trait, typename ... TArgs>
inline AggregateFunctionPtr createAggregateFunctionGroupArrayImpl(const DataTypePtr & argument_type, const Array & parameters, TArgs ... args)
{
    if (auto res = createWithNumericOrTimeType<GroupArrayNumericImpl, Trait>(*argument_type, argument_type, parameters, args...))
        return AggregateFunctionPtr(res);

    WhichDataType which(argument_type);
    if (which.idx == TypeIndex::String)
        return std::make_shared<GroupArrayGeneralImpl<GroupArrayNodeString, Trait>>(argument_type, parameters, args...);

    return std::make_shared<GroupArrayGeneralImpl<GroupArrayNodeGeneral, Trait>>(argument_type, parameters, args...);
}

size_t getMaxArraySize()
{
    if (auto context = Context::getGlobalContextInstance())
        return context->getServerSettings()[ServerSetting::aggregate_function_group_array_max_element_size];

    return 0xFFFFFF;
}

bool discardOnLimitReached()
{
    if (auto context = Context::getGlobalContextInstance())
        return context->getServerSettings()[ServerSetting::aggregate_function_group_array_action_when_limit_is_reached]
            == GroupArrayActionWhenLimitReached::DISCARD;

    return false;
}

template <bool Tlast>
AggregateFunctionPtr createAggregateFunctionGroupArray(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertUnary(name, argument_types);

    bool has_limit = discardOnLimitReached();
    UInt64 max_elems = getMaxArraySize();

    if (parameters.empty())
    {
        // no limit
    }
    else if (parameters.size() == 1)
    {
        auto type = parameters[0].getType();
        if (type != Field::Types::Int64 && type != Field::Types::UInt64)
               throw Exception(ErrorCodes::BAD_ARGUMENTS, "Parameter for aggregate function {} should be positive number", name);

        if ((type == Field::Types::Int64 && parameters[0].safeGet<Int64>() < 0) ||
            (type == Field::Types::UInt64 && parameters[0].safeGet<UInt64>() == 0))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Parameter for aggregate function {} should be positive number", name);

        has_limit = true;
        max_elems = parameters[0].safeGet<UInt64>();
    }
    else
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Incorrect number of parameters for aggregate function {}, should be 0 or 1", name);

    if (!has_limit)
    {
        if (Tlast)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "groupArrayLast make sense only with max_elems (groupArrayLast(max_elems)())");
        return createAggregateFunctionGroupArrayImpl<GroupArrayTrait</* Thas_limit= */ false, Tlast, /* Tsampler= */ Sampler::NONE>>(argument_types[0], parameters, max_elems, std::nullopt);
    }
    return createAggregateFunctionGroupArrayImpl<GroupArrayTrait</* Thas_limit= */ true, Tlast, /* Tsampler= */ Sampler::NONE>>(
        argument_types[0], parameters, max_elems, std::nullopt);
}

AggregateFunctionPtr createAggregateFunctionGroupArraySample(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertUnary(name, argument_types);

    if (parameters.size() != 1 && parameters.size() != 2)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Incorrect number of parameters for aggregate function {}, should be 1 or 2", name);

    auto get_parameter = [&](size_t i)
    {
        auto type = parameters[i].getType();
        if (type != Field::Types::Int64 && type != Field::Types::UInt64)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Parameter for aggregate function {} should be positive number", name);

        if ((type == Field::Types::Int64 && parameters[i].safeGet<Int64>() < 0) ||
                (type == Field::Types::UInt64 && parameters[i].safeGet<UInt64>() == 0))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Parameter for aggregate function {} should be positive number", name);

        return parameters[i].safeGet<UInt64>();
    };

    UInt64 max_elems = get_parameter(0);

    std::optional<UInt64> seed;
    if (parameters.size() >= 2)
        seed = get_parameter(1);

    return createAggregateFunctionGroupArrayImpl<GroupArrayTrait</* Thas_limit= */ true, /* Tlast= */ false, /* Tsampler= */ Sampler::RNG>>(argument_types[0], parameters, max_elems, seed);
}

}


void registerAggregateFunctionGroupArray(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = { .returns_default_when_only_null = false, .is_order_dependent = true };

    factory.registerFunction("groupArray", { createAggregateFunctionGroupArray<false>, properties });
    factory.registerAlias("array_agg", "groupArray", AggregateFunctionFactory::Case::Insensitive);
    factory.registerAliasUnchecked("array_concat_agg", "groupArrayArray", AggregateFunctionFactory::Case::Insensitive);
    factory.registerFunction("groupArraySample", { createAggregateFunctionGroupArraySample, properties });
    factory.registerFunction("groupArrayLast", { createAggregateFunctionGroupArray<true>, properties });
}

}
