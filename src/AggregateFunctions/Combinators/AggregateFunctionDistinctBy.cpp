#include <AggregateFunctions/Combinators/AggregateFunctionCombinatorFactory.h>
#include <AggregateFunctions/Helpers.h>
#include <Core/Types.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/KeyHolderHelpers.h>
#include <Common/HashTable/HashMap.h>
#include <Common/UnorderedMapWithMemoryTracking.h>
#include <base/types.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadHelpersArena.h>
#include <IO/ReadBufferFromMemory.h>

#include <DataTypes/Serializations/SerializationWrapper.h>
#include <IO/WriteHelpers.h>

namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

    template <typename Key>
    struct AggregateFunctionDistinctByNumericKeyData
    {
        /// When creating, the hash table must be small.
        using Map = HashMapWithStackMemory<Key, UInt64, DefaultHash<Key>, 4>;
        using Self = AggregateFunctionDistinctByNumericKeyData;
        Map map;
        size_t value_size = 0;

        void add(const IColumn ** columns, size_t arg_num, size_t row_num, Arena *)
        {
            const auto & key_vec = assert_cast<const ColumnVector<Key> &>(*columns[arg_num]).getData();
            auto ref = columns[arg_num - 1]->getDataAt(row_num);
            UInt64 raw = 0;
            std::memcpy(&raw, ref.data(), ref.size());
            map[key_vec[row_num]] = raw;
        }

        void merge(const Self & rhs, Arena *)
        {
            auto & mutable_rhs_map = const_cast<Map &>(rhs.map);
            /// `inserted` is true when the key was newly inserted into the destination map
            /// and the destination cell needs initialization. For DistinctBy semantics we keep
            /// the first value seen, so on existing keys we leave the destination untouched.
            mutable_rhs_map.mergeToViaEmplace(map, [&](UInt64 & dst, UInt64 & src, bool inserted)
            {
                if (inserted)
                    dst = src;
            });
        }

        void serialize(WriteBuffer & buf) const
        {
            writeVarUInt(map.size(), buf);
            for (const auto & [key, raw_value] : map)
            {
                writeBinaryLittleEndian(key, buf);
                buf.write(reinterpret_cast<const char *>(&raw_value), value_size);
            }
        }

        void deserialize(ReadBuffer & buf, Arena *)
        {
            size_t size;
            readVarUInt(size, buf);
            for (size_t i = 0; i < size; ++i)
            {
                Key key;
                readBinaryLittleEndian(key, buf);
                UInt64 raw = 0;
                buf.readStrict(reinterpret_cast<char *>(&raw), value_size);
                map[key] = raw;
            }
        }

        MutableColumns getArguments(const DataTypes & argument_types, size_t /* arg_num */) const
        {
            MutableColumns argument_columns;
            argument_columns.emplace_back(argument_types[0]->createColumn());
            const size_t actual_value_size = argument_types[0]->getSizeOfValueInMemory();
            for (const auto & [_, raw_value] : map)
            {
                char buffer[sizeof(UInt64)] = {};
                std::memcpy(buffer, &raw_value, actual_value_size);
                argument_columns[0]->insertData(buffer, actual_value_size);
            }
            return argument_columns;
        }
    };

    template <typename T>
    struct AggregateFunctionDistinctByNumericData
    {
        /// When creating, the hash table must be small.
        using Map = HashMapWithStackMemory<T, T, DefaultHash<T>, 4>;
        using Self = AggregateFunctionDistinctByNumericData;
        Map map;

        void add(const IColumn ** columns, size_t arg_num, size_t row_num, Arena *)
        {
            const auto & key_vec = assert_cast<const ColumnVector<T> &>(*columns[arg_num]).getData();
            const auto & val_vec = assert_cast<const ColumnVector<T> &>(*columns[0]).getData();
            map[key_vec[row_num]] = val_vec[row_num];
        }

        void merge(const Self & rhs, Arena *)
        {
            auto & mutable_rhs_map = const_cast<Map &>(rhs.map);
            mutable_rhs_map.mergeToViaEmplace(map, [&](T & dst, T & src, bool inserted)
            {
                if (inserted)
                    dst = src;
            });
        }

        void serialize(WriteBuffer & buf) const
        {
            writeVarUInt(map.size(), buf);
            for (const auto & [key, value] : map)
            {
                writeBinaryLittleEndian(key, buf);
                writeBinaryLittleEndian(value, buf);
            }
        }

        void deserialize(ReadBuffer & buf, Arena *)
        {
            size_t size;
            readVarUInt(size, buf);
            for (size_t i = 0; i < size; ++i)
            {
                T key;
                readBinaryLittleEndian(key, buf);
                T value;
                readBinaryLittleEndian(value, buf);
                map[key] = value;
            }
        }

        MutableColumns getArguments(const DataTypes & argument_types, size_t /* arg_num */) const
        {
            MutableColumns argument_columns;
            argument_columns.emplace_back(argument_types[0]->createColumn());
            for (const auto & [_, value] : map)
                argument_columns[0]->insert(value);

            return argument_columns;
        }
    };

struct AggregateFunctionDistinctByGenericData
{
    using Map = HashMapWithSavedHash<std::string_view, std::string_view, StringViewHash>;
    using Self = AggregateFunctionDistinctByGenericData;
    Map map;

    void add(const IColumn ** columns, size_t arg_num, size_t row_num, Arena * arena)
    {
        const IColumn * key_column = columns[arg_num];
        const char * begin = nullptr;
        std::string_view key = key_column->serializeValueIntoArena(row_num, *arena, begin, nullptr);

        typename Map::LookupResult it;
        bool inserted;
        map.emplace(SerializedKeyHolder{key, *arena}, it, inserted);
        if (inserted)
        {
            const char * val_begin = nullptr;
            size_t total_size = 0;
            for (size_t i = 0; i < arg_num; ++i)
            {
                auto cur = columns[i]->serializeValueIntoArena(row_num, *arena, val_begin, nullptr);
                total_size += cur.size();
            }
            it->getMapped() = std::string_view(val_begin, total_size);
            //nested_func->add(nested_place, columns, row_num, arena);
        }
    }

    void merge(const Self & rhs, Arena * arena)
    {
        auto & mutable_rhs_map = const_cast<Map &>(rhs.map);
        mutable_rhs_map.mergeToViaEmplace(map, [&](std::string_view & dst, std::string_view & src, bool inserted)
        {
            if (inserted)
                dst = std::string_view(arena->insert(src.data(), src.size()), src.size());
        });
    }

    void serialize(WriteBuffer & buf) const
    {
        writeVarUInt(map.size(), buf);
        for (const auto & elem : map)
        {
            writeStringBinary(elem.getKey(), buf);
            writeStringBinary(elem.getMapped(), buf);
        }
    }

    void deserialize(ReadBuffer & buf, Arena * arena)
    {
        size_t size;
        readVarUInt(size, buf);
        for (size_t i = 0; i < size; ++i)
        {
            std::string_view key = readStringBinaryInto(*arena, buf);
            std::string_view value = readStringBinaryInto(*arena, buf);
            typename Map::LookupResult it;
            bool inserted;
            map.emplace(ArenaKeyHolder{key, *arena}, it, inserted);
            it->getMapped() = value;
        }
    }

    MutableColumns getArguments(const DataTypes & argument_types, size_t arg_num) const
    {
        MutableColumns argument_columns(arg_num);
        for (size_t i = 0; i < arg_num; ++i)
            argument_columns[i] = argument_types[i]->createColumn();

        for (const auto & elem : map)
        {
            auto mapped = elem.getMapped();
            ReadBufferFromMemory buf(mapped.data(), mapped.size());
            for (size_t i = 0; i < arg_num; ++i)
                argument_columns[i]->deserializeAndInsertFromArena(buf, nullptr);
        }
        return argument_columns;
    }
};

template <typename Data>
class AggregateFunctionDistinctBy final : public IAggregateFunctionDataHelper<Data, AggregateFunctionDistinctBy<Data>>
{
private:
    AggregateFunctionPtr nested_func;
    size_t arg_num;

public:
    AggregateFunctionDistinctBy(const AggregateFunctionPtr & nested, const DataTypes & arguments, const Array & params)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionDistinctBy<Data>>(arguments, params, nested->getResultType())
        , nested_func(nested)
    {
        arg_num = arguments.size() - 1;
    }

    String getName() const override
    {
        return nested_func->getName() + "DistinctBy";
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

    size_t sizeOfData() const override
    {
        return sizeof(Data);
    }

    size_t alignOfData() const override
    {
        return alignof(Data);
    }

    void create(AggregateDataPtr __restrict place) const override
    {
        new (place) Data;
        if constexpr (requires (Data & d) { d.value_size; })
            this->data(place).value_size = this->argument_types[0]->getSizeOfValueInMemory();
    }

    void destroy(AggregateDataPtr place) const noexcept override
    {
        this->data(place).~Data();
    }

    void destroyUpToState(AggregateDataPtr place) const noexcept override
    {
        this->data(place).~Data();
    }

    bool hasTrivialDestructor() const override
    {
        return std::is_trivially_destructible_v<Data> && nested_func->hasTrivialDestructor();
    }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        this->data(place).add(columns, arg_num, row_num, arena);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        this->data(place).merge(this->data(rhs), arena);
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf, std::optional<size_t> /*version*/) const override
    {
        this->data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, std::optional<size_t> /*version*/, Arena * arena) const override
    {
        this->data(place).deserialize(buf, arena);
    }

    void insertResultInto(AggregateDataPtr place, IColumn & to, Arena * arena) const override
    {
        auto arguments = this->data(place).getArguments(this->argument_types, arg_num);
        ColumnRawPtrs arguments_raw(arguments.size());
        for (size_t i = 0; i < arguments.size(); ++i)
            arguments_raw[i] = arguments[i].get();

        AggregateDataPtr nested_place = arena->alignedAlloc(nested_func->sizeOfData(), nested_func->alignOfData());
        nested_func->create(nested_place);
        if (!arguments.empty())
            nested_func->addBatchSinglePlace(0, arguments[0]->size(), nested_place, arguments_raw.data(), arena);
        nested_func->insertResultInto(nested_place, to, arena);
        nested_func->destroy(nested_place);
    }

    void insertMergeResultInto(AggregateDataPtr place, IColumn & to, Arena * arena) const override
    {
        auto arguments = this->data(place).getArguments(this->argument_types, arg_num);
        ColumnRawPtrs arguments_raw(arguments.size());
        for (size_t i = 0; i < arguments.size(); ++i)
            arguments_raw[i] = arguments[i].get();

        AggregateDataPtr nested_place = arena->alignedAlloc(nested_func->sizeOfData(), nested_func->alignOfData());
        nested_func->create(nested_place);
        if (!arguments.empty())
            nested_func->addBatchSinglePlace(0, arguments[0]->size(), nested_place, arguments_raw.data(), arena);
        nested_func->insertMergeResultInto(nested_place, to, arena);
        nested_func->destroy(nested_place);
    }

    AggregateFunctionPtr getNestedFunction() const override { return nested_func; }
};

class AggregateFunctionCombinatorDistinctBy final : public IAggregateFunctionCombinator
{
public:
    String getName() const override { return "DistinctBy"; }

    DataTypes transformArguments(const DataTypes & arguments) const override
    {
        if (arguments.size() < 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Incorrect number of arguments for aggregate function with {} suffix", getName());
        return DataTypes(arguments.begin(), arguments.end() - 1);
    }

    AggregateFunctionPtr transformAggregateFunction(
        const AggregateFunctionPtr & nested_function,
        const AggregateFunctionProperties & /*properties*/,
        const DataTypes & arguments,
        const Array & params) const override
    {
        AggregateFunctionPtr res;
        if (arguments.size() == 2)
        {
            if (arguments[0]->getTypeId() == arguments[1]->getTypeId())
                res.reset(createWithNumericType<
                    AggregateFunctionDistinctBy,
                    AggregateFunctionDistinctByNumericData>(*arguments[1], nested_function, arguments, params));
            else if (arguments[0]->isValueUnambiguouslyRepresentedInFixedSizeContiguousMemoryRegion()
                && arguments[0]->getSizeOfValueInMemory() <= sizeof(UInt64))
                res.reset(createWithNumericType<
                    AggregateFunctionDistinctBy,
                    AggregateFunctionDistinctByNumericKeyData>(*arguments[1], nested_function, arguments, params));

            if (res)
                return res;
        }
        return std::make_shared<AggregateFunctionDistinctBy<AggregateFunctionDistinctByGenericData>>(nested_function, arguments, params);
    }
};

}

void registerAggregateFunctionCombinatorDistinctBy(AggregateFunctionCombinatorFactory & factory)
{
    factory.registerCombinator(std::make_shared<AggregateFunctionCombinatorDistinctBy>());
}

}
