#include <DataTypes/Serializations/SerializationLowCardinality.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>

#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnUnique.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnsCommon.h>
#include <Common/HashTable/HashMap.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>
#include <Core/Field.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{
    const ColumnLowCardinality & getColumnLowCardinality(const IColumn & column)
    {
        return typeid_cast<const ColumnLowCardinality &>(column);
    }

    ColumnLowCardinality & getColumnLowCardinality(IColumn & column)
    {
        return typeid_cast<ColumnLowCardinality &>(column);
    }
}

SerializationLowCardinality::SerializationLowCardinality(const DataTypePtr & dictionary_type_)
     : dictionary_type(dictionary_type_)
     , dict_inner_serialization(removeNullable(dictionary_type_)->getDefaultSerialization())
{
}

void SerializationLowCardinality::enumerateStreams(const StreamCallback & callback, SubstreamPath & path) const
{
    path.push_back(Substream::DictionaryKeys);
    dict_inner_serialization->enumerateStreams(callback, path);
    path.back() = Substream::DictionaryIndexes;
    callback(path);
    path.pop_back();
}

struct KeysSerializationVersion
{
    enum Value
    {
        /// Version is written at the start of <name.dict.bin>.
        /// Dictionary is written as number N and N keys after them.
        /// Dictionary can be shared for continuous range of granules, so some marks may point to the same position.
        /// Shared dictionary is stored in state and is read once.
        SharedDictionariesWithAdditionalKeys = 1,
    };

    Value value;

    static void checkVersion(UInt64 version)
    {
        if (version != SharedDictionariesWithAdditionalKeys)
            throw Exception("Invalid version for SerializationLowCardinality key column.", ErrorCodes::LOGICAL_ERROR);
    }

    explicit KeysSerializationVersion(UInt64 version) : value(static_cast<Value>(version)) { checkVersion(version); }
};

/// Version is stored at the start of each granule. It's used to store indexes type and flags.
struct IndexesSerializationType
{
    using SerializationType = UInt64;
    /// Need to read dictionary if it wasn't.
    static constexpr SerializationType NeedGlobalDictionaryBit = 1u << 8u;
    /// Need to read additional keys. Additional keys are stored before indexes as value N and N keys after them.
    static constexpr SerializationType HasAdditionalKeysBit = 1u << 9u;
    /// Need to update dictionary. It means that previous granule has different dictionary.
    static constexpr SerializationType NeedUpdateDictionary = 1u << 10u;

    enum Type
    {
        TUInt8 = 0,
        TUInt16,
        TUInt32,
        TUInt64,
    };

    Type type;
    bool has_additional_keys;
    bool need_global_dictionary;
    bool need_update_dictionary;

    static constexpr SerializationType resetFlags(SerializationType type)
    {
        return type & (~(HasAdditionalKeysBit | NeedGlobalDictionaryBit | NeedUpdateDictionary));
    }

    static void checkType(SerializationType type)
    {
        UInt64 value = resetFlags(type);
        if (value <= TUInt64)
            return;

        throw Exception("Invalid type for SerializationLowCardinality index column.", ErrorCodes::LOGICAL_ERROR);
    }

    void serialize(WriteBuffer & buffer) const
    {
        SerializationType val = type;
        if (has_additional_keys)
            val |= HasAdditionalKeysBit;
        if (need_global_dictionary)
            val |= NeedGlobalDictionaryBit;
        if (need_update_dictionary)
            val |= NeedUpdateDictionary;
        writeIntBinary(val, buffer);
    }

    void deserialize(ReadBuffer & buffer)
    {
        SerializationType val;
        readIntBinary(val, buffer);
        checkType(val);
        has_additional_keys = (val & HasAdditionalKeysBit) != 0;
        need_global_dictionary = (val & NeedGlobalDictionaryBit) != 0;
        need_update_dictionary = (val & NeedUpdateDictionary) != 0;
        type = static_cast<Type>(resetFlags(val));
    }

    IndexesSerializationType(const IColumn & column,
                             bool has_additional_keys_,
                             bool need_global_dictionary_,
                             bool enumerate_dictionaries)
        : has_additional_keys(has_additional_keys_)
        , need_global_dictionary(need_global_dictionary_)
        , need_update_dictionary(enumerate_dictionaries)
    {
        if (typeid_cast<const ColumnUInt8 *>(&column))
            type = TUInt8;
        else if (typeid_cast<const ColumnUInt16 *>(&column))
            type = TUInt16;
        else if (typeid_cast<const ColumnUInt32 *>(&column))
            type = TUInt32;
        else if (typeid_cast<const ColumnUInt64 *>(&column))
            type = TUInt64;
        else
            throw Exception("Invalid Indexes column for IndexesSerializationType. Expected ColumnUInt*, got "
                            + column.getName(), ErrorCodes::LOGICAL_ERROR);
    }

    DataTypePtr getDataType() const
    {
        if (type == TUInt8)
            return std::make_shared<DataTypeUInt8>();
        if (type == TUInt16)
            return std::make_shared<DataTypeUInt16>();
        if (type == TUInt32)
            return std::make_shared<DataTypeUInt32>();
        if (type == TUInt64)
            return std::make_shared<DataTypeUInt64>();

        throw Exception("Can't create DataType from IndexesSerializationType.", ErrorCodes::LOGICAL_ERROR);
    }

    IndexesSerializationType() = default;
};

struct SerializeStateLowCardinality : public ISerialization::SerializeBinaryBulkState
{
    KeysSerializationVersion key_version;
    MutableColumnUniquePtr shared_dictionary;

    explicit SerializeStateLowCardinality(UInt64 key_version_) : key_version(key_version_) {}
};

struct DeserializeStateLowCardinality : public ISerialization::DeserializeBinaryBulkState
{
    KeysSerializationVersion key_version;
    ColumnUniquePtr global_dictionary;

    IndexesSerializationType index_type;
    ColumnPtr additional_keys;
    ColumnPtr null_map;
    UInt64 num_pending_rows = 0;

    /// If dictionary should be updated.
    /// Can happen is some granules was skipped while reading from MergeTree.
    /// We should store this flag in State because
    ///   in case of long block of empty arrays we may not need read dictionary at first reading.
    bool need_update_dictionary = false;

    explicit DeserializeStateLowCardinality(UInt64 key_version_) : key_version(key_version_) {}
};

static SerializeStateLowCardinality * checkAndGetLowCardinalitySerializeState(
    ISerialization::SerializeBinaryBulkStatePtr & state)
{
    if (!state)
        throw Exception("Got empty state for SerializationLowCardinality.", ErrorCodes::LOGICAL_ERROR);

    auto * low_cardinality_state = typeid_cast<SerializeStateLowCardinality *>(state.get());
    if (!low_cardinality_state)
    {
        auto & state_ref = *state;
        throw Exception("Invalid SerializeBinaryBulkState for SerializationLowCardinality. Expected: "
                        + demangle(typeid(SerializeStateLowCardinality).name()) + ", got "
                        + demangle(typeid(state_ref).name()), ErrorCodes::LOGICAL_ERROR);
    }

    return low_cardinality_state;
}

static DeserializeStateLowCardinality * checkAndGetLowCardinalityDeserializeState(
    ISerialization::DeserializeBinaryBulkStatePtr & state)
{
    if (!state)
        throw Exception("Got empty state for SerializationLowCardinality.", ErrorCodes::LOGICAL_ERROR);

    auto * low_cardinality_state = typeid_cast<DeserializeStateLowCardinality *>(state.get());
    if (!low_cardinality_state)
    {
        auto & state_ref = *state;
        throw Exception("Invalid DeserializeBinaryBulkState for SerializationLowCardinality. Expected: "
                        + demangle(typeid(DeserializeStateLowCardinality).name()) + ", got "
                        + demangle(typeid(state_ref).name()), ErrorCodes::LOGICAL_ERROR);
    }

    return low_cardinality_state;
}

void SerializationLowCardinality::serializeBinaryBulkStatePrefix(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    settings.path.push_back(Substream::DictionaryKeys);
    auto * stream = settings.getter(settings.path);
    settings.path.pop_back();

    if (!stream)
        throw Exception("Got empty stream in SerializationLowCardinality::serializeBinaryBulkStatePrefix",
                        ErrorCodes::LOGICAL_ERROR);

    /// Write version and create SerializeBinaryBulkState.
    UInt64 key_version = KeysSerializationVersion::SharedDictionariesWithAdditionalKeys;

    writeIntBinary(key_version, *stream);

    state = std::make_shared<SerializeStateLowCardinality>(key_version);
}

void SerializationLowCardinality::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    auto * low_cardinality_state = checkAndGetLowCardinalitySerializeState(state);
    KeysSerializationVersion::checkVersion(low_cardinality_state->key_version.value);

    if (low_cardinality_state->shared_dictionary && settings.low_cardinality_max_dictionary_size)
    {
        auto nested_column = low_cardinality_state->shared_dictionary->getNestedNotNullableColumn();

        settings.path.push_back(Substream::DictionaryKeys);
        auto * stream = settings.getter(settings.path);
        settings.path.pop_back();

        if (!stream)
            throw Exception("Got empty stream in SerializationLowCardinality::serializeBinaryBulkStateSuffix",
                            ErrorCodes::LOGICAL_ERROR);

        UInt64 num_keys = nested_column->size();
        writeIntBinary(num_keys, *stream);
        dict_inner_serialization->serializeBinaryBulk(*nested_column, *stream, 0, num_keys);
        low_cardinality_state->shared_dictionary = nullptr;
    }
}

void SerializationLowCardinality::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state) const
{
    settings.path.push_back(Substream::DictionaryKeys);
    auto * stream = settings.getter(settings.path);
    settings.path.pop_back();

    if (!stream)
        return;

    UInt64 keys_version;
    readIntBinary(keys_version, *stream);

    state = std::make_shared<DeserializeStateLowCardinality>(keys_version);
}

namespace
{
    template <typename T>
    PaddedPODArray<T> * getIndexesData(IColumn & indexes)
    {
        auto * column = typeid_cast<ColumnVector<T> *>(&indexes);
        if (column)
            return &column->getData();

        return nullptr;
    }

    struct IndexMapsWithAdditionalKeys
    {
        MutableColumnPtr dictionary_map;
        MutableColumnPtr additional_keys_map;
    };

    template <typename T>
    IndexMapsWithAdditionalKeys mapIndexWithAdditionalKeysRef(PaddedPODArray<T> & index, size_t dict_size)
    {
        PaddedPODArray<T> copy(index.cbegin(), index.cend());

        HashMap<T, T> dict_map;
        HashMap<T, T> add_keys_map;

        for (auto val : index)
        {
            if (val < dict_size)
                dict_map.insert({val, dict_map.size()});
            else
                add_keys_map.insert({val, add_keys_map.size()});
        }

        auto dictionary_map = ColumnVector<T>::create(dict_map.size());
        auto additional_keys_map = ColumnVector<T>::create(add_keys_map.size());
        auto & dict_data = dictionary_map->getData();
        auto & add_keys_data = additional_keys_map->getData();

        for (auto val : dict_map)
            dict_data[val.second] = val.first;

        for (auto val : add_keys_map)
            add_keys_data[val.second] = val.first - dict_size;

        for (auto & val : index)
            val = val < dict_size ? dict_map[val]
                                  : add_keys_map[val] + dict_map.size();

        for (size_t i = 0; i < index.size(); ++i)
        {
            T expected = index[i] < dict_data.size() ? dict_data[index[i]]
                                                     : add_keys_data[index[i] - dict_data.size()] + dict_size;
            if (expected != copy[i])
                throw Exception("Expected " + toString(expected) + ", but got " + toString(copy[i]), ErrorCodes::LOGICAL_ERROR);

        }

        return {std::move(dictionary_map), std::move(additional_keys_map)};
    }

    template <typename T>
    IndexMapsWithAdditionalKeys mapIndexWithAdditionalKeys(PaddedPODArray<T> & index, size_t dict_size)
    {
        T max_less_dict_size = 0;
        T max_value = 0;

        auto size = index.size();
        if (size == 0)
            return {ColumnVector<T>::create(), ColumnVector<T>::create()};

        for (size_t i = 0; i < size; ++i)
        {
            auto val = index[i];
            if (val < dict_size)
                max_less_dict_size = std::max(max_less_dict_size, val);

            max_value = std::max(max_value, val);
        }

        auto map_size = UInt64(max_less_dict_size) + 1;
        auto overflow_map_size = max_value >= dict_size ? (UInt64(max_value - dict_size) + 1) : 0;
        PaddedPODArray<T> map(map_size, 0);
        PaddedPODArray<T> overflow_map(overflow_map_size, 0);

        T zero_pos_value = 0;
        T zero_pos_overflowed_value = 0;
        UInt64 cur_pos = 0;
        UInt64 cur_overflowed_pos = 0;

        for (size_t i = 0; i < size; ++i)
        {
            T val = index[i];
            if (val < dict_size)
            {
                if (cur_pos == 0)
                {
                    zero_pos_value = val;
                    ++cur_pos;
                }
                else if (map[val] == 0 && val != zero_pos_value)
                {
                    map[val] = cur_pos;
                    ++cur_pos;
                }
            }
            else
            {
                T shifted_val = val - dict_size;
                if (cur_overflowed_pos == 0)
                {
                    zero_pos_overflowed_value = shifted_val;
                    ++cur_overflowed_pos;
                }
                else if (overflow_map[shifted_val] == 0 && shifted_val != zero_pos_overflowed_value)
                {
                    overflow_map[shifted_val] = cur_overflowed_pos;
                    ++cur_overflowed_pos;
                }
            }
        }

        auto dictionary_map = ColumnVector<T>::create(cur_pos);
        auto additional_keys_map = ColumnVector<T>::create(cur_overflowed_pos);
        auto & dict_data = dictionary_map->getData();
        auto & add_keys_data = additional_keys_map->getData();

        for (size_t i = 0; i < map_size; ++i)
            if (map[i])
                dict_data[map[i]] = static_cast<T>(i);

        for (size_t i = 0; i < overflow_map_size; ++i)
            if (overflow_map[i])
                add_keys_data[overflow_map[i]] = static_cast<T>(i);

        if (cur_pos)
            dict_data[0] = zero_pos_value;
        if (cur_overflowed_pos)
            add_keys_data[0] = zero_pos_overflowed_value;

        for (size_t i = 0; i < size; ++i)
        {
            T & val = index[i];
            if (val < dict_size)
                val = map[val];
            else
                val = overflow_map[val - dict_size] + cur_pos;
        }

        return {std::move(dictionary_map), std::move(additional_keys_map)};
    }

    /// Update column and return map with old indexes.
    /// Let N is the number of distinct values which are less than max_size;
    ///     old_column - column before function call;
    ///     new_column - column after function call:
    /// * if old_column[i] < max_size, than
    ///       dictionary_map[new_column[i]] = old_column[i]
    /// * else
    ///       additional_keys_map[new_column[i]] = old_column[i] - dict_size + N
    IndexMapsWithAdditionalKeys mapIndexWithAdditionalKeys(IColumn & column, size_t dict_size)
    {
        if (auto * data_uint8 = getIndexesData<UInt8>(column))
            return mapIndexWithAdditionalKeys(*data_uint8, dict_size);
        else if (auto * data_uint16 = getIndexesData<UInt16>(column))
            return mapIndexWithAdditionalKeys(*data_uint16, dict_size);
        else if (auto * data_uint32 = getIndexesData<UInt32>(column))
            return mapIndexWithAdditionalKeys(*data_uint32, dict_size);
        else if (auto * data_uint64 = getIndexesData<UInt64>(column))
            return mapIndexWithAdditionalKeys(*data_uint64, dict_size);
        else
            throw Exception("Indexes column for mapIndexWithAdditionalKeys must be UInt, got " + column.getName(),
                            ErrorCodes::LOGICAL_ERROR);
    }
}

void SerializationLowCardinality::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column,
    size_t offset,
    size_t limit,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    settings.path.push_back(Substream::DictionaryKeys);
    auto * keys_stream = settings.getter(settings.path);
    settings.path.back() = Substream::DictionaryIndexes;
    auto * indexes_stream = settings.getter(settings.path);
    settings.path.pop_back();

    if (!keys_stream && !indexes_stream)
        return;

    if (!keys_stream)
        throw Exception("Got empty stream for SerializationLowCardinality keys.", ErrorCodes::LOGICAL_ERROR);

    if (!indexes_stream)
        throw Exception("Got empty stream for SerializationLowCardinality indexes.", ErrorCodes::LOGICAL_ERROR);

    const ColumnLowCardinality & low_cardinality_column = typeid_cast<const ColumnLowCardinality &>(column);

    auto * low_cardinality_state = checkAndGetLowCardinalitySerializeState(state);
    auto & global_dictionary = low_cardinality_state->shared_dictionary;
    KeysSerializationVersion::checkVersion(low_cardinality_state->key_version.value);

    bool need_update_dictionary = global_dictionary == nullptr;
    if (need_update_dictionary)
        global_dictionary = DataTypeLowCardinality::createColumnUnique(*dictionary_type);

    size_t max_limit = column.size() - offset;
    limit = limit ? std::min(limit, max_limit) : max_limit;

    /// Do not write anything for empty column. (May happen while writing empty arrays.)
    if (limit == 0)
        return;

    auto sub_column = low_cardinality_column.cutAndCompact(offset, limit);
    ColumnPtr positions = sub_column->getIndexesPtr();
    ColumnPtr keys = sub_column->getDictionary().getNestedColumn();

    if (settings.low_cardinality_max_dictionary_size)
    {
        /// Insert used_keys into global dictionary and update sub_index.
        auto indexes_with_overflow = global_dictionary->uniqueInsertRangeWithOverflow(*keys, 0, keys->size(),
                                                                                      settings.low_cardinality_max_dictionary_size);
        size_t max_size = settings.low_cardinality_max_dictionary_size + indexes_with_overflow.overflowed_keys->size();
        ColumnLowCardinality::Index(indexes_with_overflow.indexes->getPtr()).check(max_size);

        if (global_dictionary->size() > settings.low_cardinality_max_dictionary_size)
            throw Exception("Got dictionary with size " + toString(global_dictionary->size()) +
                            " but max dictionary size is " + toString(settings.low_cardinality_max_dictionary_size),
                            ErrorCodes::LOGICAL_ERROR);

        positions = indexes_with_overflow.indexes->index(*positions, 0);
        keys = std::move(indexes_with_overflow.overflowed_keys);

        if (global_dictionary->size() < settings.low_cardinality_max_dictionary_size && !keys->empty())
            throw Exception("Has additional keys, but dict size is " + toString(global_dictionary->size()) +
                            " which is less then max dictionary size (" + toString(settings.low_cardinality_max_dictionary_size) + ")",
                            ErrorCodes::LOGICAL_ERROR);
    }

    if (const auto * nullable_keys = checkAndGetColumn<ColumnNullable>(*keys))
        keys = nullable_keys->getNestedColumnPtr();

    bool need_additional_keys = !keys->empty();
    bool need_dictionary = settings.low_cardinality_max_dictionary_size != 0;
    bool need_write_dictionary = !settings.low_cardinality_use_single_dictionary_for_part
                                 && global_dictionary->size() >= settings.low_cardinality_max_dictionary_size;

    IndexesSerializationType index_version(*positions, need_additional_keys, need_dictionary, need_update_dictionary);
    index_version.serialize(*indexes_stream);

    if (need_write_dictionary)
    {
        const auto & nested_column = global_dictionary->getNestedNotNullableColumn();
        UInt64 num_keys = nested_column->size();
        writeIntBinary(num_keys, *keys_stream);
        dict_inner_serialization->serializeBinaryBulk(*nested_column, *keys_stream, 0, num_keys);
        low_cardinality_state->shared_dictionary = nullptr;
    }

    if (need_additional_keys)
    {
        UInt64 num_keys = keys->size();
        writeIntBinary(num_keys, *indexes_stream);
        dict_inner_serialization->serializeBinaryBulk(*keys, *indexes_stream, 0, num_keys);
    }

    UInt64 num_rows = positions->size();
    writeIntBinary(num_rows, *indexes_stream);
    auto index_serialization = index_version.getDataType()->getDefaultSerialization();
    index_serialization->serializeBinaryBulk(*positions, *indexes_stream, 0, num_rows);
}

void SerializationLowCardinality::deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr & column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * /* cache */) const
{
    auto mutable_column = column->assumeMutable();
    ColumnLowCardinality & low_cardinality_column = typeid_cast<ColumnLowCardinality &>(*mutable_column);

    settings.path.push_back(Substream::DictionaryKeys);
    auto * keys_stream = settings.getter(settings.path);
    settings.path.back() = Substream::DictionaryIndexes;
    auto * indexes_stream = settings.getter(settings.path);
    settings.path.pop_back();

    if (!keys_stream && !indexes_stream)
        return;

    if (!keys_stream)
        throw Exception("Got empty stream for SerializationLowCardinality keys.", ErrorCodes::LOGICAL_ERROR);

    if (!indexes_stream)
        throw Exception("Got empty stream for SerializationLowCardinality indexes.", ErrorCodes::LOGICAL_ERROR);

    auto * low_cardinality_state = checkAndGetLowCardinalityDeserializeState(state);
    KeysSerializationVersion::checkVersion(low_cardinality_state->key_version.value);

    auto read_dictionary = [this, low_cardinality_state, keys_stream]()
    {
        UInt64 num_keys;
        readIntBinary(num_keys, *keys_stream);

        auto keys_type = removeNullable(dictionary_type);
        auto global_dict_keys = keys_type->createColumn();
        dict_inner_serialization->deserializeBinaryBulk(*global_dict_keys, *keys_stream, num_keys, 0);

        auto column_unique = DataTypeLowCardinality::createColumnUnique(*dictionary_type, std::move(global_dict_keys));
        low_cardinality_state->global_dictionary = std::move(column_unique);
    };

    auto read_additional_keys = [this, low_cardinality_state, indexes_stream]()
    {
        UInt64 num_keys;
        readIntBinary(num_keys, *indexes_stream);
        auto keys_type = removeNullable(dictionary_type);
        auto additional_keys = keys_type->createColumn();
        dict_inner_serialization->deserializeBinaryBulk(*additional_keys, *indexes_stream, num_keys, 0);
        low_cardinality_state->additional_keys = std::move(additional_keys);

        if (!low_cardinality_state->index_type.need_global_dictionary && dictionary_type->isNullable())
        {
            auto null_map = ColumnUInt8::create(num_keys, 0);
            if (num_keys)
                null_map->getElement(0) = 1;

            low_cardinality_state->null_map = std::move(null_map);
        }
    };

    auto read_indexes = [this, low_cardinality_state, indexes_stream, &low_cardinality_column](UInt64 num_rows)
    {
        auto indexes_type = low_cardinality_state->index_type.getDataType();
        MutableColumnPtr indexes_column = indexes_type->createColumn();
        indexes_type->getDefaultSerialization()->deserializeBinaryBulk(*indexes_column, *indexes_stream, num_rows, 0);

        auto & global_dictionary = low_cardinality_state->global_dictionary;
        const auto & additional_keys = low_cardinality_state->additional_keys;

        bool has_additional_keys = low_cardinality_state->index_type.has_additional_keys;
        bool column_is_empty = low_cardinality_column.empty();

        if (!low_cardinality_state->index_type.need_global_dictionary)
        {
            ColumnPtr keys_column = additional_keys;
            if (low_cardinality_state->null_map)
                keys_column = ColumnNullable::create(additional_keys, low_cardinality_state->null_map);
            low_cardinality_column.insertRangeFromDictionaryEncodedColumn(*keys_column, *indexes_column);
        }
        else if (!has_additional_keys)
        {
            if (column_is_empty)
                low_cardinality_column.setSharedDictionary(global_dictionary);

            auto local_column = ColumnLowCardinality::create(global_dictionary, std::move(indexes_column));
            low_cardinality_column.insertRangeFrom(*local_column, 0, num_rows);
        }
        else
        {
            auto maps = mapIndexWithAdditionalKeys(*indexes_column, global_dictionary->size());

            ColumnLowCardinality::Index(maps.additional_keys_map->getPtr()).check(additional_keys->size());

            ColumnLowCardinality::Index(indexes_column->getPtr()).check(
                    maps.dictionary_map->size() + maps.additional_keys_map->size());

            auto used_keys = IColumn::mutate(global_dictionary->getNestedColumn()->index(*maps.dictionary_map, 0));

            if (!maps.additional_keys_map->empty())
            {
                auto used_add_keys = additional_keys->index(*maps.additional_keys_map, 0);

                if (dictionary_type->isNullable())
                {
                    ColumnPtr null_map = ColumnUInt8::create(used_add_keys->size(), 0);
                    used_add_keys = ColumnNullable::create(used_add_keys, null_map);
                }

                used_keys->insertRangeFrom(*used_add_keys, 0, used_add_keys->size());
            }

            low_cardinality_column.insertRangeFromDictionaryEncodedColumn(*used_keys, *indexes_column);
        }
    };

    if (!settings.continuous_reading)
    {
        low_cardinality_state->num_pending_rows = 0;

        /// Remember in state that some granules were skipped and we need to update dictionary.
        low_cardinality_state->need_update_dictionary = true;
    }

    while (limit)
    {
        if (low_cardinality_state->num_pending_rows == 0)
        {
            if (indexes_stream->eof())
                break;

            auto & index_type = low_cardinality_state->index_type;
            auto & global_dictionary = low_cardinality_state->global_dictionary;

            index_type.deserialize(*indexes_stream);

            bool need_update_dictionary =
                !global_dictionary || index_type.need_update_dictionary || low_cardinality_state->need_update_dictionary;
            if (index_type.need_global_dictionary && need_update_dictionary)
            {
                read_dictionary();
                low_cardinality_state->need_update_dictionary = false;
            }

            if (low_cardinality_state->index_type.has_additional_keys)
                read_additional_keys();
            else
                low_cardinality_state->additional_keys = nullptr;

            readIntBinary(low_cardinality_state->num_pending_rows, *indexes_stream);
        }

        size_t num_rows_to_read = std::min<UInt64>(limit, low_cardinality_state->num_pending_rows);
        read_indexes(num_rows_to_read);
        limit -= num_rows_to_read;
        low_cardinality_state->num_pending_rows -= num_rows_to_read;
    }

    column = std::move(mutable_column);
}

void SerializationLowCardinality::serializeBinary(const Field & field, WriteBuffer & ostr) const
{
    dictionary_type->getDefaultSerialization()->serializeBinary(field, ostr);
}
void SerializationLowCardinality::deserializeBinary(Field & field, ReadBuffer & istr) const
{
    dictionary_type->getDefaultSerialization()->deserializeBinary(field, istr);
}

void SerializationLowCardinality::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    serializeImpl(column, row_num, &ISerialization::serializeBinary, ostr);
}
void SerializationLowCardinality::deserializeBinary(IColumn & column, ReadBuffer & istr) const
{
    deserializeImpl(column, &ISerialization::deserializeBinary, istr);
}

void SerializationLowCardinality::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serializeImpl(column, row_num, &ISerialization::serializeTextEscaped, ostr, settings);
}

void SerializationLowCardinality::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserializeImpl(column, &ISerialization::deserializeTextEscaped, istr, settings);
}

void SerializationLowCardinality::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serializeImpl(column, row_num, &ISerialization::serializeTextQuoted, ostr, settings);
}

void SerializationLowCardinality::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserializeImpl(column, &ISerialization::deserializeTextQuoted, istr, settings);
}

void SerializationLowCardinality::deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserializeImpl(column, &ISerialization::deserializeWholeText, istr, settings);
}

void SerializationLowCardinality::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serializeImpl(column, row_num, &ISerialization::serializeTextCSV, ostr, settings);
}

void SerializationLowCardinality::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserializeImpl(column, &ISerialization::deserializeTextCSV, istr, settings);
}

void SerializationLowCardinality::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serializeImpl(column, row_num, &ISerialization::serializeText, ostr, settings);
}

void SerializationLowCardinality::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serializeImpl(column, row_num, &ISerialization::serializeTextJSON, ostr, settings);
}
void SerializationLowCardinality::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserializeImpl(column, &ISerialization::deserializeTextJSON, istr, settings);
}

void SerializationLowCardinality::serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serializeImpl(column, row_num, &ISerialization::serializeTextXML, ostr, settings);
}

template <typename... Params, typename... Args>
void SerializationLowCardinality::serializeImpl(
    const IColumn & column, size_t row_num, SerializationLowCardinality::SerializeFunctionPtr<Params...> func, Args &&... args) const
{
    const auto & low_cardinality_column = getColumnLowCardinality(column);
    size_t unique_row_number = low_cardinality_column.getIndexes().getUInt(row_num);
    auto serialization = dictionary_type->getDefaultSerialization();
    (serialization.get()->*func)(*low_cardinality_column.getDictionary().getNestedColumn(), unique_row_number, std::forward<Args>(args)...);
}

template <typename... Params, typename... Args>
void SerializationLowCardinality::deserializeImpl(
    IColumn & column, SerializationLowCardinality::DeserializeFunctionPtr<Params...> func, Args &&... args) const
{
    auto & low_cardinality_column= getColumnLowCardinality(column);
    auto temp_column = low_cardinality_column.getDictionary().getNestedColumn()->cloneEmpty();

    auto serialization = dictionary_type->getDefaultSerialization();
    (serialization.get()->*func)(*temp_column, std::forward<Args>(args)...);

    low_cardinality_column.insertFromFullColumn(*temp_column, 0);
}

}
