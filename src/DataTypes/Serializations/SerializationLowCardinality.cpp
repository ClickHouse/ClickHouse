#include <DataTypes/Serializations/SerializationLowCardinality.h>
#include <Formats/FormatSettings.h>
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
#include <Common/SipHash.h>
#include <Core/Field.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_DATA;
    extern const int CANNOT_READ_ALL_DATA;
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

    /// The key count comes from the stream and must not size an allocation, so the keys are read in batches.
    MutableColumnPtr deserializeDictionaryKeys(const IDataType & keys_type, const ISerialization & keys_serialization, ReadBuffer & istr, UInt64 num_keys)
    {
        auto keys = keys_type.createColumn();
        while (keys->size() < num_keys)
        {
            const size_t keys_read = keys->size();
            keys_serialization.deserializeBinaryBulk(*keys, istr, std::min<UInt64>(num_keys - keys_read, DEFAULT_BLOCK_SIZE), 0);
            if (keys->size() == keys_read)
                throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA,
                    "Cannot read all keys of a LowCardinality dictionary: read {} of {}", keys_read, num_keys);
        }
        return keys;
    }

    MutableColumnPtr deserializeDictionaryKeys(const IDataType & keys_type, const ISerialization & keys_serialization, ReadBuffer & istr)
    {
        UInt64 num_keys = 0;
        readBinaryLittleEndian(num_keys, istr);
        return deserializeDictionaryKeys(keys_type, keys_serialization, istr, num_keys);
    }
}

SerializationLowCardinality::SerializationLowCardinality(const DataTypePtr & dictionary_type_)
     : dictionary_type(dictionary_type_)
     , nested_serialization(dictionary_type_->getDefaultSerialization())
     , dict_inner_serialization(removeNullable(dictionary_type_)->getDefaultSerialization())
{
}

UInt128 SerializationLowCardinality::getHash(const DataTypePtr & dictionary_type_)
{
    /// Include the nested serializations' hashes so types whose default
    /// serialization depends on session settings get distinct cache entries.
    auto nested_serialization = dictionary_type_->getDefaultSerialization();
    auto dict_inner_serialization = removeNullable(dictionary_type_)->getDefaultSerialization();

    SipHash hash;
    hash.update("LowCardinality");
    auto dict_type_name = dictionary_type_->getName();
    hash.update(dict_type_name.size());
    hash.update(dict_type_name);
    if (nested_serialization->supportsPooling())
        hash.update(nested_serialization->getHash());
    if (dict_inner_serialization->supportsPooling()
        && dict_inner_serialization.get() != nested_serialization.get())
        hash.update(dict_inner_serialization->getHash());
    return hash.get128();
}

SerializationPtr SerializationLowCardinality::create(const DataTypePtr & dictionary_type_)
{
    if (!removeNullable(dictionary_type_)->getDefaultSerialization()->supportsPooling())
        return std::shared_ptr<ISerialization>(new SerializationLowCardinality(dictionary_type_));
    return ISerialization::pooled(getHash(dictionary_type_), [&] { return new SerializationLowCardinality(dictionary_type_); });
}

void SerializationLowCardinality::enumerateStreams(
    EnumerateStreamsSettings & settings,
    const StreamCallback & callback,
    const SubstreamData & data) const
{
    const auto * column_lc = data.column ? &getColumnLowCardinality(*data.column) : nullptr;

    if (settings.use_specialized_prefixes_and_suffixes_substreams)
    {
        settings.path.push_back(Substream::DictionaryKeysPrefix);
        callback(settings.path);
        settings.path.pop_back();
    }

    settings.path.push_back(Substream::DictionaryKeys);
    auto dict_data = SubstreamData(dict_inner_serialization)
        .withType(data.type ? dictionary_type : nullptr)
        .withColumn(column_lc ? column_lc->getDictionary().getNestedColumn() : nullptr)
        .withSerializationInfo(data.serialization_info);

    settings.path.back().data = dict_data;
    callback(settings.path);

    settings.path.back() = Substream::DictionaryIndexes;
    settings.path.back().data = data;

    callback(settings.path);
    settings.path.pop_back();
}

/// Written instead of the number of keys of the next dictionary after the last dictionary in the `DictionaryKeys`
/// stream, if all keys of the column are in its dictionaries. See `serializeBinaryBulkStateSuffix`.
/// The marker is followed by a UInt64 with these flags: whether the rows use the `NULL` and the default value,
/// which every dictionary contains anyway.
constexpr UInt64 COMPLETE_DICTIONARIES_MARKER = 0;
constexpr UInt64 COMPLETE_DICTIONARIES_HAS_NULL = 1;
constexpr UInt64 COMPLETE_DICTIONARIES_HAS_DEFAULT = 2;

namespace
{

template <typename T>
bool containsIndexImpl(const IColumn & positions, UInt64 index, bool & found)
{
    const auto * column = typeid_cast<const ColumnVector<T> *>(&positions);
    if (!column)
        return false;
    const auto & data = column->getData();
    found = std::find(data.begin(), data.end(), static_cast<T>(index)) != data.end();
    return true;
}

bool containsIndex(const IColumn & positions, UInt64 index)
{
    bool found = false;
    if (containsIndexImpl<UInt8>(positions, index, found) || containsIndexImpl<UInt16>(positions, index, found)
        || containsIndexImpl<UInt32>(positions, index, found) || containsIndexImpl<UInt64>(positions, index, found))
        return found;
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Indexes column for LowCardinality must be UInt, got {}", positions.getName());
}

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
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid version for SerializationLowCardinality key column.");
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

        throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid type for SerializationLowCardinality index column.");
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
        writeBinaryLittleEndian(val, buffer);
    }

    void deserialize(ReadBuffer & buffer, const ISerialization::DeserializeBinaryBulkSettings & settings)
    {
        SerializationType val = 0;
        readBinaryLittleEndian(val, buffer);

        checkType(val);
        has_additional_keys = (val & HasAdditionalKeysBit) != 0;
        need_global_dictionary = (val & NeedGlobalDictionaryBit) != 0;
        need_update_dictionary = (val & NeedUpdateDictionary) != 0;
        type = static_cast<Type>(resetFlags(val));

        if (settings.native_format)
        {
            if (need_global_dictionary)
                throw Exception(ErrorCodes::INCORRECT_DATA,
                                "LowCardinality indexes serialization type for Native format "
                                "cannot use global dictionary");
        }
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
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid Indexes column for IndexesSerializationType. "
                            "Expected ColumnUInt*, got {}", column.getName());
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

        throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't create DataType from IndexesSerializationType.");
    }

    IndexesSerializationType() = default;
};

struct SerializeStateLowCardinality : public ISerialization::SerializeBinaryBulkState
{
    KeysSerializationVersion key_version;
    MutableColumnUniquePtr shared_dictionary;

    /// Whether some keys were written as additional keys into the `DictionaryIndexes` stream,
    /// so that the dictionaries in the `DictionaryKeys` stream do not contain all keys of the column.
    bool has_additional_keys = false;

    /// Whether at least one dictionary was written into the `DictionaryKeys` stream.
    bool has_dictionaries = false;

    /// Whether the rows use the `NULL` or the default value of the dictionaries (only for the marker of complete dictionaries).
    bool uses_null = false;
    bool uses_default = false;

    explicit SerializeStateLowCardinality(UInt64 key_version_) : key_version(key_version_) {}
};

struct DeserializeStateLowCardinality : public ISerialization::DeserializeBinaryBulkState
{
    KeysSerializationVersion key_version;
    ColumnUniquePtr global_dictionary;

    IndexesSerializationType index_type{};
    ColumnPtr additional_keys;
    ColumnPtr null_map;
    UInt64 num_pending_rows = 0;

    /// If dictionary should be updated.
    /// Can happen is some granules was skipped while reading from MergeTree.
    /// We should store this flag in State because
    ///   in case of long block of empty arrays we may not need read dictionary at first reading.
    bool need_update_dictionary = false;

    /// True if this part has a single dictionary for the entire column. The prefix
    /// reader first checks `has_uniform_marks_callback` as a cheap prefilter, then
    /// verifies it by reading the first dictionary and checking that `DictionaryKeys`
    /// reached EOF. When true, non-continuous reads (granule skips) do not invalidate
    /// the cached dictionary, and the dictionary can be shared across all threads via `clone`.
    bool single_dictionary_for_part = false;

    explicit DeserializeStateLowCardinality(UInt64 key_version_) : key_version(key_version_) {}

    ISerialization::DeserializeBinaryBulkStatePtr cloneImpl(ISerialization::DeserializeBinaryBulkStateCloneContext &) const override
    {
        return std::make_shared<DeserializeStateLowCardinality>(*this);
    }
};

void SerializationLowCardinality::serializeBinaryBulkStatePrefix(
    const IColumn & /*column*/,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    settings.path.push_back(settings.use_specialized_prefixes_and_suffixes_substreams ? Substream::DictionaryKeysPrefix : Substream::DictionaryKeys);
    auto * stream = settings.getter(settings.path);
    settings.path.pop_back();

    if (!stream)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Got empty stream in SerializationLowCardinality::serializeBinaryBulkStatePrefix");

    /// Write version and create SerializeBinaryBulkState.
    UInt64 key_version = KeysSerializationVersion::SharedDictionariesWithAdditionalKeys;

    writeBinaryLittleEndian(key_version, *stream);

    state = std::make_shared<SerializeStateLowCardinality>(key_version);
}

void SerializationLowCardinality::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    auto * low_cardinality_state = checkAndGetState<SerializeStateLowCardinality>(state);
    KeysSerializationVersion::checkVersion(low_cardinality_state->key_version.value);

    if (low_cardinality_state->shared_dictionary && settings.low_cardinality_max_dictionary_size)
    {
        auto nested_column = low_cardinality_state->shared_dictionary->getNestedNotNullableColumn();

        settings.path.push_back(Substream::DictionaryKeys);
        auto * stream = settings.getter(settings.path);
        settings.path.pop_back();

        if (!stream)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Got empty stream in SerializationLowCardinality::serializeBinaryBulkStateSuffix");

        UInt64 num_keys = nested_column->size();
        writeBinaryLittleEndian(num_keys, *stream);
        dict_inner_serialization->serializeBinaryBulk(*nested_column, *stream, 0, num_keys);
        low_cardinality_state->shared_dictionary = nullptr;
        low_cardinality_state->has_dictionaries = true;
    }

    /// A dictionary always contains the default value, so a dictionary of zero keys is never written,
    /// and the zero number of keys after the last dictionary marks that the dictionaries contain all keys.
    /// Readers never read past the last dictionary, so the marker is invisible to them.
    /// Only a top-level column is marked: its dictionaries are what `deserializeCompleteDictionaries` reads.
    /// The marker always follows a dictionary, so a reader that takes it for the size of a dictionary
    /// has read a dictionary before and can tell them apart.
    if (settings.low_cardinality_write_complete_dictionaries_marker && settings.path.empty()
        && settings.low_cardinality_max_dictionary_size && low_cardinality_state->has_dictionaries
        && !low_cardinality_state->has_additional_keys)
    {
        settings.path.push_back(Substream::DictionaryKeys);
        auto * stream = settings.getter(settings.path);
        settings.path.pop_back();

        if (!stream)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Got empty stream in SerializationLowCardinality::serializeBinaryBulkStateSuffix");

        writeBinaryLittleEndian(COMPLETE_DICTIONARIES_MARKER, *stream);
        UInt64 flags = (low_cardinality_state->uses_null ? COMPLETE_DICTIONARIES_HAS_NULL : 0)
            | (low_cardinality_state->uses_default ? COMPLETE_DICTIONARIES_HAS_DEFAULT : 0);
        writeBinaryLittleEndian(flags, *stream);
    }
}

std::optional<Columns> SerializationLowCardinality::deserializeCompleteDictionaries(ReadBuffer & keys_stream) const
{
    UInt64 keys_version = 0;
    readBinaryLittleEndian(keys_version, keys_stream);
    KeysSerializationVersion::checkVersion(keys_version);

    auto keys_type = removeNullable(dictionary_type);
    std::vector<MutableColumnUniquePtr> dictionaries;

    while (!keys_stream.eof())
    {
        UInt64 num_keys = 0;
        readBinaryLittleEndian(num_keys, keys_stream);

        if (num_keys == COMPLETE_DICTIONARIES_MARKER)
            break;

        auto keys = deserializeDictionaryKeys(*keys_type, *dict_inner_serialization, keys_stream, num_keys);
        dictionaries.push_back(DataTypeLowCardinality::createColumnUnique(*dictionary_type, std::move(keys)));
    }

    if (keys_stream.eof())
        return std::nullopt;

    UInt64 flags = 0;
    readBinaryLittleEndian(flags, keys_stream);
    if (!keys_stream.eof())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected data after the marker of complete LowCardinality dictionaries");

    /// The rows use every key of the dictionaries, and also `NULL` and the default value if the flags say so.
    /// A function that cannot be executed on the default value is then executed only on the values of the rows,
    /// like when the column is read, see `IExecutableFunction::executeWithoutSparseColumns`.
    Columns result;
    result.reserve(dictionaries.size());
    for (auto & dictionary : dictionaries)
    {
        auto positions = ColumnUInt64::create();
        auto & positions_data = positions->getData();
        positions_data.reserve(dictionary->size());
        for (size_t i = 0; i < dictionary->size(); ++i)
        {
            if (dictionary_type->isNullable() && i == dictionary->getNullValueIndex() && !(flags & COMPLETE_DICTIONARIES_HAS_NULL))
                continue;
            if (i == dictionary->getDefaultValueIndex() && !(flags & COMPLETE_DICTIONARIES_HAS_DEFAULT))
                continue;
            positions_data.push_back(i);
        }

        if (positions_data.empty())
            continue;

        ColumnPtr dictionary_ptr = std::move(dictionary);
        ColumnPtr positions_ptr = std::move(positions);
        result.push_back(ColumnLowCardinality::create(dictionary_ptr, positions_ptr, /*is_shared=*/ false));
    }

    return result;
}

void SerializationLowCardinality::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsDeserializeStatesCache * cache) const
{
    settings.path.push_back(settings.use_specialized_prefixes_and_suffixes_substreams ? Substream::DictionaryKeysPrefix : Substream::DictionaryKeys);
    const auto dictionary_keys_path = settings.path;
    settings.path.pop_back();

    if (auto cached_state = getFromSubstreamsDeserializeStatesCache(cache, dictionary_keys_path))
    {
        state = std::move(cached_state);
        return;
    }

    /// First use marks as a cheap prefilter: a true single-dictionary part must
    /// have uniform positions in the `DictionaryKeys` stream. The check is not
    /// exact because several dictionaries can be written inside one mark interval.
    bool might_have_single_dictionary = settings.has_uniform_marks_callback
        && settings.data_part_type == MergeTreeDataPartType::Wide
        && settings.has_uniform_marks_callback(dictionary_keys_path, 2);

    auto * stream = settings.getter(dictionary_keys_path);

    if (!stream)
        return;

    UInt64 keys_version = 0;
    readBinaryLittleEndian(keys_version, *stream);

    auto new_state = std::make_shared<DeserializeStateLowCardinality>(keys_version);

    if (might_have_single_dictionary && !stream->eof())
    {
        /// For `Wide` parts that might have a single dictionary, the stream is
        /// now positioned right at the dictionary data (after the version).
        /// Read the first dictionary and check EOF afterwards. If this proves
        /// that there are no more dictionaries, all threads sharing this prefix
        /// state via `DeserializationPrefixesCache` get the dictionary through
        /// a `shared_ptr` clone instead of each reading it independently.
        ///
        /// Some streams are allowed to contain only `keys_version` with no dictionary
        /// body afterwards, for example unused `LowCardinality` alternatives inside
        /// `Variant` or empty nested `LowCardinality` streams. In that case `eof`
        /// is true and we keep `global_dictionary` empty.
        auto global_dict_keys = deserializeDictionaryKeys(*removeNullable(dictionary_type), *dict_inner_serialization, *stream);

        auto global_dictionary = DataTypeLowCardinality::createColumnUnique(
            *dictionary_type, std::move(global_dict_keys));

        /// `Wide` part readers mark `DictionaryKeys` streams as LowCardinality
        /// dictionary streams. When all relevant marks have the same position,
        /// `MergeTreeReaderStreamSingleColumn::getRightOffset` extends the read
        /// bound to the next different mark or to the file end. So this `eof`
        /// check is not limited by the current task range in the only case where
        /// `might_have_single_dictionary` can prove useful.
        /// The dictionary can be followed by the marker of complete dictionaries (see `serializeBinaryBulkStateSuffix`).
        /// If anything else follows it, the stream is read again from the start below.
        auto is_last_dictionary = [&]
        {
            if (stream->eof())
                return true;
            UInt64 next_num_keys = 0;
            readBinaryLittleEndian(next_num_keys, *stream);
            if (next_num_keys != COMPLETE_DICTIONARIES_MARKER || stream->eof())
                return false;
            UInt64 flags = 0;
            readBinaryLittleEndian(flags, *stream);
            return stream->eof();
        };

        if (is_last_dictionary())
        {
            new_state->single_dictionary_for_part = true;
            new_state->global_dictionary = std::move(global_dictionary);
        }
        else
        {
            /// The mark heuristic can be fooled by several dictionaries written
            /// inside one mark interval. Reset the `DictionaryKeys` stream back
            /// to the normal post-prefix position and let data deserialization
            /// read dictionary updates from the stream.
            if (!settings.seek_to_start_callback)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot reset LowCardinality dictionary stream without seek_to_start_callback");

            settings.seek_to_start_callback(dictionary_keys_path);

            UInt64 repeated_keys_version = 0;
            readBinaryLittleEndian(repeated_keys_version, *stream);
            if (repeated_keys_version != keys_version)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Inconsistent version while resetting LowCardinality dictionary stream");
        }
    }

    /// All readers of this stream must share the state: only one of them decodes each range, the others take it
    /// from the substreams cache.
    addToSubstreamsDeserializeStatesCache(cache, dictionary_keys_path, new_state);
    state = std::move(new_state);
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
                    map[val] = static_cast<T>(cur_pos);
                    ++cur_pos;
                }
            }
            else
            {
                T shifted_val = static_cast<T>(val - dict_size);
                if (cur_overflowed_pos == 0)
                {
                    zero_pos_overflowed_value = shifted_val;
                    ++cur_overflowed_pos;
                }
                else if (overflow_map[shifted_val] == 0 && shifted_val != zero_pos_overflowed_value)
                {
                    overflow_map[shifted_val] = static_cast<T>(cur_overflowed_pos);
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
                val = overflow_map[val - dict_size] + static_cast<T>(cur_pos);
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
        if (auto * data_uint16 = getIndexesData<UInt16>(column))
            return mapIndexWithAdditionalKeys(*data_uint16, dict_size);
        if (auto * data_uint32 = getIndexesData<UInt32>(column))
            return mapIndexWithAdditionalKeys(*data_uint32, dict_size);
        if (auto * data_uint64 = getIndexesData<UInt64>(column))
            return mapIndexWithAdditionalKeys(*data_uint64, dict_size);
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Indexes column for mapIndexWithAdditionalKeys must be UInt, got {}", column.getName());
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
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Got empty stream for SerializationLowCardinality keys.");

    if (!indexes_stream)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Got empty stream for SerializationLowCardinality indexes.");

    const ColumnLowCardinality & low_cardinality_column = typeid_cast<const ColumnLowCardinality &>(column);

    auto * low_cardinality_state = checkAndGetState<SerializeStateLowCardinality>(state);
    auto & global_dictionary = low_cardinality_state->shared_dictionary;
    KeysSerializationVersion::checkVersion(low_cardinality_state->key_version.value);

    size_t max_limit = column.size() - offset;
    limit = limit ? std::min(limit, max_limit) : max_limit;

    /// Do not write anything for empty column. (May happen while writing empty arrays.)
    if (limit == 0)
        return;

    bool need_update_dictionary = global_dictionary == nullptr;
    if (need_update_dictionary)
        global_dictionary = DataTypeLowCardinality::createColumnUnique(*dictionary_type);

    auto sub_column = low_cardinality_column.cutAndCompact(offset, limit);
    ColumnPtr positions = sub_column->getIndexesPtr();
    ColumnPtr keys = sub_column->getDictionary().getNestedColumn();

    if (settings.low_cardinality_max_dictionary_size)
    {
        /// Insert used_keys into global dictionary and update sub_index.
        auto indexes_with_overflow = global_dictionary->uniqueInsertRangeWithOverflow(*keys, 0, keys->size(),
                                                                                      settings.low_cardinality_max_dictionary_size);

        if (global_dictionary->size() > settings.low_cardinality_max_dictionary_size)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Got dictionary with size {} but max dictionary size is {}",
                            global_dictionary->size(), settings.low_cardinality_max_dictionary_size);

        positions = indexes_with_overflow.indexes->index(*positions, 0);
        keys = std::move(indexes_with_overflow.overflowed_keys);

        if (global_dictionary->size() < settings.low_cardinality_max_dictionary_size && !keys->empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Has additional keys, but dict size is {} which is less "
                            "then max dictionary size ({})", global_dictionary->size(),
                            settings.low_cardinality_max_dictionary_size);
    }

    if (const auto * nullable_keys = checkAndGetColumn<ColumnNullable>(&*keys))
        keys = nullable_keys->getNestedColumnPtr();

    /// The marker of complete dictionaries tells whether the rows use the values that every dictionary has.
    /// If there are additional keys, the marker is not written, and the positions are not only in the dictionary.
    if (settings.low_cardinality_write_complete_dictionaries_marker && settings.path.empty()
        && keys->empty() && !low_cardinality_state->has_additional_keys)
    {
        if (!low_cardinality_state->uses_null && dictionary_type->isNullable())
            low_cardinality_state->uses_null = containsIndex(*positions, global_dictionary->getNullValueIndex());
        if (!low_cardinality_state->uses_default)
            low_cardinality_state->uses_default = containsIndex(*positions, global_dictionary->getDefaultValueIndex());
    }

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
        writeBinaryLittleEndian(num_keys, *keys_stream);
        dict_inner_serialization->serializeBinaryBulk(*nested_column, *keys_stream, 0, num_keys);
        low_cardinality_state->shared_dictionary = nullptr;
        low_cardinality_state->has_dictionaries = true;
    }

    if (need_additional_keys)
    {
        low_cardinality_state->has_additional_keys = true;
        UInt64 num_keys = keys->size();
        writeBinaryLittleEndian(num_keys, *indexes_stream);
        dict_inner_serialization->serializeBinaryBulk(*keys, *indexes_stream, 0, num_keys);
    }

    UInt64 num_rows = positions->size();
    writeBinaryLittleEndian(num_rows, *indexes_stream);
    auto index_serialization = index_version.getDataType()->getDefaultSerialization();
    index_serialization->serializeBinaryBulk(*positions, *indexes_stream, 0, num_rows);
}

void SerializationLowCardinality::deserializeBinaryBulkWithMultipleStreams(
    IColumn & column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    if (insertDataFromSubstreamsCacheIfAny(cache, settings, column))
        return;

    size_t prev_size = column.size();
    ColumnLowCardinality & low_cardinality_column = typeid_cast<ColumnLowCardinality &>(column);

    settings.path.push_back(Substream::DictionaryKeys);
    auto * keys_stream = settings.getter(settings.path);
    settings.path.back() = Substream::DictionaryIndexes;
    auto * indexes_stream = settings.getter(settings.path);
    settings.path.pop_back();

    if (!keys_stream && !indexes_stream)
        return;

    if (!keys_stream)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Got empty stream for SerializationLowCardinality keys.");

    if (!indexes_stream)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Got empty stream for SerializationLowCardinality indexes.");

    auto * low_cardinality_state = checkAndGetState<DeserializeStateLowCardinality>(state);
    KeysSerializationVersion::checkVersion(low_cardinality_state->key_version.value);

    auto read_dictionary = [this, low_cardinality_state, keys_stream]()
    {
        auto global_dict_keys = deserializeDictionaryKeys(*removeNullable(dictionary_type), *dict_inner_serialization, *keys_stream);

        auto column_unique = DataTypeLowCardinality::createColumnUnique(*dictionary_type, std::move(global_dict_keys));
        low_cardinality_state->global_dictionary = std::move(column_unique);
    };

    auto read_additional_keys = [this, low_cardinality_state, indexes_stream]()
    {
        auto additional_keys = deserializeDictionaryKeys(*removeNullable(dictionary_type), *dict_inner_serialization, *indexes_stream);
        const size_t num_keys = additional_keys->size();
        low_cardinality_state->additional_keys = std::move(additional_keys);

        if (!low_cardinality_state->index_type.need_global_dictionary && dictionary_type->isNullable())
        {
            auto null_map = ColumnUInt8::create(num_keys, static_cast<UInt8>(0));
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

        /// Indexes come from the stream.
        if (low_cardinality_state->index_type.need_global_dictionary)
        {
            const size_t num_additional_keys = additional_keys ? additional_keys->size() : 0;
            ColumnLowCardinality::checkIndexesAreLimited(*indexes_column, global_dictionary->size() + num_additional_keys);
        }

        if (!low_cardinality_state->index_type.need_global_dictionary)
        {
            if (additional_keys == nullptr)
                throw Exception(ErrorCodes::INCORRECT_DATA, "No additional keys found.");

            ColumnPtr keys_column = additional_keys;
            if (low_cardinality_state->null_map)
                keys_column = ColumnNullable::create(additional_keys, low_cardinality_state->null_map);
            low_cardinality_column.insertRangeFromDictionaryEncodedColumn(*keys_column, *indexes_column);
        }
        else if (!has_additional_keys)
        {
            if (column_is_empty)
                low_cardinality_column.setSharedDictionary(global_dictionary);

            auto local_column = ColumnLowCardinality::create(global_dictionary, std::move(indexes_column), /*is_shared=*/true);
            low_cardinality_column.insertRangeFrom(*local_column, 0, num_rows);
        }
        else
        {
            auto maps = mapIndexWithAdditionalKeys(*indexes_column, global_dictionary->size());

            auto used_keys = IColumn::mutate(global_dictionary->getNestedColumn()->index(*maps.dictionary_map, 0));

            if (!maps.additional_keys_map->empty())
            {
                if (additional_keys == nullptr)
                    throw Exception(ErrorCodes::INCORRECT_DATA, "No additional keys found.");

                auto used_add_keys = additional_keys->index(*maps.additional_keys_map, 0);

                if (dictionary_type->isNullable())
                {
                    ColumnPtr null_map = ColumnUInt8::create(used_add_keys->size(), static_cast<UInt8>(0));
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

        /// Some granules were skipped; the next granule header may signal a dictionary
        /// update. For multi-dict parts this forces a re-read at the next boundary.
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

            index_type.deserialize(*indexes_stream, settings);

            bool need_update_dictionary =
                !global_dictionary || index_type.need_update_dictionary || low_cardinality_state->need_update_dictionary;
            if (index_type.need_global_dictionary && need_update_dictionary)
            {
                /// For single-dict parts the in-memory dictionary is valid for the entire part,
                /// so skip re-reading it even if the stream was seeked back to the beginning.
                if (!low_cardinality_state->single_dictionary_for_part || !global_dictionary)
                    read_dictionary();
                low_cardinality_state->need_update_dictionary = false;
            }

            if (low_cardinality_state->index_type.has_additional_keys)
                read_additional_keys();
            else
                low_cardinality_state->additional_keys = nullptr;

            readBinaryLittleEndian(low_cardinality_state->num_pending_rows, *indexes_stream);
        }

        size_t num_rows_to_read = std::min<UInt64>(limit, low_cardinality_state->num_pending_rows);
        read_indexes(num_rows_to_read);
        limit -= num_rows_to_read;
        low_cardinality_state->num_pending_rows -= num_rows_to_read;
    }

    size_t num_read_rows = column.size() - prev_size;
    addColumnWithNumReadRowsToSubstreamsCache(cache, settings.path, column.getPtr(), num_read_rows);
}

void SerializationLowCardinality::serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings & settings) const
{
    nested_serialization->serializeBinary(field, ostr, settings);
}
void SerializationLowCardinality::deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings & settings) const
{
    nested_serialization->deserializeBinary(field, istr, settings);
}

void SerializationLowCardinality::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serializeImpl(column, row_num, &ISerialization::serializeBinary, ostr, settings);
}
void SerializationLowCardinality::deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserializeImpl(column, &ISerialization::deserializeBinary, istr, settings);
}

void SerializationLowCardinality::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serializeImpl(column, row_num, &ISerialization::serializeTextEscaped, ostr, settings);
}

void SerializationLowCardinality::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserializeImpl(column, &ISerialization::deserializeTextEscaped, istr, settings);
}

bool SerializationLowCardinality::tryDeserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    return tryDeserializeImpl(column, &ISerialization::tryDeserializeTextEscaped, istr, settings);
}

void SerializationLowCardinality::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serializeImpl(column, row_num, &ISerialization::serializeTextQuoted, ostr, settings);
}

void SerializationLowCardinality::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserializeImpl(column, &ISerialization::deserializeTextQuoted, istr, settings);
}

bool SerializationLowCardinality::tryDeserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    return tryDeserializeImpl(column, &ISerialization::tryDeserializeTextQuoted, istr, settings);
}

void SerializationLowCardinality::deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserializeImpl(column, &ISerialization::deserializeWholeText, istr, settings);
}

bool SerializationLowCardinality::tryDeserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    return tryDeserializeImpl(column, &ISerialization::tryDeserializeWholeText, istr, settings);
}

void SerializationLowCardinality::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serializeImpl(column, row_num, &ISerialization::serializeTextCSV, ostr, settings);
}

void SerializationLowCardinality::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserializeImpl(column, &ISerialization::deserializeTextCSV, istr, settings);
}

bool SerializationLowCardinality::tryDeserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    return tryDeserializeImpl(column, &ISerialization::tryDeserializeTextCSV, istr, settings);
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

bool SerializationLowCardinality::tryDeserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    return tryDeserializeImpl(column, &ISerialization::tryDeserializeTextJSON, istr, settings);
}

void SerializationLowCardinality::serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serializeImpl(column, row_num, &ISerialization::serializeTextXML, ostr, settings);
}

void SerializationLowCardinality::deserializeTextRaw(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserializeImpl(column, &ISerialization::deserializeTextRaw, istr, settings);
}

bool SerializationLowCardinality::tryDeserializeTextRaw(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    return tryDeserializeImpl(column, &ISerialization::tryDeserializeTextRaw, istr, settings);
}

void SerializationLowCardinality::serializeTextRaw(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serializeImpl(column, row_num, &ISerialization::serializeTextRaw, ostr, settings);
}

template <typename... Params, typename... Args>
void SerializationLowCardinality::serializeImpl(
    const IColumn & column, size_t row_num, SerializationLowCardinality::SerializeFunctionPtr<Params...> func, Args &&... args) const
{
    const auto & low_cardinality_column = getColumnLowCardinality(column);
    size_t unique_row_number = low_cardinality_column.getIndexes().getUInt(row_num);
    (nested_serialization.get()->*func)(*low_cardinality_column.getDictionary().getNestedColumn(), unique_row_number, std::forward<Args>(args)...);
}

template <typename... Params, typename... Args>
void SerializationLowCardinality::deserializeImpl(
    IColumn & column, SerializationLowCardinality::DeserializeFunctionPtr<Params...> func, Args &&... args) const
{
    auto & low_cardinality_column = getColumnLowCardinality(column);
    auto temp_column = low_cardinality_column.getDictionary().getNestedColumn()->cloneEmpty();

    (nested_serialization.get()->*func)(*temp_column, std::forward<Args>(args)...);
    low_cardinality_column.insertFromFullColumn(*temp_column, 0);
}

template <typename... Params, typename... Args>
bool SerializationLowCardinality::tryDeserializeImpl(
    IColumn & column, SerializationLowCardinality::TryDeserializeFunctionPtr<Params...> func, Args &&... args) const
{
    auto & low_cardinality_column = getColumnLowCardinality(column);
    auto temp_column = low_cardinality_column.getDictionary().getNestedColumn()->cloneEmpty();

    if (!(nested_serialization.get()->*func)(*temp_column, std::forward<Args>(args)...))
        return false;

    low_cardinality_column.insertFromFullColumn(*temp_column, 0);
    return true;
}

}
