#include <Columns/ColumnWithDictionary.h>
#include <Columns/ColumnUnique.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnsCommon.h>
#include <Common/typeid_cast.h>
#include <Core/TypeListNumber.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeWithDictionary.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <Parsers/IAST.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{
    const ColumnWithDictionary & getColumnWithDictionary(const IColumn & column)
    {
        return typeid_cast<const ColumnWithDictionary &>(column);
    }

    ColumnWithDictionary & getColumnWithDictionary(IColumn & column)
    {
        return typeid_cast<ColumnWithDictionary &>(column);
    }
}

DataTypeWithDictionary::DataTypeWithDictionary(DataTypePtr dictionary_type_, DataTypePtr indexes_type_)
        : dictionary_type(std::move(dictionary_type_)), indexes_type(std::move(indexes_type_))
{
    if (!indexes_type->isUnsignedInteger())
        throw Exception("Index type of DataTypeWithDictionary must be unsigned integer, but got "
                        + indexes_type->getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    auto inner_type = dictionary_type;
    if (dictionary_type->isNullable())
        inner_type = static_cast<const DataTypeNullable &>(*dictionary_type).getNestedType();

    if (!inner_type->isStringOrFixedString()
        && !inner_type->isDateOrDateTime()
        && !inner_type->isNumber())
        throw Exception("DataTypeWithDictionary is supported only for numbers, strings, Date or DateTime, but got "
                        + dictionary_type->getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
}

void DataTypeWithDictionary::enumerateStreams(const StreamCallback & callback, SubstreamPath & path) const
{
    path.push_back(Substream::DictionaryKeys);
    dictionary_type->enumerateStreams(callback, path);
    path.back() = Substream::DictionaryIndexes;
    indexes_type->enumerateStreams(callback, path);
    path.pop_back();
}

struct  KeysSerializationVersion
{
    /// Write keys as full column. No indexes is written. Structure:
    ///   <name>.dict.bin : [version - 32 bits][keys]
    ///   <name>.dict.mrk : [marks for keys]
    // FullColumn = 0,
    /// Write all keys in serializePostfix and read in deserializePrefix.
    ///   <name>.dict.bin : [version - 32 bits][indexes type - 32 bits][keys]
    ///   <name>.bin : [indexes]
    ///   <name>.mrk : [marks for indexes]
    // SingleDictionary,
    /// Write distinct set of keys for each granule. Structure:
    ///   <name>.dict.bin : [version - 32 bits][indexes type - 32 bits][keys]
    ///   <name>.dict.mrk : [marks for keys]
    ///   <name>.bin : [indexes]
    ///   <name>.mrk : [marks for indexes]
    // DictionaryPerGranule,

    enum Value
    {
        SingleDictionaryWithAdditionalKeysPerBlock = 1,
    };

    Value value;

    static void checkVersion(UInt64 version)
    {
        if (version != SingleDictionaryWithAdditionalKeysPerBlock)
            throw Exception("Invalid version for DataTypeWithDictionary key column.", ErrorCodes::LOGICAL_ERROR);
    }

    KeysSerializationVersion(UInt64 version) : value(static_cast<Value>(version)) { checkVersion(version); }
};

struct IndexesSerializationType
{
    using SerializationType = UInt64;
    static constexpr UInt64 NeedGlobalDictionaryBit = 1u << 8u;
    static constexpr UInt64 HasAdditionalKeysBit = 1u << 9u;

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

    static constexpr SerializationType resetFlags(SerializationType type)
    {
        return type & (~(HasAdditionalKeysBit | NeedGlobalDictionaryBit));
    }

    static void checkType(SerializationType type)
    {
        UInt64 value = resetFlags(type);
        if (value <= TUInt64)
            return;

        throw Exception("Invalid type for DataTypeWithDictionary index column.", ErrorCodes::LOGICAL_ERROR);
    }

    void serialize(WriteBuffer & buffer) const
    {
        SerializationType val = type;
        if (has_additional_keys)
            val |= HasAdditionalKeysBit;
        if (need_global_dictionary)
            val |= NeedGlobalDictionaryBit;
        writeIntBinary(val, buffer);
    }

    void deserialize(ReadBuffer & buffer)
    {
        SerializationType val;
        readIntBinary(val, buffer);
        checkType(val);
        has_additional_keys = (val & HasAdditionalKeysBit) != 0;
        need_global_dictionary = (val & NeedGlobalDictionaryBit) != 0;
        type = static_cast<Type>(resetFlags(val));
    }

    IndexesSerializationType(const IDataType & data_type, bool has_additional_keys, bool need_global_dictionary)
        : has_additional_keys(has_additional_keys), need_global_dictionary(need_global_dictionary)
    {
        if (typeid_cast<const DataTypeUInt8 *>(&data_type))
            type = TUInt8;
        else if (typeid_cast<const DataTypeUInt16 *>(&data_type))
            type = TUInt16;
        else if (typeid_cast<const DataTypeUInt32 *>(&data_type))
            type = TUInt32;
        else if (typeid_cast<const DataTypeUInt64 *>(&data_type))
            type = TUInt64;
        else
            throw Exception("Invalid DataType for IndexesSerializationType. Expected UInt*, got " + data_type.getName(),
                            ErrorCodes::LOGICAL_ERROR);
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

struct SerializeStateWithDictionary : public IDataType::SerializeBinaryBulkState
{
    KeysSerializationVersion key_version;
    MutableColumnUniquePtr global_dictionary;

    explicit SerializeStateWithDictionary(
        UInt64 key_version,
        MutableColumnUniquePtr && column_unique)
        : key_version(key_version)
        , global_dictionary(std::move(column_unique)) {}
};

struct DeserializeStateWithDictionary : public IDataType::DeserializeBinaryBulkState
{
    KeysSerializationVersion key_version;
    ColumnUniquePtr global_dictionary;
    UInt64 num_bytes_in_dictionary;

    IndexesSerializationType index_type;
    MutableColumnPtr additional_keys;
    UInt64 num_pending_rows = 0;

    explicit DeserializeStateWithDictionary(UInt64 key_version) : key_version(key_version) {}
};

static SerializeStateWithDictionary * checkAndGetWithDictionarySerializeState(
    IDataType::SerializeBinaryBulkStatePtr & state)
{
    if (!state)
        throw Exception("Got empty state for DataTypeWithDictionary.", ErrorCodes::LOGICAL_ERROR);

    auto * with_dictionary_state = typeid_cast<SerializeStateWithDictionary *>(state.get());
    if (!with_dictionary_state)
        throw Exception("Invalid SerializeBinaryBulkState for DataTypeWithDictionary. Expected: "
                        + demangle(typeid(SerializeStateWithDictionary).name()) + ", got "
                        + demangle(typeid(*state).name()), ErrorCodes::LOGICAL_ERROR);

    return with_dictionary_state;
}

static DeserializeStateWithDictionary * checkAndGetWithDictionaryDeserializeState(
    IDataType::DeserializeBinaryBulkStatePtr & state)
{
    if (!state)
        throw Exception("Got empty state for DataTypeWithDictionary.", ErrorCodes::LOGICAL_ERROR);

    auto * with_dictionary_state = typeid_cast<DeserializeStateWithDictionary *>(state.get());
    if (!with_dictionary_state)
        throw Exception("Invalid DeserializeBinaryBulkState for DataTypeWithDictionary. Expected: "
                        + demangle(typeid(DeserializeStateWithDictionary).name()) + ", got "
                        + demangle(typeid(*state).name()), ErrorCodes::LOGICAL_ERROR);

    return with_dictionary_state;
}

void DataTypeWithDictionary::serializeBinaryBulkStatePrefix(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    settings.path.push_back(Substream::DictionaryKeys);
    auto * stream = settings.getter(settings.path);
    settings.path.pop_back();

    if (!stream)
        throw Exception("Got empty stream in DataTypeWithDictionary::serializeBinaryBulkStatePrefix",
                        ErrorCodes::LOGICAL_ERROR);

    /// Write version and create SerializeBinaryBulkState.
    UInt64 key_version = KeysSerializationVersion::SingleDictionaryWithAdditionalKeysPerBlock;

    writeIntBinary(key_version, *stream);

    auto column_unique = createColumnUnique(*dictionary_type, *indexes_type);
    state = std::make_shared<SerializeStateWithDictionary>(key_version, std::move(column_unique));
}

void DataTypeWithDictionary::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    auto * state_with_dictionary = checkAndGetWithDictionarySerializeState(state);
    KeysSerializationVersion::checkVersion(state_with_dictionary->key_version.value);

    if (state_with_dictionary->global_dictionary)
    {
        auto unique_state = state_with_dictionary->global_dictionary->getSerializableState();
        UInt64 num_keys = unique_state.limit;
        if (settings.max_dictionary_size)
        {
            settings.path.push_back(Substream::DictionaryKeys);
            auto * stream = settings.getter(settings.path);
            settings.path.pop_back();

            if (!stream)
                throw Exception("Got empty stream in DataTypeWithDictionary::serializeBinaryBulkStateSuffix",
                                ErrorCodes::LOGICAL_ERROR);

            writeIntBinary(num_keys, *stream);
            removeNullable(dictionary_type)->serializeBinaryBulk(*unique_state.column, *stream,
                                                                 unique_state.offset, unique_state.limit);
        }
    }
}

void DataTypeWithDictionary::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state) const
{
    settings.path.push_back(Substream::DictionaryKeys);
    auto * stream = settings.getter(settings.path);
    settings.path.pop_back();

    if (!stream)
        throw Exception("Got empty stream in DataTypeWithDictionary::deserializeBinaryBulkStatePrefix",
                        ErrorCodes::LOGICAL_ERROR);

    UInt64 keys_version;
    readIntBinary(keys_version, *stream);

    state = std::make_shared<DeserializeStateWithDictionary>(keys_version);
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

    template <typename T>
    MutableColumnPtr mapUniqueIndexImpl(PaddedPODArray<T> & index)
    {
        HashMap<T, T> hash_map;
        for (auto val : index)
            hash_map.insert({val, hash_map.size()});

        auto res_col = ColumnVector<T>::create();
        auto & data = res_col->getData();

        data.resize(hash_map.size());
        for (auto val : hash_map)
            data[val.second] = val.first;

        for (auto & ind : index)
            ind = hash_map[ind];

        return std::move(res_col);
    }

    /// Returns unique values of column. Write new index to column.
    MutableColumnPtr mapUniqueIndex(IColumn & column)
    {
        if (auto * data_uint8 = getIndexesData<UInt8>(column))
            return mapUniqueIndexImpl(*data_uint8);
        else if (auto * data_uint16 = getIndexesData<UInt16>(column))
            return mapUniqueIndexImpl(*data_uint16);
        else if (auto * data_uint32 = getIndexesData<UInt32>(column))
            return mapUniqueIndexImpl(*data_uint32);
        else if (auto * data_uint64 = getIndexesData<UInt64>(column))
            return mapUniqueIndexImpl(*data_uint64);
        else
            throw Exception("Indexes column for getUniqueIndex must be ColumnUInt, got" + column.getName(),
                            ErrorCodes::LOGICAL_ERROR);
    }

    template <typename T>
    MutableColumnPtr mapIndexWithOverflow(PaddedPODArray<T> & index, size_t max_val)
    {
        HashMap<T, T> hash_map;

        for (auto val : index)
        {
            if (val < max_val)
                hash_map.insert({val, hash_map.size()});
        }

        auto index_map_col = ColumnVector<T>::create();
        auto & index_data = index_map_col->getData();

        index_data.resize(hash_map.size());
        for (auto val : hash_map)
            index_data[val.second] = val.first;

        for (auto & val : index)
            val = val < max_val ? hash_map[val]
                                : val - max_val + hash_map.size();

        return index_map_col;
    }

    MutableColumnPtr mapIndexWithOverflow(IColumn & column, size_t max_size)
    {
        if (auto * data_uint8 = getIndexesData<UInt8>(column))
            return mapIndexWithOverflow(*data_uint8, max_size);
        else if (auto * data_uint16 = getIndexesData<UInt16>(column))
            return mapIndexWithOverflow(*data_uint16, max_size);
        else if (auto * data_uint32 = getIndexesData<UInt32>(column))
            return mapIndexWithOverflow(*data_uint32, max_size);
        else if (auto * data_uint64 = getIndexesData<UInt64>(column))
            return mapIndexWithOverflow(*data_uint64, max_size);
        else
            throw Exception("Indexes column for makeIndexWithOverflow must be ColumnUInt, got" + column.getName(),
                            ErrorCodes::LOGICAL_ERROR);
    }
}

void DataTypeWithDictionary::serializeBinaryBulkWithMultipleStreams(
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
        throw Exception("Got empty stream for DataTypeWithDictionary keys.", ErrorCodes::LOGICAL_ERROR);

    if (!indexes_stream)
        throw Exception("Got empty stream for DataTypeWithDictionary indexes.", ErrorCodes::LOGICAL_ERROR);

    const ColumnWithDictionary & column_with_dictionary = typeid_cast<const ColumnWithDictionary &>(column);

    auto * state_with_dictionary = checkAndGetWithDictionarySerializeState(state);
    auto & global_dictionary = state_with_dictionary->global_dictionary;
    KeysSerializationVersion::checkVersion(state_with_dictionary->key_version.value);

    auto unique_state = global_dictionary->getSerializableState();
    bool was_global_dictionary_written = unique_state.limit >= settings.max_dictionary_size;

    const auto & indexes = column_with_dictionary.getIndexesPtr();
    const auto & keys = column_with_dictionary.getUnique()->getSerializableState().column;

    size_t max_limit = column.size() - offset;
    limit = limit ? std::min(limit, max_limit) : max_limit;

    /// Create pair (used_keys, sub_index) which is the dictionary for [offset, offset + limit) range.
    MutableColumnPtr sub_index = (*indexes->cut(offset, limit)).mutate();
    auto unique_indexes = mapUniqueIndex(*sub_index);
    /// unique_indexes->index(*sub_index) == indexes[offset:offset + limit]
    MutableColumnPtr used_keys = (*keys->index(*unique_indexes, 0)).mutate();

    if (settings.max_dictionary_size)
    {
        /// Insert used_keys into global dictionary and update sub_index.
        auto indexes_with_overflow = global_dictionary->uniqueInsertRangeWithOverflow(*used_keys, 0, used_keys->size(),
                                                                                      settings.max_dictionary_size);
        sub_index = (*indexes_with_overflow.indexes->index(*sub_index, 0)).mutate();
        used_keys = std::move(indexes_with_overflow.overflowed_keys);
    }

    bool need_additional_keys = !used_keys->empty();
    bool need_dictionary = settings.max_dictionary_size != 0;
    bool need_write_dictionary = !was_global_dictionary_written && unique_state.limit >= settings.max_dictionary_size;

    IndexesSerializationType index_version(*indexes_type, need_additional_keys, need_dictionary);
    index_version.serialize(*indexes_stream);

    unique_state = global_dictionary->getSerializableState();

    if (need_write_dictionary)
    {
        /// Write global dictionary if it wasn't written and has too many keys.
        UInt64 num_keys = unique_state.limit;
        writeIntBinary(num_keys, *keys_stream);
        removeNullable(dictionary_type)->serializeBinaryBulk(*unique_state.column, *keys_stream, unique_state.offset, num_keys);
    }

    if (need_additional_keys)
    {
        UInt64 num_keys = used_keys->size();
        writeIntBinary(num_keys, *indexes_stream);
        removeNullable(dictionary_type)->serializeBinaryBulk(*used_keys, *indexes_stream, 0, num_keys);
    }

    UInt64 num_rows = sub_index->size();
    writeIntBinary(num_rows, *indexes_stream);
    indexes_type->serializeBinaryBulk(*sub_index, *indexes_stream, 0, num_rows);
}

void DataTypeWithDictionary::deserializeBinaryBulkWithMultipleStreams(
    IColumn & column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state) const
{
    ColumnWithDictionary & column_with_dictionary = typeid_cast<ColumnWithDictionary &>(column);

    auto * state_with_dictionary = checkAndGetWithDictionaryDeserializeState(state);
    KeysSerializationVersion::checkVersion(state_with_dictionary->key_version.value);

    settings.path.push_back(Substream::DictionaryKeys);
    auto * keys_stream = settings.getter(settings.path);
    settings.path.back() = Substream::DictionaryIndexes;
    auto * indexes_stream = settings.getter(settings.path);
    settings.path.pop_back();

    if (!keys_stream && !indexes_stream)
        return;

    if (!keys_stream)
        throw Exception("Got empty stream for DataTypeWithDictionary keys.", ErrorCodes::LOGICAL_ERROR);

    if (!indexes_stream)
        throw Exception("Got empty stream for DataTypeWithDictionary indexes.", ErrorCodes::LOGICAL_ERROR);

    auto readDictionary = [this, state_with_dictionary, keys_stream, &column_with_dictionary]()
    {
        UInt64 num_keys;
        readIntBinary(num_keys, *keys_stream);

        auto keys_type = removeNullable(dictionary_type);
        auto global_dict_keys = keys_type->createColumn();
        keys_type->deserializeBinaryBulk(*global_dict_keys, *keys_stream, num_keys, 0);

        auto column_unique = createColumnUnique(*dictionary_type, *indexes_type);
        column_unique->uniqueInsertRangeFrom(*global_dict_keys, 0, num_keys);
        state_with_dictionary->global_dictionary = std::move(column_unique);
    };

    auto readAdditionalKeys = [this, state_with_dictionary, indexes_stream]()
    {
        UInt64 num_keys;
        readIntBinary(num_keys, *indexes_stream);
        auto keys_type = removeNullable(dictionary_type);
        state_with_dictionary->additional_keys = keys_type->createColumn();
        keys_type->deserializeBinaryBulk(*state_with_dictionary->additional_keys, *indexes_stream, num_keys, 0);
    };

    auto readIndexes = [this, state_with_dictionary, indexes_stream, &column_with_dictionary](UInt64 num_rows,
                                                                                              bool need_dictionary)
    {
        MutableColumnPtr indexes_column = indexes_type->createColumn();
        indexes_type->deserializeBinaryBulk(*indexes_column, *indexes_stream, num_rows, 0);

        auto & global_dictionary = state_with_dictionary->global_dictionary;
        const auto & additional_keys = state_with_dictionary->additional_keys;
        auto * column_unique = column_with_dictionary.getUnique();

        bool has_additional_keys = state_with_dictionary->additional_keys != nullptr;
        bool column_is_empty = column_with_dictionary.empty();
        bool column_with_global_dictionary = column_unique == global_dictionary.get();

        if (!has_additional_keys && (column_is_empty || column_with_global_dictionary))
        {
            if (column_is_empty)
                column_with_dictionary.setUnique(global_dictionary);

            column_with_dictionary.getIndexes()->insertRangeFrom(*indexes_column, 0, num_rows);
        }
        else if (!need_dictionary)
        {
            auto indexes = column_unique->uniqueInsertRangeFrom(*additional_keys, 0, additional_keys->size());
            column_with_dictionary.getIndexes()->insertRangeFrom(*indexes->index(*indexes_column, 0), 0, num_rows);
        }
        else
        {
            auto index_map = mapIndexWithOverflow(*indexes_column, global_dictionary->size());
            auto used_keys = global_dictionary->getNestedColumn()->index(*index_map, 0);
            auto indexes = column_unique->uniqueInsertRangeFrom(*used_keys, 0, used_keys->size());

            if (additional_keys)
            {
                size_t num_keys = additional_keys->size();
                auto additional_indexes = column_unique->uniqueInsertRangeFrom(*additional_keys, 0, num_keys);
                indexes->insertRangeFrom(*additional_indexes, 0, num_keys);
            }

            column_with_dictionary.getIndexes()->insertRangeFrom(*indexes->index(*indexes_column, 0), 0, num_rows);
        }
    };

    while (limit)
    {
        if (state_with_dictionary->num_pending_rows == 0)
        {
            if (indexes_stream->eof())
                break;

            state_with_dictionary->index_type.deserialize(*indexes_stream);

            if (state_with_dictionary->index_type.need_global_dictionary && !state_with_dictionary->global_dictionary)
                readDictionary();

            if (state_with_dictionary->index_type.has_additional_keys)
                readAdditionalKeys();
            else
                state_with_dictionary->additional_keys = nullptr;

            readIntBinary(state_with_dictionary->num_pending_rows, *indexes_stream);
        }

        size_t num_rows_to_read = std::min(limit, state_with_dictionary->num_pending_rows);
        readIndexes(num_rows_to_read, state_with_dictionary->index_type.need_global_dictionary);
        limit -= num_rows_to_read;
        state_with_dictionary->num_pending_rows -= num_rows_to_read;
    }
}

void DataTypeWithDictionary::serializeBinary(const Field & field, WriteBuffer & ostr) const
{
    dictionary_type->serializeBinary(field, ostr);
}
void DataTypeWithDictionary::deserializeBinary(Field & field, ReadBuffer & istr) const
{
    dictionary_type->deserializeBinary(field, istr);
}

template <typename ... Args>
void DataTypeWithDictionary::serializeImpl(
        const IColumn & column, size_t row_num, WriteBuffer & ostr,
        DataTypeWithDictionary::SerealizeFunctionPtr<Args ...> func, Args & ... args) const
{
    auto & column_with_dictionary = getColumnWithDictionary(column);
    size_t unique_row_number = column_with_dictionary.getIndexes()->getUInt(row_num);
    (dictionary_type.get()->*func)(*column_with_dictionary.getUnique()->getNestedColumn(), unique_row_number, ostr, std::forward<Args>(args)...);
}

template <typename ... Args>
void DataTypeWithDictionary::deserializeImpl(
        IColumn & column, ReadBuffer & istr,
        DataTypeWithDictionary::DeserealizeFunctionPtr<Args ...> func, Args ... args) const
{
    auto & column_with_dictionary = getColumnWithDictionary(column);
    auto temp_column = column_with_dictionary.getUnique()->cloneEmpty();

    (dictionary_type.get()->*func)(*temp_column, istr, std::forward<Args>(args)...);

    column_with_dictionary.insertFromFullColumn(*temp_column, 0);
}

template <typename ColumnType, typename IndexType>
MutableColumnUniquePtr DataTypeWithDictionary::createColumnUniqueImpl(const IDataType & keys_type)
{
    return ColumnUnique<ColumnType, IndexType>::create(keys_type);
}

template <typename ColumnType>
MutableColumnUniquePtr DataTypeWithDictionary::createColumnUniqueImpl(const IDataType & keys_type,
                                                                      const IDataType & indexes_type)
{
    if (typeid_cast<const DataTypeUInt8 *>(&indexes_type))
        return createColumnUniqueImpl<ColumnType, UInt8>(keys_type);
    if (typeid_cast<const DataTypeUInt16 *>(&indexes_type))
        return createColumnUniqueImpl<ColumnType, UInt16>(keys_type);
    if (typeid_cast<const DataTypeUInt32 *>(&indexes_type))
        return createColumnUniqueImpl<ColumnType, UInt32>(keys_type);
    if (typeid_cast<const DataTypeUInt64 *>(&indexes_type))
        return createColumnUniqueImpl<ColumnType, UInt64>(keys_type);

    throw Exception("The type of indexes must be unsigned integer, but got " + indexes_type.getName(),
                    ErrorCodes::LOGICAL_ERROR);
}

struct CreateColumnVector
{
    MutableColumnUniquePtr & column;
    const IDataType & keys_type;
    const IDataType & indexes_type;
    const IDataType * nested_type;

    CreateColumnVector(MutableColumnUniquePtr & column, const IDataType & keys_type, const IDataType & indexes_type)
            : column(column), keys_type(keys_type), indexes_type(indexes_type), nested_type(&keys_type)
    {
        if (auto nullable_type = typeid_cast<const DataTypeNullable *>(&keys_type))
            nested_type = nullable_type->getNestedType().get();
    }

    template <typename T, size_t>
    void operator()()
    {
        if (typeid_cast<const DataTypeNumber<T> *>(nested_type))
            column = DataTypeWithDictionary::createColumnUniqueImpl<ColumnVector<T>>(keys_type, indexes_type);
    }
};

MutableColumnUniquePtr DataTypeWithDictionary::createColumnUnique(const IDataType & keys_type,
                                                                  const IDataType & indexes_type)
{
    auto * type = &keys_type;
    if (type->isNullable())
        type = static_cast<const DataTypeNullable &>(keys_type).getNestedType().get();

    if (type->isString())
        return createColumnUniqueImpl<ColumnString>(keys_type, indexes_type);
    if (type->isFixedString())
        return createColumnUniqueImpl<ColumnFixedString>(keys_type, indexes_type);
    if (typeid_cast<const DataTypeDate *>(type))
        return createColumnUniqueImpl<ColumnVector<UInt16>>(keys_type, indexes_type);
    if (typeid_cast<const DataTypeDateTime *>(type))
        return createColumnUniqueImpl<ColumnVector<UInt32>>(keys_type, indexes_type);
    if (type->isNumber())
    {
        MutableColumnUniquePtr column;
        TypeListNumbers::forEach(CreateColumnVector(column, keys_type, indexes_type));

        if (!column)
            throw Exception("Unexpected numeric type: " + type->getName(), ErrorCodes::LOGICAL_ERROR);

        return column;
    }

    throw Exception("Unexpected dictionary type for DataTypeWithDictionary: " + type->getName(),
                    ErrorCodes::LOGICAL_ERROR);
}

MutableColumnPtr DataTypeWithDictionary::createColumn() const
{
    MutableColumnPtr indexes = indexes_type->createColumn();
    MutableColumnPtr dictionary = createColumnUnique(*dictionary_type, *indexes_type);
    return ColumnWithDictionary::create(std::move(dictionary), std::move(indexes));
}

bool DataTypeWithDictionary::equals(const IDataType & rhs) const
{
    if (typeid(rhs) != typeid(*this))
        return false;

    auto & rhs_with_dictionary = static_cast<const DataTypeWithDictionary &>(rhs);
    return dictionary_type->equals(*rhs_with_dictionary.dictionary_type)
           && indexes_type->equals(*rhs_with_dictionary.indexes_type);
}



static DataTypePtr create(const ASTPtr & arguments)
{
    if (!arguments || arguments->children.size() != 2)
        throw Exception("WithDictionary data type family must have two arguments - type of elements and type of indices"
                        , ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    return std::make_shared<DataTypeWithDictionary>(DataTypeFactory::instance().get(arguments->children[0]),
                                                    DataTypeFactory::instance().get(arguments->children[1]));
}

void registerDataTypeWithDictionary(DataTypeFactory & factory)
{
    factory.registerDataType("WithDictionary", create);
}

}
