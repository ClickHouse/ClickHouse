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

void DataTypeWithDictionary::enumerateStreams(StreamCallback callback, SubstreamPath path) const
{
    path.push_back(Substream::DictionaryKeys);
    dictionary_type->enumerateStreams(callback, path);
    path.back() = Substream::DictionaryIndexes;
    indexes_type->enumerateStreams(callback, path);
}

void DataTypeWithDictionary::serializeBinaryBulkWithMultipleStreams(
        const IColumn & column,
        OutputStreamGetter getter,
        size_t offset,
        size_t limit,
        bool position_independent_encoding,
        SubstreamPath path) const
{
    const ColumnWithDictionary & column_with_dictionary = typeid_cast<const ColumnWithDictionary &>(column);

    size_t max_limit = column.size() - offset;
    limit = limit ? std::min(limit, max_limit) : max_limit;

    path.push_back(Substream::DictionaryIndexes);
    if (auto stream = getter(path))
    {
        const auto & indexes = column_with_dictionary.getIndexesPtr();
        const auto & keys = column_with_dictionary.getUnique()->getNestedColumn();
        MutableColumnPtr sub_index = (*indexes->cut(offset, limit)).mutate();
        ColumnPtr unique_indexes = makeSubIndex(*sub_index);
        /// unique_indexes->index(sub_index) == indexes[offset:offset + limit]
        auto used_keys = keys->index(unique_indexes, 0);
        /// (used_keys, sub_index) is ColumnWithDictionary for range [offset:offset + limit]

        UInt64 used_keys_size = used_keys->size();
        writeIntBinary(used_keys_size, *stream);

        UInt64 indexes_size = sub_index->size();
        writeIntBinary(indexes_size, *stream);

        path.back() = Substream::DictionaryKeys;
        dictionary_type->serializeBinaryBulkWithMultipleStreams(*used_keys, getter, 0, 0,
                                                                position_independent_encoding, path);

        indexes_type->serializeBinaryBulk(*sub_index, *stream, 0, limit);
    }
}

struct DeserializeBinaryBulkStateWithDictionary : public IDataType::DeserializeBinaryBulkState
{
    UInt64 num_rows_to_read_until_next_index = 0;
    ColumnPtr index;
    IDataType::DeserializeBinaryBulkStatePtr state;

    explicit DeserializeBinaryBulkStateWithDictionary(IDataType::DeserializeBinaryBulkStatePtr && state)
            : state(std::move(state)) {}
};

IDataType::DeserializeBinaryBulkStatePtr DataTypeWithDictionary::createDeserializeBinaryBulkState() const
{
    return std::make_shared<DeserializeBinaryBulkStateWithDictionary>(
            dictionary_type->createDeserializeBinaryBulkState());
}

void DataTypeWithDictionary::deserializeBinaryBulkWithMultipleStreams(
        IColumn & column,
        InputStreamGetter getter,
        size_t limit,
        double /*avg_value_size_hint*/,
        bool position_independent_encoding,
        SubstreamPath path,
        const DeserializeBinaryBulkStatePtr & state) const
{
    ColumnWithDictionary & column_with_dictionary = typeid_cast<ColumnWithDictionary &>(column);

    auto dict_state = typeid_cast<DeserializeBinaryBulkStateWithDictionary *>(state.get());
    if (dict_state == nullptr)
        throw Exception("Invalid DeserializeBinaryBulkState.", ErrorCodes::LOGICAL_ERROR);

    auto readIndexes = [&](ReadBuffer * stream, const ColumnPtr & index, size_t num_rows)
    {
        auto index_col = indexes_type->createColumn();
        indexes_type->deserializeBinaryBulk(*index_col, *stream, num_rows, 0);
        column_with_dictionary.getIndexes()->insertRangeFrom(*index->index(std::move(index_col), 0), 0, num_rows);
    };

    using CachedStreams = std::unordered_map<std::string, ReadBuffer *>;
    CachedStreams cached_streams;

    IDataType::InputStreamGetter cached_stream_getter = [&] (const IDataType::SubstreamPath & path) -> ReadBuffer *
    {
        std::string stream_name = IDataType::getFileNameForStream("", path);
        auto iter = cached_streams.find(stream_name);
        if (iter == cached_streams.end())
            iter = cached_streams.insert({stream_name, getter(path)}).first;
        return iter->second;
    };

    auto readDict = [&](UInt64 num_keys)
    {
        auto dict_column = dictionary_type->createColumn();
        dictionary_type->deserializeBinaryBulkWithMultipleStreams(*dict_column, cached_stream_getter, num_keys, 0,
                                                                  position_independent_encoding, path, dict_state->state);
        return column_with_dictionary.getUnique()->uniqueInsertRangeFrom(*dict_column, 0, num_keys);
    };

    path.push_back(Substream::DictionaryIndexes);

    if (auto stream = getter(path))
    {
        path.back() = Substream::DictionaryKeys;

        while (limit)
        {
            if (dict_state->num_rows_to_read_until_next_index == 0)
            {
                if (stream->eof())
                    break;

                UInt64 num_keys;
                readIntBinary(num_keys, *stream);
                readIntBinary(dict_state->num_rows_to_read_until_next_index, *stream);
                dict_state->index = readDict(num_keys);
            }

            size_t num_rows_to_read = std::min(limit, dict_state->num_rows_to_read_until_next_index);
            readIndexes(stream, dict_state->index, num_rows_to_read);
            limit -= num_rows_to_read;
            dict_state->num_rows_to_read_until_next_index -= num_rows_to_read;
        }
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
MutableColumnPtr DataTypeWithDictionary::createColumnImpl() const
{
    return ColumnWithDictionary::create(ColumnUnique<ColumnType, IndexType>::create(dictionary_type),
                                        indexes_type->createColumn());
}

template <typename ColumnType>
MutableColumnPtr DataTypeWithDictionary::createColumnImpl() const
{
    if (typeid_cast<const DataTypeUInt8 *>(indexes_type.get()))
        return createColumnImpl<ColumnType, UInt8>();
    if (typeid_cast<const DataTypeUInt16 *>(indexes_type.get()))
        return createColumnImpl<ColumnType, UInt16>();
    if (typeid_cast<const DataTypeUInt32 *>(indexes_type.get()))
        return createColumnImpl<ColumnType, UInt32>();
    if (typeid_cast<const DataTypeUInt64 *>(indexes_type.get()))
        return createColumnImpl<ColumnType, UInt64>();

    throw Exception("The type of indexes must be unsigned integer, but got " + dictionary_type->getName(),
                    ErrorCodes::LOGICAL_ERROR);
}

struct CreateColumnVector
{
    MutableColumnPtr & column;
    const DataTypeWithDictionary * data_type_with_dictionary;
    const IDataType * type;

    CreateColumnVector(MutableColumnPtr & column, const DataTypeWithDictionary * data_type_with_dictionary,
                       const IDataType * type)
            : column(column), data_type_with_dictionary(data_type_with_dictionary), type(type) {}

    template <typename T, size_t>
    void operator()()
    {
        if (typeid_cast<const DataTypeNumber<T> *>(type))
            column = data_type_with_dictionary->createColumnImpl<ColumnVector<T>>();
    }
};

MutableColumnPtr DataTypeWithDictionary::createColumn() const
{
    auto type = dictionary_type;
    if (type->isNullable())
        type = static_cast<const DataTypeNullable &>(*dictionary_type).getNestedType();

    if (type->isString())
        return createColumnImpl<ColumnString>();
    if (type->isFixedString())
        return createColumnImpl<ColumnFixedString>();
    if (typeid_cast<const DataTypeDate *>(type.get()))
        return createColumnImpl<ColumnVector<UInt16>>();
    if (typeid_cast<const DataTypeDateTime *>(type.get()))
        return createColumnImpl<ColumnVector<UInt32>>();
    if (type->isNumber())
    {
        MutableColumnPtr column;
        TypeListNumbers::forEach(CreateColumnVector(column, this, type.get()));

        if (!column)
            throw Exception("Unexpected numeric type: " + type->getName(), ErrorCodes::LOGICAL_ERROR);

        return column;
    }

    throw Exception("Unexpected dictionary type for DataTypeWithDictionary: " + type->getName(),
                    ErrorCodes::LOGICAL_ERROR);
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
