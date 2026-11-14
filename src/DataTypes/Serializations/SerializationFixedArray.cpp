#include <DataTypes/Serializations/SerializationFixedArray.h>
#include <DataTypes/Serializations/SerializationArray.h>
#include <DataTypes/DataTypeFixedArray.h>
#include <Columns/ColumnFixedArray.h>
#include <Common/SipHash.h>
#include <Core/Field.h>
#include <Common/assert_cast.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_ALL_DATA;
    extern const int SIZES_OF_ARRAYS_DONT_MATCH;
}

SerializationFixedArray::SerializationFixedArray(const SerializationPtr & nested_, size_t n_)
    : nested(nested_)
    , array_serialization(std::static_pointer_cast<const SerializationArray>(SerializationArray::create(nested_)))
    , n(n_)
    , nested_supports_pooling(nested_->supportsPooling())
{
}

UInt128 SerializationFixedArray::getHash(const SerializationPtr & nested_, size_t n_)
{
    SipHash hash;
    hash.update("FixedArray");
    hash.update(n_);
    hash.update(nested_->getHash());
    return hash.get128();
}

SerializationPtr SerializationFixedArray::create(const SerializationPtr & nested_, size_t n_)
{
    if (!nested_->supportsPooling())
        return std::shared_ptr<ISerialization>(new SerializationFixedArray(nested_, n_));
    return ISerialization::pooled(getHash(nested_, n_), [&] { return new SerializationFixedArray(nested_, n_); });
}


void SerializationFixedArray::serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const Array & array = field.safeGet<Array>();
    if (array.size() != n)
        throw Exception(ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH, "Cannot serialize array of size {} as FixedArray({}, ...)", array.size(), n);

    for (const auto & element : array)
        nested->serializeBinary(element, ostr, settings);
}

void SerializationFixedArray::deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings & settings) const
{
    field = Array();
    Array & array = field.safeGet<Array>();
    array.reserve(n);
    for (size_t i = 0; i < n; ++i)
        nested->deserializeBinary(array.emplace_back(), istr, settings);
}

void SerializationFixedArray::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const IColumn & nested_column = assert_cast<const ColumnFixedArray &>(column).getData();
    for (size_t i = row_num * n, end = (row_num + 1) * n; i < end; ++i)
        nested->serializeBinary(nested_column, i, ostr, settings);
}

void SerializationFixedArray::deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    IColumn & nested_column = assert_cast<ColumnFixedArray &>(column).getData();

    size_t i = 0;
    try
    {
        for (; i < n; ++i)
            nested->deserializeBinary(nested_column, istr, settings);
    }
    catch (...)
    {
        if (i)
            nested_column.popBack(i);
        throw;
    }
}

void SerializationFixedArray::serializeForHashCalculation(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    const IColumn & nested_column = assert_cast<const ColumnFixedArray &>(column).getData();
    for (size_t i = row_num * n, end = (row_num + 1) * n; i < end; ++i)
        nested->serializeForHashCalculation(nested_column, i, ostr);
}


void SerializationFixedArray::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    array_serialization->serializeText(column, row_num, ostr, settings);
}

void SerializationFixedArray::deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, bool whole) const
{
    array_serialization->deserializeText(column, istr, settings, whole);
}

bool SerializationFixedArray::tryDeserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, bool whole) const
{
    return array_serialization->tryDeserializeText(column, istr, settings, whole);
}

void SerializationFixedArray::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    array_serialization->serializeTextJSON(column, row_num, ostr, settings);
}

void SerializationFixedArray::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    array_serialization->deserializeTextJSON(column, istr, settings);
}

bool SerializationFixedArray::tryDeserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    return array_serialization->tryDeserializeTextJSON(column, istr, settings);
}

void SerializationFixedArray::serializeTextJSONPretty(
    const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings, size_t indent) const
{
    array_serialization->serializeTextJSONPretty(column, row_num, ostr, settings, indent);
}

void SerializationFixedArray::serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    array_serialization->serializeTextXML(column, row_num, ostr, settings);
}

void SerializationFixedArray::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    array_serialization->serializeTextCSV(column, row_num, ostr, settings);
}

void SerializationFixedArray::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    array_serialization->deserializeTextCSV(column, istr, settings);
}

bool SerializationFixedArray::tryDeserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    return array_serialization->tryDeserializeTextCSV(column, istr, settings);
}

void SerializationFixedArray::serializeTextHive(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    array_serialization->serializeTextHive(column, row_num, ostr, settings);
}


DataTypePtr SerializationFixedArray::SubcolumnCreator::create(const DataTypePtr & prev) const
{
    return std::make_shared<DataTypeFixedArray>(n, prev);
}

SerializationPtr SerializationFixedArray::SubcolumnCreator::create(const SerializationPtr & prev, const DataTypePtr &) const
{
    return SerializationFixedArray::create(prev, n);
}

ColumnPtr SerializationFixedArray::SubcolumnCreator::create(const ColumnPtr & prev) const
{
    return ColumnFixedArray::create(prev, n);
}

void SerializationFixedArray::enumerateStreams(
    EnumerateStreamsSettings & settings,
    const StreamCallback & callback,
    const SubstreamData & data) const
{
    const auto * type_fixed_array = data.type ? &assert_cast<const DataTypeFixedArray &>(*data.type) : nullptr;
    const auto * column_fixed_array = data.column ? &assert_cast<const ColumnFixedArray &>(*data.column) : nullptr;

    /// There is no stream of sizes, only the elements.
    settings.path.push_back(Substream::ArrayElements);
    settings.path.back().data = data;
    settings.path.back().creator = std::make_shared<SubcolumnCreator>(n);
    ++settings.array_level;

    auto next_data = SubstreamData(nested)
        .withType(type_fixed_array ? type_fixed_array->getNestedType() : nullptr)
        .withColumn(column_fixed_array ? column_fixed_array->getDataPtr() : nullptr)
        .withSerializationInfo(data.serialization_info)
        .withDeserializeState(data.deserialize_state);

    nested->enumerateStreams(settings, callback, next_data);
    --settings.array_level;
    settings.path.pop_back();
}

void SerializationFixedArray::serializeBinaryBulkStatePrefix(
    const IColumn & column,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    settings.path.push_back(Substream::ArrayElements);
    nested->serializeBinaryBulkStatePrefix(assert_cast<const ColumnFixedArray &>(column).getData(), settings, state);
    settings.path.pop_back();
}

void SerializationFixedArray::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    settings.path.push_back(Substream::ArrayElements);
    nested->serializeBinaryBulkStateSuffix(settings, state);
    settings.path.pop_back();
}

void SerializationFixedArray::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsDeserializeStatesCache * cache) const
{
    settings.path.push_back(Substream::ArrayElements);
    nested->deserializeBinaryBulkStatePrefix(settings, state, cache);
    settings.path.pop_back();
}

void SerializationFixedArray::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column,
    size_t offset,
    size_t limit,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    const auto & column_fixed_array = assert_cast<const ColumnFixedArray &>(column);
    size_t rows = column_fixed_array.size();
    offset = std::min(offset, rows);
    if (limit == 0 || offset + limit > rows)
        limit = rows - offset;

    settings.path.push_back(Substream::ArrayElements);
    /// Called even if there is nothing to write, see the comment in `SerializationArray`.
    nested->serializeBinaryBulkWithMultipleStreams(column_fixed_array.getData(), offset * n, limit * n, settings, state);
    settings.path.pop_back();
}

void SerializationFixedArray::deserializeBinaryBulkWithMultipleStreams(
    IColumn & column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    auto & column_fixed_array = assert_cast<ColumnFixedArray &>(column);
    IColumn & nested_column = column_fixed_array.getData();

    settings.path.push_back(Substream::ArrayElements);
    nested->deserializeBinaryBulkWithMultipleStreams(nested_column, limit * n, settings, state, cache);
    settings.path.pop_back();

    if (nested_column.size() % n != 0)
        throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA,
            "Cannot read all elements of FixedArray({}, ...): the number of read elements {} is not a multiple of the array size",
            n, nested_column.size());
}

}
