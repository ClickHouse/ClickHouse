#include <DataTypes/Serializations/SerializationArray.h>
#include <DataTypes/Serializations/SerializationNullable.h>
#include <DataTypes/Serializations/SerializationNumber.h>
#include <DataTypes/Serializations/SerializationNamed.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnArray.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

#include <Formats/FormatSettings.h>
#include <Formats/JSONUtils.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_ALL_DATA;
    extern const int CANNOT_READ_ARRAY_FROM_TEXT;
    extern const int LOGICAL_ERROR;
    extern const int TOO_LARGE_ARRAY_SIZE;
}

static constexpr size_t MAX_ARRAY_SIZE = 1ULL << 30;
static constexpr size_t MAX_ARRAYS_SIZE = 1ULL << 40;


void SerializationArray::serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const Array & a = field.safeGet<const Array &>();
    writeVarUInt(a.size(), ostr);
    for (const auto & i : a)
    {
        nested->serializeBinary(i, ostr, settings);
    }
}


void SerializationArray::deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings & settings) const
{
    size_t size;
    readVarUInt(size, istr);
    if (settings.binary.max_binary_string_size && size > settings.binary.max_binary_string_size)
        throw Exception(
            ErrorCodes::TOO_LARGE_ARRAY_SIZE,
            "Too large array size: {}. The maximum is: {}. To increase the maximum, use setting "
            "format_binary_max_array_size",
            size,
            settings.binary.max_binary_string_size);

    field = Array();
    Array & arr = field.safeGet<Array &>();
    arr.reserve(size);
    for (size_t i = 0; i < size; ++i)
        nested->deserializeBinary(arr.emplace_back(), istr, settings);
}


void SerializationArray::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const ColumnArray & column_array = assert_cast<const ColumnArray &>(column);
    const ColumnArray::Offsets & offsets = column_array.getOffsets();

    size_t offset = offsets[row_num - 1];
    size_t next_offset = offsets[row_num];
    size_t size = next_offset - offset;

    writeVarUInt(size, ostr);

    const IColumn & nested_column = column_array.getData();
    for (size_t i = offset; i < next_offset; ++i)
        nested->serializeBinary(nested_column, i, ostr, settings);
}


void SerializationArray::deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    ColumnArray & column_array = assert_cast<ColumnArray &>(column);
    ColumnArray::Offsets & offsets = column_array.getOffsets();

    size_t size;
    readVarUInt(size, istr);
    if (settings.binary.max_binary_string_size && size > settings.binary.max_binary_string_size)
        throw Exception(
            ErrorCodes::TOO_LARGE_ARRAY_SIZE,
            "Too large array size: {}. The maximum is: {}. To increase the maximum, use setting "
            "format_binary_max_array_size",
            size,
            settings.binary.max_binary_string_size);

    IColumn & nested_column = column_array.getData();

    size_t i = 0;
    try
    {
        for (; i < size; ++i)
            nested->deserializeBinary(nested_column, istr, settings);
    }
    catch (...)
    {
        if (i)
            nested_column.popBack(i);
        throw;
    }

    offsets.push_back(offsets.back() + size);
}


namespace
{
    void serializeArraySizesPositionIndependent(const IColumn & column, WriteBuffer & ostr, UInt64 offset, UInt64 limit)
    {
        const ColumnArray & column_array = typeid_cast<const ColumnArray &>(column);
        const ColumnArray::Offsets & offset_values = column_array.getOffsets();
        size_t size = offset_values.size();

        if (!size)
            return;

        size_t end = limit && (offset + limit < size)
            ? offset + limit
            : size;

        ColumnArray::Offset prev_offset = offset_values[offset - 1];
        for (size_t i = offset; i < end; ++i)
        {
            ColumnArray::Offset current_offset = offset_values[i];
            writeBinaryLittleEndian(current_offset - prev_offset, ostr);
            prev_offset = current_offset;
        }
    }

    void deserializeArraySizesPositionIndependent(ColumnArray & column_array, ReadBuffer & istr, UInt64 limit)
    {
        ColumnArray::Offsets & offset_values = column_array.getOffsets();
        size_t initial_size = offset_values.size();
        offset_values.resize(initial_size + limit);

        size_t i = initial_size;
        ColumnArray::Offset current_offset = initial_size ? offset_values[initial_size - 1] : 0;
        while (i < initial_size + limit && !istr.eof())
        {
            ColumnArray::Offset current_size = 0;
            readBinaryLittleEndian(current_size, istr);

            if (unlikely(current_size > MAX_ARRAY_SIZE))
                throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Array size is too large: {}", current_size);
            if (unlikely(__builtin_add_overflow(current_offset, current_size, &current_offset)))
                throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Deserialization of array offsets will lead to overflow");

            offset_values[i] = current_offset;
            ++i;
        }

        offset_values.resize(i);
    }

    ColumnPtr arraySizesToOffsets(const IColumn & column)
    {
        const auto & column_sizes = assert_cast<const ColumnArray::ColumnOffsets &>(column);
        MutableColumnPtr column_offsets = column_sizes.cloneEmpty();

        if (column_sizes.empty())
            return column_offsets;

        const auto & sizes_data = column_sizes.getData();
        auto & offsets_data = assert_cast<ColumnArray::ColumnOffsets &>(*column_offsets).getData();

        offsets_data.resize(sizes_data.size());

        IColumn::Offset prev_offset = 0;
        for (size_t i = 0, size = sizes_data.size(); i < size; ++i)
        {
            prev_offset += sizes_data[i];
            offsets_data[i] = prev_offset;
        }

        return column_offsets;
    }

    ColumnPtr arrayOffsetsToSizes(const IColumn & column)
    {
        const auto & column_offsets = assert_cast<const ColumnArray::ColumnOffsets &>(column);
        MutableColumnPtr column_sizes = column_offsets.cloneEmpty();

        if (column_offsets.empty())
            return column_sizes;

        const auto & offsets_data = column_offsets.getData();
        auto & sizes_data = assert_cast<ColumnArray::ColumnOffsets &>(*column_sizes).getData();

        sizes_data.resize(offsets_data.size());

        IColumn::Offset prev_offset = 0;
        for (size_t i = 0, size = offsets_data.size(); i < size; ++i)
        {
            auto current_offset = offsets_data[i];
            sizes_data[i] = current_offset - prev_offset;
            prev_offset = current_offset;
        }

        return column_sizes;
    }
}

DataTypePtr SerializationArray::SubcolumnCreator::create(const DataTypePtr & prev) const
{
    return std::make_shared<DataTypeArray>(prev);
}

SerializationPtr SerializationArray::SubcolumnCreator::create(const SerializationPtr & prev) const
{
    return std::make_shared<SerializationArray>(prev);
}

ColumnPtr SerializationArray::SubcolumnCreator::create(const ColumnPtr & prev) const
{
    return ColumnArray::create(prev, offsets);
}

void SerializationArray::enumerateStreams(
    EnumerateStreamsSettings & settings,
    const StreamCallback & callback,
    const SubstreamData & data) const
{
    const auto * type_array = data.type ? &assert_cast<const DataTypeArray &>(*data.type) : nullptr;
    const auto * column_array = data.column ? &assert_cast<const ColumnArray &>(*data.column) : nullptr;
    auto offsets = column_array ? column_array->getOffsetsPtr() : nullptr;

    auto subcolumn_name = "size" + std::to_string(getArrayLevel(settings.path));
    auto offsets_serialization = std::make_shared<SerializationNamed>(
        std::make_shared<SerializationNumber<UInt64>>(),
        subcolumn_name, SubstreamType::NamedOffsets);

    auto offsets_column = offsets && !settings.position_independent_encoding
        ? arrayOffsetsToSizes(*offsets)
        : offsets;

    settings.path.push_back(Substream::ArraySizes);
    settings.path.back().data = SubstreamData(offsets_serialization)
        .withType(type_array ? std::make_shared<DataTypeUInt64>() : nullptr)
        .withColumn(std::move(offsets_column))
        .withSerializationInfo(data.serialization_info);

    callback(settings.path);

    settings.path.back() = Substream::ArrayElements;
    settings.path.back().data = data;
    settings.path.back().creator = std::make_shared<SubcolumnCreator>(offsets);

    auto next_data = SubstreamData(nested)
        .withType(type_array ? type_array->getNestedType() : nullptr)
        .withColumn(column_array ? column_array->getDataPtr() : nullptr)
        .withSerializationInfo(data.serialization_info)
        .withDeserializeState(data.deserialize_state);

    nested->enumerateStreams(settings, callback, next_data);
    settings.path.pop_back();
}

void SerializationArray::serializeBinaryBulkStatePrefix(
    const IColumn & column,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    settings.path.push_back(Substream::ArrayElements);
    const auto & column_array = assert_cast<const ColumnArray &>(column);
    nested->serializeBinaryBulkStatePrefix(column_array.getData(), settings, state);
    settings.path.pop_back();
}


void SerializationArray::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    settings.path.push_back(Substream::ArrayElements);
    nested->serializeBinaryBulkStateSuffix(settings, state);
    settings.path.pop_back();
}


void SerializationArray::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsDeserializeStatesCache * cache) const
{
    settings.path.push_back(Substream::ArrayElements);
    nested->deserializeBinaryBulkStatePrefix(settings, state, cache);
    settings.path.pop_back();
}


void SerializationArray::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column,
    size_t offset,
    size_t limit,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    const ColumnArray & column_array = typeid_cast<const ColumnArray &>(column);

    /// First serialize array sizes.
    settings.path.push_back(Substream::ArraySizes);
    if (auto * stream = settings.getter(settings.path))
    {
        if (settings.position_independent_encoding)
            serializeArraySizesPositionIndependent(column, *stream, offset, limit);
        else
            SerializationNumber<ColumnArray::Offset>().serializeBinaryBulk(*column_array.getOffsetsPtr(), *stream, offset, limit);
    }

    /// Then serialize contents of arrays.
    settings.path.back() = Substream::ArrayElements;
    const ColumnArray::Offsets & offset_values = column_array.getOffsets();

    if (offset > offset_values.size())
        return;

    /** offset - from which array to write.
      * limit - how many arrays should be written, or 0, if you write everything that is.
      * end - up to which array the recorded piece ends.
      *
      * nested_offset - from which element of the innards to write.
      * nested_limit - how many elements of the innards to write, or 0, if you write everything that is.
      */

    size_t end = std::min(offset + limit, offset_values.size());

    size_t nested_offset = offset ? offset_values[offset - 1] : 0;
    size_t nested_limit = limit
        ? offset_values[end - 1] - nested_offset
        : 0;

    if (limit == 0 || nested_limit)
        nested->serializeBinaryBulkWithMultipleStreams(column_array.getData(), nested_offset, nested_limit, settings, state);
    settings.path.pop_back();
}


void SerializationArray::deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr & column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    auto mutable_column = column->assumeMutable();
    ColumnArray & column_array = typeid_cast<ColumnArray &>(*mutable_column);
    size_t prev_last_offset = column_array.getOffsets().back();

    settings.path.push_back(Substream::ArraySizes);

    if (auto cached_column = getFromSubstreamsCache(cache, settings.path))
    {
        column_array.getOffsetsPtr() = arraySizesToOffsets(*cached_column);
    }
    else if (auto * stream = settings.getter(settings.path))
    {
        if (settings.position_independent_encoding)
            deserializeArraySizesPositionIndependent(column_array, *stream, limit);
        else
            SerializationNumber<ColumnArray::Offset>().deserializeBinaryBulk(column_array.getOffsetsColumn(), *stream, limit, 0);

        addToSubstreamsCache(cache, settings.path, arrayOffsetsToSizes(column_array.getOffsetsColumn()));
    }

    settings.path.back() = Substream::ArrayElements;

    ColumnArray::Offsets & offset_values = column_array.getOffsets();
    ColumnPtr & nested_column = column_array.getDataPtr();

    /// Number of values corresponding with `offset_values` must be read.
    size_t last_offset = offset_values.back();
    if (last_offset < prev_last_offset)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Nested column is longer than last offset");
    size_t nested_limit = last_offset - prev_last_offset;

    if (unlikely(nested_limit > MAX_ARRAYS_SIZE))
        throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Array sizes are too large: {}", nested_limit);

    /// Adjust value size hint. Divide it to the average array size.
    settings.avg_value_size_hint = nested_limit ? settings.avg_value_size_hint / nested_limit * offset_values.size() : 0;

    nested->deserializeBinaryBulkWithMultipleStreams(nested_column, nested_limit, settings, state, cache);

    settings.path.pop_back();

    /// Check consistency between offsets and elements subcolumns.
    /// But if elements column is empty - it's ok for columns of Nested types that was added by ALTER.
    if (!nested_column->empty() && nested_column->size() != last_offset)
        throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Cannot read all array values: read just {} of {}",
            toString(nested_column->size()), toString(last_offset));

    column = std::move(mutable_column);
}


template <typename Writer>
static void serializeTextImpl(const IColumn & column, size_t row_num, WriteBuffer & ostr, Writer && write_nested)
{
    const ColumnArray & column_array = assert_cast<const ColumnArray &>(column);
    const ColumnArray::Offsets & offsets = column_array.getOffsets();

    size_t offset = offsets[row_num - 1];
    size_t next_offset = offsets[row_num];

    const IColumn & nested_column = column_array.getData();

    writeChar('[', ostr);
    for (size_t i = offset; i < next_offset; ++i)
    {
        if (i != offset)
            writeChar(',', ostr);
        write_nested(nested_column, i);
    }
    writeChar(']', ostr);
}


template <typename ReturnType = void, typename Reader>
static ReturnType deserializeTextImpl(IColumn & column, ReadBuffer & istr, Reader && read_nested, bool allow_unenclosed)
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    ColumnArray & column_array = assert_cast<ColumnArray &>(column);
    ColumnArray::Offsets & offsets = column_array.getOffsets();

    IColumn & nested_column = column_array.getData();

    size_t size = 0;

    bool has_braces = false;
    if (checkChar('[', istr))
        has_braces = true;
    else if (!allow_unenclosed)
    {
        if constexpr (throw_exception)
            throw Exception(ErrorCodes::CANNOT_READ_ARRAY_FROM_TEXT, "Array does not start with '[' character");
        return ReturnType(false);
    }

    auto on_error_no_throw = [&]()
    {
        if (size)
            nested_column.popBack(size);
        return ReturnType(false);
    };

    try
    {
        bool first = true;
        while (!istr.eof() && *istr.position() != ']')
        {
            if (!first)
            {
                if (*istr.position() == ',')
                {
                    ++istr.position();
                }
                else
                {
                    if constexpr (throw_exception)
                        throw Exception(ErrorCodes::CANNOT_READ_ARRAY_FROM_TEXT,
                            "Cannot read array from text, expected comma or end of array, found '{}'",
                            *istr.position());
                    return on_error_no_throw();
                }
            }

            first = false;

            skipWhitespaceIfAny(istr);

            if (*istr.position() == ']')
                break;

            if constexpr (throw_exception)
                read_nested(nested_column);
            else if (!read_nested(nested_column))
                return on_error_no_throw();

            ++size;

            skipWhitespaceIfAny(istr);
        }

        if (has_braces)
        {
            if constexpr (throw_exception)
                assertChar(']', istr);
            else if (!checkChar(']', istr))
                return on_error_no_throw();
        }
        else /// If array is not enclosed in braces, we read until EOF.
        {
            if constexpr (throw_exception)
                assertEOF(istr);
            else if (!istr.eof())
                return on_error_no_throw();
        }
    }
    catch (...)
    {
        if (size)
            nested_column.popBack(size);
        if constexpr (throw_exception)
            throw;
        return ReturnType(false);
    }

    offsets.push_back(offsets.back() + size);
    return ReturnType(true);
}


void SerializationArray::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serializeTextImpl(column, row_num, ostr,
        [&](const IColumn & nested_column, size_t i)
        {
            nested->serializeTextQuoted(nested_column, i, ostr, settings);
        });
}


void SerializationArray::deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, bool whole) const
{
    deserializeTextImpl(column, istr,
        [&](IColumn & nested_column)
        {
            if (settings.null_as_default && !isColumnNullableOrLowCardinalityNullable(nested_column))
                SerializationNullable::deserializeNullAsDefaultOrNestedTextQuoted(nested_column, istr, settings, nested);
            else
                nested->deserializeTextQuoted(nested_column, istr, settings);
        }, false);

    if (whole && !istr.eof())
        throwUnexpectedDataAfterParsedValue(column, istr, settings, "Array");
}

bool SerializationArray::tryDeserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, bool whole) const
{
    auto read_nested = [&](IColumn & nested_column)
    {
        if (settings.null_as_default && !isColumnNullableOrLowCardinalityNullable(nested_column))
            return SerializationNullable::tryDeserializeNullAsDefaultOrNestedTextQuoted(nested_column, istr, settings, nested);
        return nested->tryDeserializeTextQuoted(nested_column, istr, settings);
    };

    bool ok = deserializeTextImpl<bool>(column, istr, std::move(read_nested), false);

    if (!ok)
        return false;

    if (whole && !istr.eof())
    {
        column.popBack(1);
        return false;
    }

    return true;
}

void SerializationArray::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const ColumnArray & column_array = assert_cast<const ColumnArray &>(column);
    const ColumnArray::Offsets & offsets = column_array.getOffsets();

    size_t offset = offsets[row_num - 1];
    size_t next_offset = offsets[row_num];

    const IColumn & nested_column = column_array.getData();

    writeChar('[', ostr);
    for (size_t i = offset; i < next_offset; ++i)
    {
        if (i != offset)
            writeChar(',', ostr);
        nested->serializeTextJSON(nested_column, i, ostr, settings);
    }
    writeChar(']', ostr);
}

void SerializationArray::serializeTextJSONPretty(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings, size_t indent) const
{
    const ColumnArray & column_array = assert_cast<const ColumnArray &>(column);
    const ColumnArray::Offsets & offsets = column_array.getOffsets();

    size_t offset = offsets[row_num - 1];
    size_t next_offset = offsets[row_num];

    const IColumn & nested_column = column_array.getData();

    if (offset == next_offset)
    {
        writeCString("[]", ostr);
        return;
    }

    writeCString("[\n", ostr);
    for (size_t i = offset; i < next_offset; ++i)
    {
        if (i != offset)
            writeCString(",\n", ostr);
        writeChar(' ', (indent + 1) * 4, ostr);
        nested->serializeTextJSONPretty(nested_column, i, ostr, settings, indent + 1);
    }
    writeChar('\n', ostr);
    writeChar(' ', indent * 4, ostr);
    writeChar(']', ostr);
}


template <typename ReturnType>
ReturnType SerializationArray::deserializeTextJSONImpl(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    auto deserialize_nested = [&settings, this](IColumn & nested_column, ReadBuffer & buf) -> ReturnType
    {
        if constexpr (std::is_same_v<ReturnType, void>)
        {
            if (settings.null_as_default && !isColumnNullableOrLowCardinalityNullable(nested_column))
                SerializationNullable::deserializeNullAsDefaultOrNestedTextJSON(nested_column, buf, settings, nested);
            else
                nested->deserializeTextJSON(nested_column, buf, settings);
        }
        else
        {
            if (settings.null_as_default && !isColumnNullableOrLowCardinalityNullable(nested_column))
                return SerializationNullable::tryDeserializeNullAsDefaultOrNestedTextJSON(nested_column, buf, settings, nested);
            return nested->tryDeserializeTextJSON(nested_column, buf, settings);
        }
    };

    if (settings.json.empty_as_default)
        return deserializeTextImpl<ReturnType>(column, istr,
            [&deserialize_nested, &istr](IColumn & nested_column) -> ReturnType
            {
                return JSONUtils::deserializeEmpyStringAsDefaultOrNested<ReturnType>(nested_column, istr, deserialize_nested);
            }, false);
    return deserializeTextImpl<ReturnType>(
        column,
        istr,
        [&deserialize_nested, &istr](IColumn & nested_column) -> ReturnType { return deserialize_nested(nested_column, istr); },
        false);
}


void SerializationArray::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserializeTextJSONImpl<void>(column, istr, settings);
}

bool SerializationArray::tryDeserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    return deserializeTextJSONImpl<bool>(column, istr, settings);
}


void SerializationArray::serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const ColumnArray & column_array = assert_cast<const ColumnArray &>(column);
    const ColumnArray::Offsets & offsets = column_array.getOffsets();

    size_t offset = offsets[row_num - 1];
    size_t next_offset = offsets[row_num];

    const IColumn & nested_column = column_array.getData();

    writeCString("<array>", ostr);
    for (size_t i = offset; i < next_offset; ++i)
    {
        writeCString("<elem>", ostr);
        nested->serializeTextXML(nested_column, i, ostr, settings);
        writeCString("</elem>", ostr);
    }
    writeCString("</array>", ostr);
}


void SerializationArray::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    /// There is no good way to serialize an array in CSV. Therefore, we serialize it into a string, and then write the resulting string in CSV.
    WriteBufferFromOwnString wb;
    serializeText(column, row_num, wb, settings);
    writeCSV(wb.str(), ostr);
}


void SerializationArray::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String s;
    readCSV(s, istr, settings.csv);
    ReadBufferFromString rb(s);

    if (settings.csv.arrays_as_nested_csv)
    {
        deserializeTextImpl(column, rb,
            [&](IColumn & nested_column)
            {
                if (settings.null_as_default && !isColumnNullableOrLowCardinalityNullable(nested_column))
                    SerializationNullable::deserializeNullAsDefaultOrNestedTextCSV(nested_column, rb, settings, nested);
                else
                    nested->deserializeTextCSV(nested_column, rb, settings);
            }, true);
    }
    else
    {
        deserializeTextImpl(column, rb,
            [&](IColumn & nested_column)
            {
                if (settings.null_as_default && !isColumnNullableOrLowCardinalityNullable(nested_column))
                    SerializationNullable::deserializeNullAsDefaultOrNestedTextQuoted(nested_column, rb, settings, nested);
                else
                    nested->deserializeTextQuoted(nested_column, rb, settings);
            }, true);
    }
}

bool SerializationArray::tryDeserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String s;
    if (!tryReadCSV(s, istr, settings.csv))
        return false;
    ReadBufferFromString rb(s);

    if (settings.csv.arrays_as_nested_csv)
    {
        auto read_nested = [&](IColumn & nested_column)
        {
            if (settings.null_as_default && !isColumnNullableOrLowCardinalityNullable(nested_column))
                return SerializationNullable::tryDeserializeNullAsDefaultOrNestedTextCSV(nested_column, rb, settings, nested);
            return nested->tryDeserializeTextCSV(nested_column, rb, settings);
        };

        return deserializeTextImpl<bool>(column, rb, read_nested, true);
    }

    auto read_nested = [&](IColumn & nested_column)
    {
        if (settings.null_as_default && !isColumnNullableOrLowCardinalityNullable(nested_column))
            return SerializationNullable::tryDeserializeNullAsDefaultOrNestedTextQuoted(nested_column, rb, settings, nested);
        return nested->tryDeserializeTextQuoted(nested_column, rb, settings);
    };

    return deserializeTextImpl<bool>(column, rb, read_nested, true);
}

}
