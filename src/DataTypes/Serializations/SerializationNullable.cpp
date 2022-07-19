#include <DataTypes/Serializations/SerializationNullable.h>
#include <DataTypes/Serializations/SerializationNumber.h>

#include <Columns/ColumnNullable.h>
#include <Core/Field.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/ConcatReadBuffer.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_ALL_DATA;
}

void SerializationNullable::enumerateStreams(const StreamCallback & callback, SubstreamPath & path) const
{
    path.push_back(Substream::NullMap);
    callback(path);
    path.back() = Substream::NullableElements;
    nested->enumerateStreams(callback, path);
    path.pop_back();
}


void SerializationNullable::serializeBinaryBulkStatePrefix(
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const
{
    settings.path.push_back(Substream::NullableElements);
    nested->serializeBinaryBulkStatePrefix(settings, state);
    settings.path.pop_back();
}


void SerializationNullable::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    settings.path.push_back(Substream::NullableElements);
    nested->serializeBinaryBulkStateSuffix(settings, state);
    settings.path.pop_back();
}


void SerializationNullable::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state) const
{
    settings.path.push_back(Substream::NullableElements);
    nested->deserializeBinaryBulkStatePrefix(settings, state);
    settings.path.pop_back();
}


void SerializationNullable::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column,
    size_t offset,
    size_t limit,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    const ColumnNullable & col = assert_cast<const ColumnNullable &>(column);
    col.checkConsistency();

    /// First serialize null map.
    settings.path.push_back(Substream::NullMap);
    if (auto * stream = settings.getter(settings.path))
        SerializationNumber<UInt8>().serializeBinaryBulk(col.getNullMapColumn(), *stream, offset, limit);

    /// Then serialize contents of arrays.
    settings.path.back() = Substream::NullableElements;
    nested->serializeBinaryBulkWithMultipleStreams(col.getNestedColumn(), offset, limit, settings, state);
    settings.path.pop_back();
}


void SerializationNullable::deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr & column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    auto mutable_column = column->assumeMutable();
    ColumnNullable & col = assert_cast<ColumnNullable &>(*mutable_column);

    settings.path.push_back(Substream::NullMap);
    if (auto cached_column = getFromSubstreamsCache(cache, settings.path))
    {
        col.getNullMapColumnPtr() = cached_column;
    }
    else if (auto * stream = settings.getter(settings.path))
    {
        SerializationNumber<UInt8>().deserializeBinaryBulk(col.getNullMapColumn(), *stream, limit, 0);
        addToSubstreamsCache(cache, settings.path, col.getNullMapColumnPtr());
    }

    settings.path.back() = Substream::NullableElements;
    nested->deserializeBinaryBulkWithMultipleStreams(col.getNestedColumnPtr(), limit, settings, state, cache);
    settings.path.pop_back();
}


void SerializationNullable::serializeBinary(const Field & field, WriteBuffer & ostr) const
{
    if (field.isNull())
    {
        writeBinary(true, ostr);
    }
    else
    {
        writeBinary(false, ostr);
        nested->serializeBinary(field, ostr);
    }
}

void SerializationNullable::deserializeBinary(Field & field, ReadBuffer & istr) const
{
    bool is_null = false;
    readBinary(is_null, istr);
    if (!is_null)
    {
        nested->deserializeBinary(field, istr);
    }
    else
    {
        field = Null();
    }
}

void SerializationNullable::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    const ColumnNullable & col = assert_cast<const ColumnNullable &>(column);

    bool is_null = col.isNullAt(row_num);
    writeBinary(is_null, ostr);
    if (!is_null)
        nested->serializeBinary(col.getNestedColumn(), row_num, ostr);
}

/// Deserialize value into ColumnNullable.
/// We need to insert both to nested column and to null byte map, or, in case of exception, to not insert at all.
template <typename ReturnType = void, typename CheckForNull, typename DeserializeNested, typename std::enable_if_t<std::is_same_v<ReturnType, void>, ReturnType>* = nullptr>
static ReturnType safeDeserialize(
    IColumn & column, const ISerialization &,
    CheckForNull && check_for_null, DeserializeNested && deserialize_nested)
{
    ColumnNullable & col = assert_cast<ColumnNullable &>(column);

    if (check_for_null())
    {
        col.insertDefault();
    }
    else
    {
        deserialize_nested(col.getNestedColumn());

        try
        {
            col.getNullMapData().push_back(0);
        }
        catch (...)
        {
            col.getNestedColumn().popBack(1);
            throw;
        }
    }
}

/// Deserialize value into non-nullable column. In case of NULL, insert default value and return false.
template <typename ReturnType = void, typename CheckForNull, typename DeserializeNested, typename std::enable_if_t<std::is_same_v<ReturnType, bool>, ReturnType>* = nullptr>
static ReturnType safeDeserialize(
        IColumn & column, const ISerialization & nested,
        CheckForNull && check_for_null, DeserializeNested && deserialize_nested)
{
    assert(!dynamic_cast<ColumnNullable *>(&column));
    assert(!dynamic_cast<const SerializationNullable *>(&nested));
    UNUSED(nested);

    bool insert_default = check_for_null();
    if (insert_default)
        column.insertDefault();
    else
        deserialize_nested(column);
    return !insert_default;
}


void SerializationNullable::deserializeBinary(IColumn & column, ReadBuffer & istr) const
{
    safeDeserialize(column, *nested,
        [&istr] { bool is_null = false; readBinary(is_null, istr); return is_null; },
        [this, &istr] (IColumn & nested_column) { nested->deserializeBinary(nested_column, istr); });
}


void SerializationNullable::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const ColumnNullable & col = assert_cast<const ColumnNullable &>(column);

    if (col.isNullAt(row_num))
        writeString(settings.tsv.null_representation, ostr);
    else
        nested->serializeTextEscaped(col.getNestedColumn(), row_num, ostr, settings);
}


void SerializationNullable::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserializeTextEscapedImpl<void>(column, istr, settings, nested);
}

template<typename ReturnType>
ReturnType SerializationNullable::deserializeTextEscapedImpl(IColumn & column, ReadBuffer & istr, const FormatSettings & settings,
                                                    const SerializationPtr & nested)
{
    /// Little tricky, because we cannot discriminate null from first character.

    if (istr.eof() || *istr.position() != '\\') /// Some data types can deserialize absence of data (e.g. empty string), so eof is ok.
    {
        /// This is not null, surely.
        return safeDeserialize<ReturnType>(column, *nested,
            [] { return false; },
            [&nested, &istr, &settings] (IColumn & nested_column) { nested->deserializeTextEscaped(nested_column, istr, settings); });
    }
    else
    {
        /// Now we know, that data in buffer starts with backslash.
        ++istr.position();

        if (istr.eof())
            throw ParsingException("Unexpected end of stream, while parsing value of Nullable type, after backslash", ErrorCodes::CANNOT_READ_ALL_DATA);

        return safeDeserialize<ReturnType>(column, *nested,
            [&istr]
            {
                if (*istr.position() == 'N')
                {
                    ++istr.position();
                    return true;
                }
                return false;
            },
            [&nested, &istr, &settings] (IColumn & nested_column)
            {
                if (istr.position() != istr.buffer().begin())
                {
                    /// We could step back to consume backslash again.
                    --istr.position();
                    nested->deserializeTextEscaped(nested_column, istr, settings);
                }
                else
                {
                    /// Otherwise, we need to place backslash back in front of istr.
                    ReadBufferFromMemory prefix("\\", 1);
                    ConcatReadBuffer prepended_istr(prefix, istr);

                    nested->deserializeTextEscaped(nested_column, prepended_istr, settings);

                    /// Synchronise cursor position in original buffer.

                    if (prepended_istr.count() > 1)
                        istr.position() = prepended_istr.position();
                }
            });
    }
}

void SerializationNullable::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const ColumnNullable & col = assert_cast<const ColumnNullable &>(column);

    if (col.isNullAt(row_num))
        writeCString("NULL", ostr);
    else
        nested->serializeTextQuoted(col.getNestedColumn(), row_num, ostr, settings);
}


void SerializationNullable::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserializeTextQuotedImpl<void>(column, istr, settings, nested);
}

template<typename ReturnType>
ReturnType SerializationNullable::deserializeTextQuotedImpl(IColumn & column, ReadBuffer & istr, const FormatSettings & settings,
                                                   const SerializationPtr & nested)
{
    return safeDeserialize<ReturnType>(column, *nested,
        [&istr]
        {
            return checkStringByFirstCharacterAndAssertTheRestCaseInsensitive("NULL", istr);
        },
        [&nested, &istr, &settings] (IColumn & nested_column) { nested->deserializeTextQuoted(nested_column, istr, settings); });
}


void SerializationNullable::deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserializeWholeTextImpl<void>(column, istr, settings, nested);
}

template <typename ReturnType>
ReturnType SerializationNullable::deserializeWholeTextImpl(IColumn & column, ReadBuffer & istr, const FormatSettings & settings,
                                                  const SerializationPtr & nested)
{
    return safeDeserialize<ReturnType>(column, *nested,
        [&istr]
        {
            return checkStringByFirstCharacterAndAssertTheRestCaseInsensitive("NULL", istr)
                || checkStringByFirstCharacterAndAssertTheRest("ᴺᵁᴸᴸ", istr);
        },
        [&nested, &istr, &settings] (IColumn & nested_column) { nested->deserializeWholeText(nested_column, istr, settings); });
}


void SerializationNullable::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const ColumnNullable & col = assert_cast<const ColumnNullable &>(column);

    if (col.isNullAt(row_num))
        writeCString("\\N", ostr);
    else
        nested->serializeTextCSV(col.getNestedColumn(), row_num, ostr, settings);
}

void SerializationNullable::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserializeTextCSVImpl<void>(column, istr, settings, nested);
}

template<typename ReturnType>
ReturnType SerializationNullable::deserializeTextCSVImpl(IColumn & column, ReadBuffer & istr, const FormatSettings & settings,
                                                    const SerializationPtr & nested)
{
    constexpr char const * null_literal = "NULL";
    constexpr size_t len = 4;
    size_t null_prefix_len = 0;

    auto check_for_null = [&istr, &settings, &null_prefix_len]
    {
        if (checkStringByFirstCharacterAndAssertTheRest("\\N", istr))
            return true;
        if (!settings.csv.unquoted_null_literal_as_null)
            return false;

        /// Check for unquoted NULL
        while (!istr.eof() && null_prefix_len < len && null_literal[null_prefix_len] == *istr.position())
        {
            ++null_prefix_len;
            ++istr.position();
        }
        if (null_prefix_len == len)
            return true;

        /// Value and "NULL" have common prefix, but value is not "NULL".
        /// Restore previous buffer position if possible.
        if (null_prefix_len <= istr.offset())
        {
            istr.position() -= null_prefix_len;
            null_prefix_len = 0;
        }
        return false;
    };

    auto deserialize_nested = [&nested, &settings, &istr, &null_prefix_len] (IColumn & nested_column)
    {
        if (likely(!null_prefix_len))
            nested->deserializeTextCSV(nested_column, istr, settings);
        else
        {
            /// Previous buffer position was not restored,
            /// so we need to prepend extracted characters (rare case)
            ReadBufferFromMemory prepend(null_literal, null_prefix_len);
            ConcatReadBuffer buf(prepend, istr);
            nested->deserializeTextCSV(nested_column, buf, settings);

            /// Check if all extracted characters were read by nested parser and update buffer position
            if (null_prefix_len < buf.count())
                istr.position() = buf.position();
            else if (null_prefix_len > buf.count())
            {
                /// It can happen only if there is an unquoted string instead of a number
                /// or if someone uses 'U' or 'L' as delimiter in CSV.
                /// In the first case we cannot continue reading anyway. The second case seems to be unlikely.
                if (settings.csv.delimiter == 'U' || settings.csv.delimiter == 'L')
                    throw DB::ParsingException("Enabled setting input_format_csv_unquoted_null_literal_as_null may not work correctly "
                                        "with format_csv_delimiter = 'U' or 'L' for large input.", ErrorCodes::CANNOT_READ_ALL_DATA);
                WriteBufferFromOwnString parsed_value;
                nested->serializeTextCSV(nested_column, nested_column.size() - 1, parsed_value, settings);
                throw DB::ParsingException("Error while parsing \"" + std::string(null_literal, null_prefix_len)
                                    + std::string(istr.position(), std::min(size_t{10}, istr.available())) + "\" as Nullable"
                                    + " at position " + std::to_string(istr.count()) + ": got \"" + std::string(null_literal, buf.count())
                                    + "\", which was deserialized as \""
                                    + parsed_value.str() + "\". It seems that input data is ill-formatted.",
                                    ErrorCodes::CANNOT_READ_ALL_DATA);
            }
        }
    };

    return safeDeserialize<ReturnType>(column, *nested, check_for_null, deserialize_nested);
}

void SerializationNullable::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const ColumnNullable & col = assert_cast<const ColumnNullable &>(column);

    /// In simple text format (like 'Pretty' format) (these formats are suitable only for output and cannot be parsed back),
    ///  data is printed without escaping.
    /// It makes theoretically impossible to distinguish between NULL and some string value, regardless on how do we print NULL.
    /// For this reason, we output NULL in a bit strange way.
    /// This assumes UTF-8 and proper font support. This is Ok, because Pretty formats are "presentational", not for data exchange.

    if (col.isNullAt(row_num))
    {
        if (settings.pretty.charset == FormatSettings::Pretty::Charset::UTF8)
            writeCString("ᴺᵁᴸᴸ", ostr);
        else
            writeCString("NULL", ostr);
    }
    else
        nested->serializeText(col.getNestedColumn(), row_num, ostr, settings);
}

void SerializationNullable::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const ColumnNullable & col = assert_cast<const ColumnNullable &>(column);

    if (col.isNullAt(row_num))
        writeCString("null", ostr);
    else
        nested->serializeTextJSON(col.getNestedColumn(), row_num, ostr, settings);
}

void SerializationNullable::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserializeTextJSONImpl<void>(column, istr, settings, nested);
}

template<typename ReturnType>
ReturnType SerializationNullable::deserializeTextJSONImpl(IColumn & column, ReadBuffer & istr, const FormatSettings & settings,
                                                    const SerializationPtr & nested)
{
    return safeDeserialize<ReturnType>(column, *nested,
        [&istr] { return checkStringByFirstCharacterAndAssertTheRest("null", istr); },
        [&nested, &istr, &settings] (IColumn & nested_column) { nested->deserializeTextJSON(nested_column, istr, settings); });
}

void SerializationNullable::serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const ColumnNullable & col = assert_cast<const ColumnNullable &>(column);

    if (col.isNullAt(row_num))
        writeCString("\\N", ostr);
    else
        nested->serializeTextXML(col.getNestedColumn(), row_num, ostr, settings);
}

template bool SerializationNullable::deserializeWholeTextImpl<bool>(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, const SerializationPtr & nested);
template bool SerializationNullable::deserializeTextEscapedImpl<bool>(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, const SerializationPtr & nested);
template bool SerializationNullable::deserializeTextQuotedImpl<bool>(IColumn & column, ReadBuffer & istr, const FormatSettings &, const SerializationPtr & nested);
template bool SerializationNullable::deserializeTextCSVImpl<bool>(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, const SerializationPtr & nested);
template bool SerializationNullable::deserializeTextJSONImpl<bool>(IColumn & column, ReadBuffer & istr, const FormatSettings &, const SerializationPtr & nested);

}
