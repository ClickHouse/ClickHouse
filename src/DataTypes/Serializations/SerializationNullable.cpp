#include <DataTypes/Serializations/SerializationNullable.h>
#include <DataTypes/Serializations/SerializationNumber.h>
#include <DataTypes/Serializations/SerializationNamed.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>

#include <Columns/ColumnNullable.h>
#include <Core/Field.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/PeekableReadBuffer.h>
#include <Common/assert_cast.h>
#include <base/scope_guard.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_ALL_DATA;
}

DataTypePtr SerializationNullable::SubcolumnCreator::create(const DataTypePtr & prev) const
{
    return std::make_shared<DataTypeNullable>(prev);
}

SerializationPtr SerializationNullable::SubcolumnCreator::create(const SerializationPtr & prev) const
{
    return std::make_shared<SerializationNullable>(prev);
}

ColumnPtr SerializationNullable::SubcolumnCreator::create(const ColumnPtr & prev) const
{
    return ColumnNullable::create(prev, null_map);
}

void SerializationNullable::enumerateStreams(
    SubstreamPath & path,
    const StreamCallback & callback,
    const SubstreamData & data) const
{
    const auto * type_nullable = data.type ? &assert_cast<const DataTypeNullable &>(*data.type) : nullptr;
    const auto * column_nullable = data.column ? &assert_cast<const ColumnNullable &>(*data.column) : nullptr;

    path.push_back(Substream::NullMap);
    path.back().data =
    {
        std::make_shared<SerializationNamed>(std::make_shared<SerializationNumber<UInt8>>(), "null", false),
        type_nullable ? std::make_shared<DataTypeUInt8>() : nullptr,
        column_nullable ? column_nullable->getNullMapColumnPtr() : nullptr,
        data.serialization_info,
    };

    callback(path);

    path.back() = Substream::NullableElements;
    path.back().creator = std::make_shared<SubcolumnCreator>(path.back().data.column);
    path.back().data = data;

    SubstreamData next_data =
    {
        nested,
        type_nullable ? type_nullable->getNestedType() : nullptr,
        column_nullable ? column_nullable->getNestedColumnPtr() : nullptr,
        data.serialization_info,
    };

    nested->enumerateStreams(path, callback, next_data);
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

void SerializationNullable::deserializeTextRaw(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserializeTextRawImpl<void>(column, istr, settings, nested);
}

void SerializationNullable::serializeTextRaw(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const ColumnNullable & col = assert_cast<const ColumnNullable &>(column);

    if (col.isNullAt(row_num))
        writeString(settings.tsv.null_representation, ostr);
    else
        nested->serializeTextRaw(col.getNestedColumn(), row_num, ostr, settings);
}

template<typename ReturnType>
ReturnType SerializationNullable::deserializeTextRawImpl(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, const SerializationPtr & nested)
{
    return deserializeTextEscapedAndRawImpl<ReturnType, false>(column, istr, settings, nested);
}

template<typename ReturnType>
ReturnType SerializationNullable::deserializeTextEscapedImpl(IColumn & column, ReadBuffer & istr, const FormatSettings & settings,
                                                             const SerializationPtr & nested)
{
    return deserializeTextEscapedAndRawImpl<ReturnType, true>(column, istr, settings, nested);
}

template<typename ReturnType, bool escaped>
ReturnType SerializationNullable::deserializeTextEscapedAndRawImpl(IColumn & column, ReadBuffer & istr, const FormatSettings & settings,
                                                    const SerializationPtr & nested_serialization)
{
    const String & null_representation = settings.tsv.null_representation;

    /// Some data types can deserialize absence of data (e.g. empty string), so eof is ok.
    if (istr.eof() || (!null_representation.empty() && *istr.position() != null_representation[0]))
    {
        /// This is not null, surely.
        return safeDeserialize<ReturnType>(column, *nested_serialization,
            [] { return false; },
            [&nested_serialization, &istr, &settings] (IColumn & nested_column)
            {
                if constexpr (escaped)
                    nested_serialization->deserializeTextEscaped(nested_column, istr, settings);
                else
                    nested_serialization->deserializeTextRaw(nested_column, istr, settings);
            });
    }

    /// Check if we have enough data in buffer to check if it's a null.
    if (istr.available() > null_representation.size())
    {
        auto check_for_null = [&istr, &null_representation]()
        {
            auto * pos = istr.position();
            if (checkString(null_representation, istr) && (*istr.position() == '\t' || *istr.position() == '\n'))
                return true;
            istr.position() = pos;
            return false;
        };
        auto deserialize_nested = [&nested_serialization, &settings, &istr] (IColumn & nested_column)
        {
            if constexpr (escaped)
                nested_serialization->deserializeTextEscaped(nested_column, istr, settings);
            else
                nested_serialization->deserializeTextRaw(nested_column, istr, settings);
        };
        return safeDeserialize<ReturnType>(column, *nested_serialization, check_for_null, deserialize_nested);
    }

    /// We don't have enough data in buffer to check if it's a null.
    /// Use PeekableReadBuffer to make a checkpoint before checking null
    /// representation and rollback if check was failed.
    PeekableReadBuffer buf(istr, true);
    auto check_for_null = [&buf, &null_representation]()
    {
        buf.setCheckpoint();
        SCOPE_EXIT(buf.dropCheckpoint());
        if (checkString(null_representation, buf) && (buf.eof() || *buf.position() == '\t' || *buf.position() == '\n'))
            return true;

        buf.rollbackToCheckpoint();
        return false;
    };

    auto deserialize_nested = [&nested_serialization, &settings, &buf, &null_representation, &istr] (IColumn & nested_column)
    {
        auto * pos = buf.position();
        if constexpr (escaped)
            nested_serialization->deserializeTextEscaped(nested_column, buf, settings);
        else
            nested_serialization->deserializeTextRaw(nested_column, buf, settings);
        /// Check that we don't have any unread data in PeekableReadBuffer own memory.
        if (likely(!buf.hasUnreadData()))
            return;

        /// We have some unread data in PeekableReadBuffer own memory.
        /// It can happen only if there is a string instead of a number
        /// or if someone uses tab or LF in TSV null_representation.
        /// In the first case we cannot continue reading anyway. The second case seems to be unlikely.
        if (null_representation.find('\t') != std::string::npos || null_representation.find('\n') != std::string::npos)
            throw DB::ParsingException("TSV custom null representation containing '\\t' or '\\n' may not work correctly "
                                       "for large input.", ErrorCodes::CANNOT_READ_ALL_DATA);

        WriteBufferFromOwnString parsed_value;
        if constexpr (escaped)
            nested_serialization->serializeTextEscaped(nested_column, nested_column.size() - 1, parsed_value, settings);
        else
            nested_serialization->serializeTextRaw(nested_column, nested_column.size() - 1, parsed_value, settings);
        throw DB::ParsingException("Error while parsing \"" + std::string(pos, buf.buffer().end()) + std::string(istr.position(), std::min(size_t(10), istr.available())) + "\" as Nullable"
                                       + " at position " + std::to_string(istr.count()) + ": got \"" + std::string(pos, buf.position() - pos)
                                       + "\", which was deserialized as \""
                                       + parsed_value.str() + "\". It seems that input data is ill-formatted.",
                                   ErrorCodes::CANNOT_READ_ALL_DATA);
    };

    return safeDeserialize<ReturnType>(column, *nested_serialization, check_for_null, deserialize_nested);
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
    if (istr.eof() || (*istr.position() != 'N' && *istr.position() != 'n'))
    {
        /// This is not null, surely.
        return safeDeserialize<ReturnType>(column, *nested,
            [] { return false; },
            [&nested, &istr, &settings] (IColumn & nested_column) { nested->deserializeTextQuoted(nested_column, istr, settings); });
    }

    /// Check if we have enough data in buffer to check if it's a null.
    if (istr.available() >= 4)
    {
        auto check_for_null = [&istr]()
        {
            auto * pos = istr.position();
            if (checkStringCaseInsensitive("NULL", istr))
                return true;
            istr.position() = pos;
            return false;
        };
        auto deserialize_nested = [&nested, &settings, &istr] (IColumn & nested_column)
        {
            nested->deserializeTextQuoted(nested_column, istr, settings);
        };
        return safeDeserialize<ReturnType>(column, *nested, check_for_null, deserialize_nested);
    }

    /// We don't have enough data in buffer to check if it's a NULL
    /// and we cannot check it just by one symbol (otherwise we won't be able
    /// to differentiate for example NULL and NaN for float)
    /// Use PeekableReadBuffer to make a checkpoint before checking
    /// null and rollback if the check was failed.
    PeekableReadBuffer buf(istr, true);
    auto check_for_null = [&buf]()
    {
        buf.setCheckpoint();
        SCOPE_EXIT(buf.dropCheckpoint());
        if (checkStringCaseInsensitive("NULL", buf))
            return true;

        buf.rollbackToCheckpoint();
        return false;
    };

    auto deserialize_nested = [&nested, &settings, &buf] (IColumn & nested_column)
    {
        nested->deserializeTextQuoted(nested_column, buf, settings);
        /// Check that we don't have any unread data in PeekableReadBuffer own memory.
        if (likely(!buf.hasUnreadData()))
            return;

        /// We have some unread data in PeekableReadBuffer own memory.
        /// It can happen only if there is an unquoted string instead of a number.
        throw DB::ParsingException(
            ErrorCodes::CANNOT_READ_ALL_DATA,
            "Error while parsing Nullable: got an unquoted string {} instead of a number",
            String(buf.position(), std::min(10ul, buf.available())));
    };

    return safeDeserialize<ReturnType>(column, *nested, check_for_null, deserialize_nested);
}


void SerializationNullable::deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserializeWholeTextImpl<void>(column, istr, settings, nested);
}

template <typename ReturnType>
ReturnType SerializationNullable::deserializeWholeTextImpl(IColumn & column, ReadBuffer & istr, const FormatSettings & settings,
                                                  const SerializationPtr & nested)
{
    PeekableReadBuffer buf(istr, true);
    auto check_for_null = [&buf]()
    {
        buf.setCheckpoint();
        SCOPE_EXIT(buf.dropCheckpoint());

        if (checkStringCaseInsensitive("NULL", buf) && buf.eof())
            return true;

        buf.rollbackToCheckpoint();
        if (checkStringCaseInsensitive("ᴺᵁᴸᴸ", buf) && buf.eof())
            return true;

        buf.rollbackToCheckpoint();
        return false;
    };

    auto deserialize_nested = [&nested, &settings, &buf] (IColumn & nested_column)
    {
        nested->deserializeWholeText(nested_column, buf, settings);
        assert(!buf.hasUnreadData());
    };

    return safeDeserialize<ReturnType>(column, *nested, check_for_null, deserialize_nested);
}


void SerializationNullable::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const ColumnNullable & col = assert_cast<const ColumnNullable &>(column);

    if (col.isNullAt(row_num))
        writeString(settings.csv.null_representation, ostr);
    else
        nested->serializeTextCSV(col.getNestedColumn(), row_num, ostr, settings);
}

void SerializationNullable::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserializeTextCSVImpl<void>(column, istr, settings, nested);
}

template<typename ReturnType>
ReturnType SerializationNullable::deserializeTextCSVImpl(IColumn & column, ReadBuffer & istr, const FormatSettings & settings,
                                                         const SerializationPtr & nested_serialization)
{
    const String & null_representation = settings.csv.null_representation;
    if (istr.eof() || (!null_representation.empty() && *istr.position() != null_representation[0]))
    {
        /// This is not null, surely.
        return safeDeserialize<ReturnType>(column, *nested_serialization,
            [] { return false; },
            [&nested_serialization, &istr, &settings] (IColumn & nested_column) { nested_serialization->deserializeTextCSV(nested_column, istr, settings); });
    }

    /// Check if we have enough data in buffer to check if it's a null.
    if (istr.available() > null_representation.size())
    {
        auto check_for_null = [&istr, &null_representation, &settings]()
        {
            auto * pos = istr.position();
            if (checkString(null_representation, istr) && (*istr.position() == settings.csv.delimiter || *istr.position() == '\r' || *istr.position() == '\n'))
                return true;
            istr.position() = pos;
            return false;
        };
        auto deserialize_nested = [&nested_serialization, &settings, &istr] (IColumn & nested_column)
        {
            nested_serialization->deserializeTextCSV(nested_column, istr, settings);
        };
        return safeDeserialize<ReturnType>(column, *nested_serialization, check_for_null, deserialize_nested);
    }

    /// We don't have enough data in buffer to check if it's a null.
    /// Use PeekableReadBuffer to make a checkpoint before checking null
    /// representation and rollback if the check was failed.
    PeekableReadBuffer buf(istr, true);
    auto check_for_null = [&buf, &null_representation, &settings]()
    {
        buf.setCheckpoint();
        SCOPE_EXIT(buf.dropCheckpoint());
        if (checkString(null_representation, buf) && (buf.eof() || *buf.position() == settings.csv.delimiter || *buf.position() == '\r' || *buf.position() == '\n'))
            return true;

        buf.rollbackToCheckpoint();
        return false;
    };

    auto deserialize_nested = [&nested_serialization, &settings, &buf, &null_representation, &istr] (IColumn & nested_column)
    {
        auto * pos = buf.position();
        nested_serialization->deserializeTextCSV(nested_column, buf, settings);
        /// Check that we don't have any unread data in PeekableReadBuffer own memory.
        if (likely(!buf.hasUnreadData()))
            return;

        /// We have some unread data in PeekableReadBuffer own memory.
        /// It can happen only if there is an unquoted string instead of a number
        /// or if someone uses csv delimiter, LF or CR in CSV null representation.
        /// In the first case we cannot continue reading anyway. The second case seems to be unlikely.
        if (null_representation.find(settings.csv.delimiter) != std::string::npos || null_representation.find('\r') != std::string::npos
            || null_representation.find('\n') != std::string::npos)
            throw DB::ParsingException("CSV custom null representation containing format_csv_delimiter, '\\r' or '\\n' may not work correctly "
                                       "for large input.", ErrorCodes::CANNOT_READ_ALL_DATA);

        WriteBufferFromOwnString parsed_value;
        nested_serialization->serializeTextCSV(nested_column, nested_column.size() - 1, parsed_value, settings);
        throw DB::ParsingException("Error while parsing \"" + std::string(pos, buf.buffer().end()) + std::string(istr.position(), std::min(size_t(10), istr.available())) + "\" as Nullable"
                                       + " at position " + std::to_string(istr.count()) + ": got \"" + std::string(pos, buf.position() - pos)
                                       + "\", which was deserialized as \""
                                       + parsed_value.str() + "\". It seems that input data is ill-formatted.",
                                   ErrorCodes::CANNOT_READ_ALL_DATA);
    };

    return safeDeserialize<ReturnType>(column, *nested_serialization, check_for_null, deserialize_nested);
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
template bool SerializationNullable::deserializeTextRawImpl<bool>(IColumn & column, ReadBuffer & istr, const FormatSettings &, const SerializationPtr & nested);

}
