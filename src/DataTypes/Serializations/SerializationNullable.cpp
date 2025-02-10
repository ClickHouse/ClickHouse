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
    EnumerateStreamsSettings & settings,
    const StreamCallback & callback,
    const SubstreamData & data) const
{
    const auto * type_nullable = data.type ? &assert_cast<const DataTypeNullable &>(*data.type) : nullptr;
    const auto * column_nullable = data.column ? &assert_cast<const ColumnNullable &>(*data.column) : nullptr;

    auto null_map_serialization = std::make_shared<SerializationNamed>(
        std::make_shared<SerializationNumber<UInt8>>(),
        "null", SubstreamType::NamedNullMap);

    settings.path.push_back(Substream::NullMap);
    auto null_map_data = SubstreamData(null_map_serialization)
        .withType(type_nullable ? std::make_shared<DataTypeUInt8>() : nullptr)
        .withColumn(column_nullable ? column_nullable->getNullMapColumnPtr() : nullptr)
        .withSerializationInfo(data.serialization_info);

    settings.path.back().data = null_map_data;
    callback(settings.path);

    settings.path.back() = Substream::NullableElements;
    settings.path.back().creator = std::make_shared<SubcolumnCreator>(null_map_data.column);
    settings.path.back().data = data;

    auto next_data = SubstreamData(nested)
        .withType(type_nullable ? type_nullable->getNestedType() : nullptr)
        .withColumn(column_nullable ? column_nullable->getNestedColumnPtr() : nullptr)
        .withSerializationInfo(data.serialization_info);

    nested->enumerateStreams(settings, callback, next_data);
    settings.path.pop_back();
}

void SerializationNullable::serializeBinaryBulkStatePrefix(
        const IColumn & column,
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const
{
    settings.path.push_back(Substream::NullableElements);
    const auto & column_nullable = assert_cast<const ColumnNullable &>(column);
    nested->serializeBinaryBulkStatePrefix(column_nullable.getNestedColumn(), settings, state);
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
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsDeserializeStatesCache * cache) const
{
    settings.path.push_back(Substream::NullableElements);
    nested->deserializeBinaryBulkStatePrefix(settings, state, cache);
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


void SerializationNullable::serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings & settings) const
{
    if (field.isNull())
    {
        writeBinary(true, ostr);
    }
    else
    {
        writeBinary(false, ostr);
        nested->serializeBinary(field, ostr, settings);
    }
}

void SerializationNullable::deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings & settings) const
{
    bool is_null = false;
    readBinary(is_null, istr);
    if (!is_null)
    {
        nested->deserializeBinary(field, istr, settings);
    }
    else
    {
        field = Null();
    }
}

void SerializationNullable::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const ColumnNullable & col = assert_cast<const ColumnNullable &>(column);

    bool is_null = col.isNullAt(row_num);
    writeBinary(is_null, ostr);
    if (!is_null)
        nested->serializeBinary(col.getNestedColumn(), row_num, ostr, settings);
}

template <typename ReturnType>
ReturnType safeAppendToNullMap(ColumnNullable & column, bool is_null)
{
    try
    {
        column.getNullMapData().push_back(is_null);
    }
    catch (...)
    {
        column.getNestedColumn().popBack(1);
        if constexpr (std::is_same_v<ReturnType, void>)
            throw;
        return ReturnType(false);
    }

    return ReturnType(true);
}

/// Deserialize value into non-nullable column. In case of NULL, insert default and set is_null to true.
/// If ReturnType is bool, return true if parsing was successful and false in case of any error.
template <typename ReturnType = void, typename CheckForNull, typename DeserializeNested>
static ReturnType deserializeImpl(IColumn & column, ReadBuffer & buf, CheckForNull && check_for_null, DeserializeNested && deserialize_nested, bool & is_null)
{
    is_null = check_for_null(buf);
    if (is_null)
    {
        column.insertDefault();
    }
    else
    {
        if constexpr (std::is_same_v<ReturnType, void>)
            deserialize_nested(column, buf);
        else if (!deserialize_nested(column, buf))
            return ReturnType(false);
    }

    return ReturnType(true);
}


void SerializationNullable::deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    ColumnNullable & col = assert_cast<ColumnNullable &>(column);
    bool is_null;
    auto check_for_null = [](ReadBuffer & buf)
    {
        bool is_null_ = false;
        readBinary(is_null_, buf);
        return is_null_;
    };
    auto deserialize_nested = [this, &settings] (IColumn & nested_column, ReadBuffer & buf) { nested->deserializeBinary(nested_column, buf, settings); };
    deserializeImpl(col.getNestedColumn(), istr, check_for_null, deserialize_nested, is_null);
    safeAppendToNullMap<void>(col, is_null);
}


void SerializationNullable::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const ColumnNullable & col = assert_cast<const ColumnNullable &>(column);

    if (col.isNullAt(row_num))
        serializeNullEscaped(ostr, settings);
    else
        nested->serializeTextEscaped(col.getNestedColumn(), row_num, ostr, settings);
}

void SerializationNullable::serializeNullEscaped(DB::WriteBuffer & ostr, const DB::FormatSettings & settings)
{
    writeString(settings.tsv.null_representation, ostr);
}

bool SerializationNullable::tryDeserializeNullEscaped(DB::ReadBuffer & istr, const DB::FormatSettings & settings)
{
    return checkString(settings.tsv.null_representation, istr);
}

void SerializationNullable::serializeTextRaw(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const ColumnNullable & col = assert_cast<const ColumnNullable &>(column);

    if (col.isNullAt(row_num))
        serializeNullRaw(ostr, settings);
    else
        nested->serializeTextRaw(col.getNestedColumn(), row_num, ostr, settings);
}

void SerializationNullable::serializeNullRaw(DB::WriteBuffer & ostr, const DB::FormatSettings & settings)
{
    writeString(settings.tsv.null_representation, ostr);
}

bool SerializationNullable::tryDeserializeNullRaw(DB::ReadBuffer & istr, const DB::FormatSettings & settings)
{
    return checkString(settings.tsv.null_representation, istr);
}

template<typename ReturnType, bool escaped>
ReturnType  deserializeTextEscapedAndRawImpl(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, const SerializationPtr & nested_serialization, bool & is_null)
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    const String & null_representation = settings.tsv.null_representation;
    auto deserialize_nested = [&nested_serialization, &settings] (IColumn & nested_column, ReadBuffer & buf_)
    {
        if constexpr (throw_exception)
        {
            if constexpr (escaped)
                nested_serialization->deserializeTextEscaped(nested_column, buf_, settings);
            else
                nested_serialization->deserializeTextRaw(nested_column, buf_, settings);
        }
        else
        {
            if constexpr (escaped)
                return nested_serialization->tryDeserializeTextEscaped(nested_column, buf_, settings);
            else
                return nested_serialization->tryDeserializeTextRaw(nested_column, buf_, settings);
        }
    };

    /// Some data types can deserialize absence of data (e.g. empty string), so eof is ok.
    if (istr.eof() || (!null_representation.empty() && *istr.position() != null_representation[0]))
    {
        /// This is not null, surely.
        return deserializeImpl<ReturnType>(column, istr, [](ReadBuffer &){ return false; }, deserialize_nested, is_null);
    }

    /// Check if we have enough data in buffer to check if it's a null.
    if (istr.available() > null_representation.size())
    {
        auto check_for_null = [&null_representation, &settings](ReadBuffer & buf)
        {
            auto * pos = buf.position();
            if (checkString(null_representation, buf) && (*buf.position() == '\t' || *buf.position() == '\n' || (settings.tsv.crlf_end_of_line_input && *buf.position() == '\r')))
                return true;
            buf.position() = pos;
            return false;
        };
        return deserializeImpl<ReturnType>(column, istr, check_for_null, deserialize_nested, is_null);
    }

    /// We don't have enough data in buffer to check if it's a null.
    /// Use PeekableReadBuffer to make a checkpoint before checking null
    /// representation and rollback if check was failed.
    PeekableReadBuffer peekable_buf(istr, true);
    auto check_for_null = [&null_representation, &settings](ReadBuffer & buf_)
    {
        auto & buf = assert_cast<PeekableReadBuffer &>(buf_);
        buf.setCheckpoint();
        SCOPE_EXIT(buf.dropCheckpoint());

        if (checkString(null_representation, buf) && (buf.eof() || *buf.position() == '\t' || *buf.position() == '\n' || (settings.tsv.crlf_end_of_line_input && *buf.position() == '\r')))
            return true;
        buf.rollbackToCheckpoint();
        return false;
    };

    auto deserialize_nested_with_check = [&deserialize_nested, &nested_serialization, &settings, &null_representation, &istr] (IColumn & nested_column, ReadBuffer & buf_)
    {
        auto & buf = assert_cast<PeekableReadBuffer &>(buf_);
        auto * pos = buf.position();
        if constexpr (throw_exception)
            deserialize_nested(nested_column, buf);
        else if (!deserialize_nested(nested_column, buf))
            return ReturnType(false);

        /// Check that we don't have any unread data in PeekableReadBuffer own memory.
        if (likely(!buf.hasUnreadData()))
            return ReturnType(true);

        /// We have some unread data in PeekableReadBuffer own memory.
        /// It can happen only if there is a string instead of a number
        /// or if someone uses tab or LF in TSV null_representation.
        /// In the first case we cannot continue reading anyway. The second case seems to be unlikely.
        /// We also should delete incorrectly deserialized value from nested column.
        nested_column.popBack(1);

        if constexpr (!throw_exception)
            return ReturnType(false);

        if (null_representation.find('\t') != std::string::npos || null_representation.find('\n') != std::string::npos)
            throw DB::Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "TSV custom null representation "
                "containing '\\t' or '\\n' may not work correctly for large input.");
        if (settings.tsv.crlf_end_of_line_input && null_representation.find('\r') != std::string::npos)
            throw DB::Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "TSV custom null representation "
                "containing '\\r' may not work correctly for large input.");

        WriteBufferFromOwnString parsed_value;
        if constexpr (escaped)
            nested_serialization->serializeTextEscaped(nested_column, nested_column.size() - 1, parsed_value, settings);
        else
            nested_serialization->serializeTextRaw(nested_column, nested_column.size() - 1, parsed_value, settings);
        throw DB::Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Error while parsing \"{}{}\" as Nullable"
                                   " at position {}: got \"{}\", which was deserialized as \"{}\". "
                                   "It seems that input data is ill-formatted.",
                                   std::string(pos, buf.buffer().end()),
                                   std::string(istr.position(), std::min(size_t(10), istr.available())),
                                   istr.count(), std::string(pos, buf.position() - pos), parsed_value.str());
    };

    return deserializeImpl<ReturnType>(column, peekable_buf, check_for_null, deserialize_nested_with_check, is_null);
}

void SerializationNullable::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    ColumnNullable & col = assert_cast<ColumnNullable &>(column);
    bool is_null;
    deserializeTextEscapedAndRawImpl<void, true>(col.getNestedColumn(), istr, settings, nested, is_null);
    safeAppendToNullMap<void>(col, is_null);
}

bool SerializationNullable::tryDeserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    ColumnNullable & col = assert_cast<ColumnNullable &>(column);
    bool is_null;
    return deserializeTextEscapedAndRawImpl<bool, true>(col.getNestedColumn(), istr, settings, nested, is_null) && safeAppendToNullMap<bool>(col, is_null);
}

bool SerializationNullable::deserializeNullAsDefaultOrNestedTextEscaped(IColumn & nested_column, ReadBuffer & istr, const FormatSettings & settings, const SerializationPtr & nested_serialization)
{
    bool is_null;
    deserializeTextEscapedAndRawImpl<void, true>(nested_column, istr, settings, nested_serialization, is_null);
    return !is_null;
}

bool SerializationNullable::tryDeserializeNullAsDefaultOrNestedTextEscaped(IColumn & nested_column, ReadBuffer & istr, const FormatSettings & settings, const SerializationPtr & nested_serialization)
{
    bool is_null;
    return deserializeTextEscapedAndRawImpl<bool, true>(nested_column, istr, settings, nested_serialization, is_null);
}

void SerializationNullable::deserializeTextRaw(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    ColumnNullable & col = assert_cast<ColumnNullable &>(column);
    bool is_null;
    deserializeTextEscapedAndRawImpl<void, false>(col.getNestedColumn(), istr, settings, nested, is_null);
    safeAppendToNullMap<void>(col, is_null);
}

bool SerializationNullable::tryDeserializeTextRaw(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    ColumnNullable & col = assert_cast<ColumnNullable &>(column);
    bool is_null;
    return deserializeTextEscapedAndRawImpl<bool, false>(col.getNestedColumn(), istr, settings, nested, is_null) && safeAppendToNullMap<bool>(col, is_null);
}

bool SerializationNullable::deserializeNullAsDefaultOrNestedTextRaw(IColumn & nested_column, ReadBuffer & istr, const FormatSettings & settings, const SerializationPtr & nested_serialization)
{
    bool is_null;
    deserializeTextEscapedAndRawImpl<void, false>(nested_column, istr, settings, nested_serialization, is_null);
    return !is_null;
}

bool SerializationNullable::tryDeserializeNullAsDefaultOrNestedTextRaw(IColumn & nested_column, ReadBuffer & istr, const FormatSettings & settings, const SerializationPtr & nested_serialization)
{
    bool is_null;
    return deserializeTextEscapedAndRawImpl<bool, false>(nested_column, istr, settings, nested_serialization, is_null);
}

void SerializationNullable::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const ColumnNullable & col = assert_cast<const ColumnNullable &>(column);

    if (col.isNullAt(row_num))
        serializeNullQuoted(ostr);
    else
        nested->serializeTextQuoted(col.getNestedColumn(), row_num, ostr, settings);
}

void SerializationNullable::serializeNullQuoted(DB::WriteBuffer & ostr)
{
    writeCString("NULL", ostr);
}

bool SerializationNullable::tryDeserializeNullQuoted(DB::ReadBuffer & istr)
{
    return checkStringCaseInsensitive("NULL", istr);
}

template<typename ReturnType>
ReturnType deserializeTextQuotedImpl(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, const SerializationPtr & nested, bool & is_null)
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    auto deserialize_nested = [&nested, &settings] (IColumn & nested_column, ReadBuffer & buf)
    {
        if constexpr (!throw_exception)
            return nested->tryDeserializeTextQuoted(nested_column, buf, settings);
        nested->deserializeTextQuoted(nested_column, buf, settings);
    };

    if (istr.eof() || (*istr.position() != 'N' && *istr.position() != 'n'))
    {
        /// This is not null, surely.
        return deserializeImpl<ReturnType>(column, istr, [](ReadBuffer &){ return false; }, deserialize_nested, is_null);
    }

    /// Check if we have enough data in buffer to check if it's a null.
    if (istr.available() >= 4)
    {
        auto check_for_null = [](ReadBuffer & buf)
        {
            auto * pos = buf.position();
            if (checkStringCaseInsensitive("NULL", buf))
                return true;
            buf.position() = pos;
            return false;
        };
        return deserializeImpl<ReturnType>(column, istr, check_for_null, deserialize_nested, is_null);
    }

    /// We don't have enough data in buffer to check if it's a NULL
    /// and we cannot check it just by one symbol (otherwise we won't be able
    /// to differentiate for example NULL and NaN for float)
    /// Use PeekableReadBuffer to make a checkpoint before checking
    /// null and rollback if the check was failed.
    PeekableReadBuffer peekable_buf(istr, true);
    auto check_for_null = [](ReadBuffer & buf_)
    {
        auto & buf = assert_cast<PeekableReadBuffer &>(buf_);
        buf.setCheckpoint();
        SCOPE_EXIT(buf.dropCheckpoint());
        if (checkStringCaseInsensitive("NULL", buf))
            return true;

        buf.rollbackToCheckpoint();
        return false;
    };

    auto deserialize_nested_with_check = [&deserialize_nested] (IColumn & nested_column, ReadBuffer & buf_)
    {
        auto & buf = assert_cast<PeekableReadBuffer &>(buf_);

        if constexpr (throw_exception)
            deserialize_nested(nested_column, buf);
        else if (!deserialize_nested(nested_column, buf))
            return false;

        /// Check that we don't have any unread data in PeekableReadBuffer own memory.
        if (likely(!buf.hasUnreadData()))
            return ReturnType(true);

        /// We have some unread data in PeekableReadBuffer own memory.
        /// It can happen only if there is an unquoted string instead of a number.
        /// We also should delete incorrectly deserialized value from nested column.
        nested_column.popBack(1);

        if constexpr (!throw_exception)
            return ReturnType(false);

        throw DB::Exception(
            ErrorCodes::CANNOT_READ_ALL_DATA,
            "Error while parsing Nullable: got an unquoted string {} instead of a number",
            String(buf.position(), std::min(10ul, buf.available())));
    };

    return deserializeImpl<ReturnType>(column, peekable_buf, check_for_null, deserialize_nested_with_check, is_null);
}


void SerializationNullable::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    ColumnNullable & col = assert_cast<ColumnNullable &>(column);
    bool is_null;
    deserializeTextQuotedImpl<void>(col.getNestedColumn(), istr, settings, nested, is_null);
    safeAppendToNullMap<void>(col, is_null);
}

bool SerializationNullable::tryDeserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    ColumnNullable & col = assert_cast<ColumnNullable &>(column);
    bool is_null;
    return deserializeTextQuotedImpl<bool>(col.getNestedColumn(), istr, settings, nested, is_null) && safeAppendToNullMap<bool>(col, is_null);
}

bool SerializationNullable::deserializeNullAsDefaultOrNestedTextQuoted(DB::IColumn & nested_column, DB::ReadBuffer & istr, const DB::FormatSettings & settings, const DB::SerializationPtr & nested_serialization)
{
    bool is_null;
    deserializeTextQuotedImpl<void>(nested_column, istr, settings, nested_serialization, is_null);
    return !is_null;
}

bool SerializationNullable::tryDeserializeNullAsDefaultOrNestedTextQuoted(DB::IColumn & nested_column, DB::ReadBuffer & istr, const DB::FormatSettings & settings, const DB::SerializationPtr & nested_serialization)
{
    bool is_null;
    return deserializeTextQuotedImpl<bool>(nested_column, istr, settings, nested_serialization, is_null);
}

template <typename ReturnType>
ReturnType deserializeWholeTextImpl(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, const SerializationPtr & nested, bool & is_null)
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    PeekableReadBuffer peekable_buf(istr, true);
    auto check_for_null = [](ReadBuffer & buf_)
    {
        auto & buf = assert_cast<PeekableReadBuffer &>(buf_);
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

    auto deserialize_nested = [&nested, &settings] (IColumn & nested_column, ReadBuffer & buf_)
    {
        auto & buf = assert_cast<PeekableReadBuffer &>(buf_);
        if constexpr (!throw_exception)
            return nested->tryDeserializeWholeText(nested_column, buf, settings);

        nested->deserializeWholeText(nested_column, buf, settings);
        assert(!buf.hasUnreadData());
    };

    return deserializeImpl<ReturnType>(column, peekable_buf, check_for_null, deserialize_nested, is_null);
}

void SerializationNullable::deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    ColumnNullable & col = assert_cast<ColumnNullable &>(column);
    bool is_null;
    deserializeWholeTextImpl<void>(col.getNestedColumn(), istr, settings, nested, is_null);
    safeAppendToNullMap<void>(col, is_null);
}

bool SerializationNullable::tryDeserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    ColumnNullable & col = assert_cast<ColumnNullable &>(column);
    bool is_null;
    return deserializeWholeTextImpl<bool>(col.getNestedColumn(), istr, settings, nested, is_null) && safeAppendToNullMap<bool>(col, is_null);
}

bool SerializationNullable::deserializeNullAsDefaultOrNestedWholeText(DB::IColumn & nested_column, DB::ReadBuffer & istr, const DB::FormatSettings & settings, const DB::SerializationPtr & nested_serialization)
{
    bool is_null;
    deserializeWholeTextImpl<void>(nested_column, istr, settings, nested_serialization, is_null);
    return !is_null;
}

bool SerializationNullable::tryDeserializeNullAsDefaultOrNestedWholeText(DB::IColumn & nested_column, DB::ReadBuffer & istr, const DB::FormatSettings & settings, const DB::SerializationPtr & nested_serialization)
{
    bool is_null;
    return deserializeWholeTextImpl<bool>(nested_column, istr, settings, nested_serialization, is_null);
}

void SerializationNullable::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const ColumnNullable & col = assert_cast<const ColumnNullable &>(column);

    if (col.isNullAt(row_num))
        writeString(settings.csv.null_representation, ostr);
    else
        nested->serializeTextCSV(col.getNestedColumn(), row_num, ostr, settings);
}

void SerializationNullable::serializeNullCSV(DB::WriteBuffer & ostr, const DB::FormatSettings & settings)
{
    writeString(settings.csv.null_representation, ostr);
}

bool SerializationNullable::tryDeserializeNullCSV(DB::ReadBuffer & istr, const DB::FormatSettings & settings)
{
    return checkString(settings.csv.null_representation, istr);
}

template<typename ReturnType>
ReturnType deserializeTextCSVImpl(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, const SerializationPtr & nested_serialization, bool & is_null)
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    auto deserialize_nested = [&nested_serialization, &settings] (IColumn & nested_column, ReadBuffer & buf)
    {
        if constexpr (!throw_exception)
            return nested_serialization->tryDeserializeTextCSV(nested_column, buf, settings);
        nested_serialization->deserializeTextCSV(nested_column, buf, settings);
    };

    const String & null_representation = settings.csv.null_representation;
    if (istr.eof() || (!null_representation.empty() && *istr.position() != null_representation[0]))
    {
        /// This is not null, surely.
        return deserializeImpl<ReturnType>(column, istr, [](ReadBuffer &){ return false; }, deserialize_nested, is_null);
    }

    /// Check if we have enough data in buffer to check if it's a null.
    if (settings.csv.custom_delimiter.empty() && istr.available() > null_representation.size())
    {
        auto check_for_null = [&null_representation, &settings](ReadBuffer & buf)
        {
            auto * pos = buf.position();
            if (checkString(null_representation, buf) && (*buf.position() == settings.csv.delimiter || *buf.position() == '\r' || *buf.position() == '\n'))
                return true;
            buf.position() = pos;
            return false;
        };
        return deserializeImpl<ReturnType>(column, istr, check_for_null, deserialize_nested, is_null);
    }

    /// We don't have enough data in buffer to check if it's a null.
    /// Use PeekableReadBuffer to make a checkpoint before checking null
    /// representation and rollback if the check was failed.
    PeekableReadBuffer peekable_buf(istr, true);
    auto check_for_null = [&null_representation, &settings](ReadBuffer & buf_)
    {
        auto & buf = assert_cast<PeekableReadBuffer &>(buf_);
        buf.setCheckpoint();
        SCOPE_EXIT(buf.dropCheckpoint());
        if (checkString(null_representation, buf))
        {
            if (!settings.csv.custom_delimiter.empty())
            {
                if (checkString(settings.csv.custom_delimiter, buf))
                {
                    /// Rollback to the beginning of custom delimiter.
                    buf.rollbackToCheckpoint();
                    assertString(null_representation, buf);
                    return true;
                }
            }
            else if (buf.eof() || *buf.position() == settings.csv.delimiter || *buf.position() == '\r' || *buf.position() == '\n')
                return true;
        }

        buf.rollbackToCheckpoint();
        return false;
    };

    auto deserialize_nested_with_check = [&deserialize_nested, &nested_serialization, &settings, &null_representation, &istr] (IColumn & nested_column, ReadBuffer & buf_)
    {
        auto & buf = assert_cast<PeekableReadBuffer &>(buf_);
        auto * pos = buf.position();
        if constexpr (throw_exception)
            deserialize_nested(nested_column, buf);
        else if (!deserialize_nested(nested_column, buf))
            return ReturnType(false);

        /// Check that we don't have any unread data in PeekableReadBuffer own memory.
        if (likely(!buf.hasUnreadData()))
            return ReturnType(true);

        /// We have some unread data in PeekableReadBuffer own memory.
        /// It can happen only if there is an unquoted string instead of a number
        /// or if someone uses csv delimiter, LF or CR in CSV null representation.
        /// In the first case we cannot continue reading anyway. The second case seems to be unlikely.
        /// We also should delete incorrectly deserialized value from nested column.
        nested_column.popBack(1);

        if constexpr (!throw_exception)
            return ReturnType(false);

        if (null_representation.find(settings.csv.delimiter) != std::string::npos || null_representation.find('\r') != std::string::npos
            || null_representation.find('\n') != std::string::npos)
            throw DB::Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "CSV custom null representation containing "
                                       "format_csv_delimiter, '\\r' or '\\n' may not work correctly for large input.");

        WriteBufferFromOwnString parsed_value;
        nested_serialization->serializeTextCSV(nested_column, nested_column.size() - 1, parsed_value, settings);
        throw DB::Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Error while parsing \"{}{}\" as Nullable"
                                   " at position {}: got \"{}\", which was deserialized as \"{}\". "
                                   "It seems that input data is ill-formatted.",
                                   std::string(pos, buf.buffer().end()),
                                   std::string(istr.position(), std::min(size_t(10), istr.available())),
                                   istr.count(), std::string(pos, buf.position() - pos), parsed_value.str());
    };

    return deserializeImpl<ReturnType>(column, peekable_buf, check_for_null, deserialize_nested_with_check, is_null);
}

void SerializationNullable::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    ColumnNullable & col = assert_cast<ColumnNullable &>(column);
    bool is_null;
    deserializeTextCSVImpl<void>(col.getNestedColumn(), istr, settings, nested, is_null);
    safeAppendToNullMap<void>(col, is_null);
}

bool SerializationNullable::tryDeserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    ColumnNullable & col = assert_cast<ColumnNullable &>(column);
    bool is_null;
    return deserializeTextCSVImpl<bool>(col.getNestedColumn(), istr, settings, nested, is_null) && safeAppendToNullMap<bool>(col, is_null);
}

bool SerializationNullable::deserializeNullAsDefaultOrNestedTextCSV(DB::IColumn & nested_column, DB::ReadBuffer & istr, const DB::FormatSettings & settings, const DB::SerializationPtr & nested_serialization)
{
    bool is_null;
    deserializeTextCSVImpl<void>(nested_column, istr, settings, nested_serialization, is_null);
    return !is_null;
}

bool SerializationNullable::tryDeserializeNullAsDefaultOrNestedTextCSV(DB::IColumn & nested_column, DB::ReadBuffer & istr, const DB::FormatSettings & settings, const DB::SerializationPtr & nested_serialization)
{
    bool is_null;
    return deserializeTextCSVImpl<bool>(nested_column, istr, settings, nested_serialization, is_null);
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
        serializeNullText(ostr, settings);
    else
        nested->serializeText(col.getNestedColumn(), row_num, ostr, settings);
}

void SerializationNullable::serializeNullText(DB::WriteBuffer & ostr, const DB::FormatSettings & settings)
{
    if (settings.pretty.charset == FormatSettings::Pretty::Charset::UTF8)
        writeCString("ᴺᵁᴸᴸ", ostr);
    else
        writeCString("NULL", ostr);
}

bool SerializationNullable::tryDeserializeNullText(DB::ReadBuffer & istr)
{
    if (checkCharCaseInsensitive('N', istr))
        return checkStringCaseInsensitive("ULL", istr);
    return checkStringCaseInsensitive("ᴺᵁᴸᴸ", istr);
}

void SerializationNullable::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const ColumnNullable & col = assert_cast<const ColumnNullable &>(column);

    if (col.isNullAt(row_num))
        serializeNullJSON(ostr);
    else
        nested->serializeTextJSON(col.getNestedColumn(), row_num, ostr, settings);
}

void SerializationNullable::serializeNullJSON(DB::WriteBuffer & ostr)
{
    writeCString("null", ostr);
}

bool SerializationNullable::tryDeserializeNullJSON(DB::ReadBuffer & istr)
{
    return checkString("null", istr);
}

template<typename ReturnType>
ReturnType deserializeTextJSONImpl(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, const SerializationPtr & nested, bool & is_null)
{
    auto check_for_null = [](ReadBuffer & buf){ return checkStringByFirstCharacterAndAssertTheRest("null", buf); };
    auto deserialize_nested = [&nested, &settings](IColumn & nested_column, ReadBuffer & buf)
    {
        if constexpr (std::is_same_v<ReturnType, bool>)
            return nested->tryDeserializeTextJSON(nested_column, buf, settings);
        nested->deserializeTextJSON(nested_column, buf, settings);
    };

    return deserializeImpl<ReturnType>(column, istr, check_for_null, deserialize_nested, is_null);
}

void SerializationNullable::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    ColumnNullable & col = assert_cast<ColumnNullable &>(column);
    bool is_null;
    deserializeTextJSONImpl<void>(col.getNestedColumn(), istr, settings, nested, is_null);
    safeAppendToNullMap<void>(col, is_null);
}

bool SerializationNullable::tryDeserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    ColumnNullable & col = assert_cast<ColumnNullable &>(column);
    bool is_null;
    return deserializeTextJSONImpl<bool>(col.getNestedColumn(), istr, settings, nested, is_null) && safeAppendToNullMap<bool>(col, is_null);
}

bool SerializationNullable::deserializeNullAsDefaultOrNestedTextJSON(DB::IColumn & nested_column, DB::ReadBuffer & istr, const DB::FormatSettings & settings, const DB::SerializationPtr & nested_serialization)
{
    bool is_null;
    deserializeTextJSONImpl<void>(nested_column, istr, settings, nested_serialization, is_null);
    return !is_null;
}

bool SerializationNullable::tryDeserializeNullAsDefaultOrNestedTextJSON(DB::IColumn & nested_column, DB::ReadBuffer & istr, const DB::FormatSettings & settings, const DB::SerializationPtr & nested_serialization)
{
    bool is_null;
    return deserializeTextJSONImpl<bool>(nested_column, istr, settings, nested_serialization, is_null);
}

void SerializationNullable::serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const ColumnNullable & col = assert_cast<const ColumnNullable &>(column);

    if (col.isNullAt(row_num))
        writeCString("\\N", ostr);
    else
        nested->serializeTextXML(col.getNestedColumn(), row_num, ostr, settings);
}

void SerializationNullable::serializeNullXML(DB::WriteBuffer & ostr)
{
    writeCString("\\N", ostr);
}

}
