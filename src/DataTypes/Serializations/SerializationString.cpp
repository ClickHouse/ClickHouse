#include <algorithm>

#include <Common/SipHash.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/Serializations/SerializationString.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Field.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/Serializations/SerializationNumber.h>
#include <DataTypes/Serializations/SerializationStringSize.h>
#include <Formats/FormatSettings.h>
#include <Formats/ParseError.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>
#include <IO/WriteHelpers.h>
#include <base/unit.h>
#include <Common/assert_cast.h>

#ifdef __SSE2__
    #include <emmintrin.h>
#endif


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int TOO_LARGE_STRING_SIZE;
}

static constexpr UInt64 MAX_TOTAL_STRING_SIZE = 1ULL << 48;

UInt128 SerializationString::getHash(MergeTreeStringSerializationVersion version_)
{
    SipHash hash;
    hash.update("String");
    hash.update(static_cast<int>(version_));
    return hash.get128();
}

SerializationPtr SerializationString::create(MergeTreeStringSerializationVersion version_)
{
    return ISerialization::pooled(getHash(version_), [=] { return new SerializationString(version_); });
}

void SerializationString::serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const String & s = field.safeGet<String>();
    if (settings.binary.max_binary_string_size && s.size() > settings.binary.max_binary_string_size)
        throw Exception(
            ErrorCodes::TOO_LARGE_STRING_SIZE,
            "Too large string size: {}. The maximum is: {}. To increase the maximum, use setting "
            "format_binary_max_string_size",
            s.size(),
            settings.binary.max_binary_string_size);

    writeVarUInt(s.size(), ostr);
    writeString(s, ostr);
}


void SerializationString::deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings & settings) const
{
    UInt64 size = 0;
    readVarUInt(size, istr);
    if (settings.binary.max_binary_string_size && size > settings.binary.max_binary_string_size)
        throw Exception(
            ErrorCodes::TOO_LARGE_STRING_SIZE,
            "Too large string size: {}. The maximum is: {}. To increase the maximum, use setting "
            "format_binary_max_string_size",
            size,
            settings.binary.max_binary_string_size);

    field = String();
    String & s = field.safeGet<String>();
    s.resize(size);
    istr.readStrict(s.data(), size);
}


void SerializationString::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const std::string_view & s = assert_cast<const ColumnString &>(column).getDataAt(row_num);
    if (settings.binary.max_binary_string_size && s.size() > settings.binary.max_binary_string_size)
        throw Exception(
            ErrorCodes::TOO_LARGE_STRING_SIZE,
            "Too large string size: {}. The maximum is: {}. To increase the maximum, use setting "
            "format_binary_max_string_size",
            s.size(),
            settings.binary.max_binary_string_size);

    writeVarUInt(s.size(), ostr);
    writeString(s, ostr);
}


void SerializationString::deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    ColumnString & column_string = assert_cast<ColumnString &>(column);
    ColumnString::Chars & data = column_string.getChars();
    ColumnString::Offsets & offsets = column_string.getOffsets();

    UInt64 size = 0;
    readVarUInt(size, istr);
    if (settings.binary.max_binary_string_size && size > settings.binary.max_binary_string_size)
        throw Exception(
            ErrorCodes::TOO_LARGE_STRING_SIZE,
            "Too large string size: {}. The maximum is: {}. To increase the maximum, use setting "
            "format_binary_max_string_size",
            size,
            settings.binary.max_binary_string_size);

    size_t old_chars_size = data.size();
    size_t offset = old_chars_size + size;
    offsets.push_back(offset);

    try
    {
        data.resize(offset);
        istr.readStrict(reinterpret_cast<char*>(&data[offset - size]), size);
    }
    catch (...)
    {
        offsets.pop_back();
        data.resize_assume_reserved(old_chars_size);
        throw;
    }
}


void SerializationString::serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const
{
    const ColumnString & column_string = typeid_cast<const ColumnString &>(column);
    const ColumnString::Chars & data = column_string.getChars();
    const ColumnString::Offsets & offsets = column_string.getOffsets();

    size_t size = column_string.size();
    if (!size)
        return;

    size_t end = limit && offset + limit < size
        ? offset + limit
        : size;

    ColumnString::Offset prev_string_offset = offsets[offset - 1];
    for (size_t i = offset; i < end; ++i)
    {
        ColumnString::Offset next_string_offset = offsets[i];
        UInt64 str_size = next_string_offset - prev_string_offset;
        writeVarUInt(str_size, ostr);
        ostr.write(reinterpret_cast<const char *>(&data[prev_string_offset]), str_size);
        prev_string_offset = next_string_offset;
    }
}

template <size_t copy_size>
static NO_INLINE void deserializeBinaryImpl(ColumnString::Chars & data, ColumnString::Offsets & offsets, ReadBuffer & istr, size_t limit)
try
{
    size_t offset = data.size();
    /// Avoiding calling resize in a loop improves the performance.
    data.resize(std::max(data.capacity(), static_cast<size_t>(4096)));

    for (size_t i = 0; i < limit; ++i)
    {
        if (istr.eof())
            break;

        UInt64 size = 0;
        readVarUInt(size, istr);

        static constexpr size_t max_string_size = 16_GiB;   /// Arbitrary value to prevent logical errors and overflows, but large enough.
        if (size > max_string_size)
            throw Exception(
                ErrorCodes::TOO_LARGE_STRING_SIZE,
                "Too large string size: {}. The maximum is: {}.",
                size,
                max_string_size);

        offset += size;
        if (unlikely(offset > data.size()))
            data.resize_exact(roundUpToPowerOfTwoOrZero(std::max(offset, data.size() * 2)));

        if (size)
        {
            /// An optimistic branch in which more efficient copying is possible.
            if (offset + copy_size <= data.capacity() && istr.position() + size + copy_size <= istr.buffer().end())
            {
                const char * src_pos = istr.position();
                const char * src_end = src_pos + size;
                auto * dst_pos = &data[offset - size];

                while (src_pos < src_end)
                {
                    __builtin_memcpy(dst_pos, src_pos, copy_size);

                    src_pos += copy_size;
                    dst_pos += copy_size;
                }

                istr.position() += size;
            }
            else
            {
                istr.readStrict(reinterpret_cast<char*>(&data[offset - size]), size);
            }
        }

        offsets.push_back(offset);
    }

    data.resize_exact(offset);
}
catch (...)
{
    /// We are doing resize_exact() of bigger values than we have, let's make sure that it will be correct (even in case of exceptions)
    data.resize_exact(offsets.back());
    throw;
}


void SerializationString::deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t rows_offset, size_t limit, double avg_value_size_hint) const
{
    /// Skip certain number of values if requested
    for (size_t i = 0; i < rows_offset; ++i)
    {
        UInt64 size = 0;
        readVarUInt(size, istr);
        istr.ignore(size);
    }

    ColumnString & column_string = typeid_cast<ColumnString &>(column);
    ColumnString::Chars & data = column_string.getChars();
    ColumnString::Offsets & offsets = column_string.getOffsets();

    double avg_chars_size = 0; /// By default, do not reserve (as for empty strings).

    if (avg_value_size_hint > 0.0 && avg_value_size_hint > sizeof(offsets[0]))
    {
        /// Randomly selected.
        constexpr auto avg_value_size_hint_reserve_multiplier = 1.2;
        avg_chars_size = (avg_value_size_hint - sizeof(offsets[0])) * avg_value_size_hint_reserve_multiplier;
    }

    size_t size_to_reserve = data.size() + static_cast<size_t>(std::ceil(static_cast<double>(limit) * avg_chars_size));

    /// Never reserve for too big size.
    if (size_to_reserve < 256 * 1024 * 1024)
    {
        try
        {
            data.reserve(size_to_reserve);
        }
        catch (Exception & e)
        {
            e.addMessage(
                "(avg_value_size_hint = " + toString(avg_value_size_hint)
                + ", avg_chars_size = " + toString(avg_chars_size)
                + ", limit = " + toString(limit) + ")");
            throw;
        }
    }

    offsets.reserve(offsets.size() + limit);

    if (avg_chars_size >= 64)
        deserializeBinaryImpl<64>(data, offsets, istr, limit);
    else if (avg_chars_size >= 48)
        deserializeBinaryImpl<48>(data, offsets, istr, limit);
    else if (avg_chars_size >= 32)
        deserializeBinaryImpl<32>(data, offsets, istr, limit);
    else
        deserializeBinaryImpl<16>(data, offsets, istr, limit);
}

void SerializationString::enumerateStreams(
    EnumerateStreamsSettings & settings, const StreamCallback & callback, const SubstreamData & data) const
{
    switch (version)
    {
        case MergeTreeStringSerializationVersion::SINGLE_STREAM:
            enumerateStreamsWithoutSize(settings, callback, data);
            break;
        case MergeTreeStringSerializationVersion::WITH_SIZE_STREAM:
            enumerateStreamsWithSize(settings, callback, data);
            break;
    }
}

void SerializationString::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column,
    size_t offset,
    size_t limit,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    switch (version)
    {
        case MergeTreeStringSerializationVersion::SINGLE_STREAM:
            ISerialization::serializeBinaryBulkWithMultipleStreams(column, offset, limit, settings, state);
            break;
        case MergeTreeStringSerializationVersion::WITH_SIZE_STREAM:
            serializeBinaryBulkWithSizeStream(column, offset, limit, settings, state);
            break;
    }
}

void SerializationString::deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr & column,
    size_t rows_offset,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    switch (version)
    {
        case MergeTreeStringSerializationVersion::SINGLE_STREAM:
            deserializeBinaryBulkWithoutSizeStream(column, rows_offset, limit, settings, state, cache);
            break;
        case MergeTreeStringSerializationVersion::WITH_SIZE_STREAM:
            deserializeBinaryBulkWithSizeStream(column, rows_offset, limit, settings, state, cache);
            break;
    }
}

void SerializationString::enumerateStreamsWithoutSize(
    EnumerateStreamsSettings & settings,
    const StreamCallback & callback,
    const SubstreamData & data) const
{
    if (settings.enumerate_virtual_streams)
    {
        const auto * type_string = data.type ? &assert_cast<const DataTypeString &>(*data.type) : nullptr;

        auto sizes_serialization = SerializationStringSize::create(version);

        /// Inlined size stream. The column is not computed eagerly; instead a
        /// lazy column creator is attached so that `createFromPath` can derive it
        /// on demand when the `.size` subcolumn is actually requested.
        settings.path.push_back(Substream::InlinedStringSizes);

        auto substream_data = SubstreamData(sizes_serialization)
                                  .withType(type_string ? std::make_shared<DataTypeUInt64>() : nullptr)
                                  .withColumn(nullptr)
                                  .withSerializationInfo(data.serialization_info);

        if (data.column)
        {
            substream_data.withLazyColumnCreator([parent_col = data.column]() -> ColumnPtr
            {
                return assert_cast<const ColumnString &>(*parent_col).createSizeSubcolumn();
            });
        }

        settings.path.back().data = std::move(substream_data);

        callback(settings.path);
        settings.path.pop_back();
    }

    /// Regular stream
    settings.path.push_back(Substream::Regular);
    settings.path.back().data = data;
    callback(settings.path);
    settings.path.pop_back();
}

ISerialization::DeserializeBinaryBulkStatePtr DeserializeBinaryBulkStateStringWithoutSizeStream::clone() const
{
    auto res = std::make_shared<DeserializeBinaryBulkStateStringWithoutSizeStream>();
    res->column = column;
    res->need_string_data = need_string_data;
    return res;
}

void SerializationString::deserializeBinaryBulkWithoutSizeStream(
    ColumnPtr & column,
    size_t rows_offset,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    ISerialization::deserializeBinaryBulkWithMultipleStreams(column, rows_offset, limit, settings, state, cache);
}

void SerializationString::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeString(assert_cast<const ColumnString &>(column).getDataAt(row_num), ostr);
}


void SerializationString::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeEscapedString(assert_cast<const ColumnString &>(column).getDataAt(row_num), ostr);
}


template <typename ReturnType, typename Reader>
static inline ReturnType read(IColumn & column, Reader && reader)
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;
    ColumnString & column_string = assert_cast<ColumnString &>(column);
    ColumnString::Chars & data = column_string.getChars();
    ColumnString::Offsets & offsets = column_string.getOffsets();
    size_t old_chars_size = data.size();
    size_t old_offsets_size = offsets.size();
    auto restore_column = [&]()
    {
        offsets.resize_assume_reserved(old_offsets_size);
        data.resize_assume_reserved(old_chars_size);
    };

    try
    {
        if constexpr (throw_exception)
        {
            reader(data);
        }
        else if (!reader(data))
        {
            restore_column();
            return false;
        }

        offsets.push_back(data.size());
        return ReturnType(true);
    }
    catch (...)
    {
        restore_column();
        if constexpr (throw_exception)
            throw;
        /// Other errors (e.g. MEMORY_LIMIT_EXCEEDED) must propagate, not be reported as a failed parse.
        rethrowIfNotParseError();
        if constexpr (!throw_exception)
            return false;
    }
}


void SerializationString::deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    read<void>(column, [&](ColumnString::Chars & data) { readStringUntilEOFInto(data, istr); });
}

bool SerializationString::tryDeserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    return read<bool>(column, [&](ColumnString::Chars & data) { readStringUntilEOFInto(data, istr); return true; });
}

void SerializationString::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    read<void>(column, [&](ColumnString::Chars & data)
    {
        settings.tsv.crlf_end_of_line_input ? readEscapedStringInto<PaddedPODArray<UInt8>,true>(data, istr) : readEscapedStringInto<PaddedPODArray<UInt8>,false>(data, istr);
    });
}

bool SerializationString::tryDeserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    return read<bool>(column, [&](ColumnString::Chars & data) { readEscapedStringInto<PaddedPODArray<UInt8>,true>(data, istr); return true; });
}

void SerializationString::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    if (settings.values.escape_quote_with_quote)
        writeQuotedStringPostgreSQL(assert_cast<const ColumnString &>(column).getDataAt(row_num), ostr);
    else
        writeQuotedString(assert_cast<const ColumnString &>(column).getDataAt(row_num), ostr);
}


void SerializationString::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    read<void>(column, [&](ColumnString::Chars & data) { readQuotedStringInto<true>(data, istr); });
}

bool SerializationString::tryDeserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    return read<bool>(column, [&](ColumnString::Chars & data) { return tryReadQuotedStringInto<true>(data, istr); });
}


void SerializationString::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeJSONString(assert_cast<const ColumnString &>(column).getDataAt(row_num), ostr, settings);
}


void SerializationString::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    if (settings.json.read_objects_as_strings && !istr.eof() && *istr.position() == '{')
    {
        read<void>(column, [&](ColumnString::Chars & data) { readJSONObjectPossiblyInvalid(data, istr); });
    }
    else if (settings.json.read_arrays_as_strings && !istr.eof() && *istr.position() == '[')
    {
        read<void>(column, [&](ColumnString::Chars & data) { readJSONArrayInto(data, istr); });
    }
    else if (settings.json.read_bools_as_strings && !istr.eof() && (*istr.position() == 't' || *istr.position() == 'f'))
    {
        String str_value;
        if (*istr.position() == 't')
        {
            assertString("true", istr);
            str_value = "true";
        }
        else if (*istr.position() == 'f')
        {
            assertString("false", istr);
            str_value = "false";
        }

        read<void>(column, [&](ColumnString::Chars & data) { data.insert(str_value.begin(), str_value.end()); });
    }
    else if (settings.json.read_numbers_as_strings && !istr.eof() && *istr.position() != '"')
    {
        String field;
        readJSONField(field, istr, settings.json);
        Float64 tmp = 0;
        ReadBufferFromString buf(field);
        if (tryReadFloatTextPrecise(tmp, buf) && buf.eof())
            read<void>(column, [&](ColumnString::Chars & data) { data.insert(field.begin(), field.end()); });
        else
            throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot parse JSON String value here: {}", field);
    }
    else
        read<void>(column, [&](ColumnString::Chars & data) { readJSONStringInto(data, istr, settings.json); });
}

bool SerializationString::tryDeserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    if (settings.json.read_objects_as_strings && !istr.eof() && *istr.position() == '{')
        return read<bool>(column, [&](ColumnString::Chars & data) { return readJSONObjectPossiblyInvalid<ColumnString::Chars, bool>(data, istr); });

    if (settings.json.read_arrays_as_strings && !istr.eof() && *istr.position() == '[')
        return read<bool>(column, [&](ColumnString::Chars & data) { return readJSONArrayInto<ColumnString::Chars, bool>(data, istr); });

    if (settings.json.read_bools_as_strings && !istr.eof() && (*istr.position() == 't' || *istr.position() == 'f'))
    {
        String str_value;
        if (*istr.position() == 't')
        {
            if (!checkString("true", istr))
                return false;
            str_value = "true";
        }
        else if (*istr.position() == 'f')
        {
            if (!checkString("false", istr))
                return false;
            str_value = "false";
        }

        read<void>(column, [&](ColumnString::Chars & data) { data.insert(str_value.begin(), str_value.end()); });
        return true;
    }

    if (settings.json.read_numbers_as_strings && !istr.eof() && *istr.position() != '"')
    {
        String field;
        if (!tryReadJSONField(field, istr, settings.json))
            return false;

        Float64 tmp = 0;
        ReadBufferFromString buf(field);
        if (tryReadFloatTextPrecise(tmp, buf) && buf.eof())
        {
            read<void>(column, [&](ColumnString::Chars & data) { data.insert(field.begin(), field.end()); });
            return true;
        }

        return false;
    }

    return read<bool>(column, [&](ColumnString::Chars & data) { return tryReadJSONStringInto(data, istr, settings.json); });
}


void SerializationString::serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeXMLStringForTextElement(assert_cast<const ColumnString &>(column).getDataAt(row_num), ostr);
}


void SerializationString::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeCSVString<>(assert_cast<const ColumnString &>(column).getDataAt(row_num), ostr);
}


void SerializationString::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    read<void>(column, [&](ColumnString::Chars & data) { readCSVStringInto(data, istr, settings.csv); });
}

bool SerializationString::tryDeserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    return read<bool>(column, [&](ColumnString::Chars & data) { readCSVStringInto<ColumnString::Chars, false, false>(data, istr, settings.csv); return true; });
}

void SerializationString::serializeTextMarkdown(
    const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    if (settings.markdown.escape_special_characters)
        writeMarkdownEscapedString(assert_cast<const ColumnString &>(column).getDataAt(row_num), ostr);
    else
        serializeTextEscaped(column, row_num, ostr, settings);
}

SerializationString::SerializationString(MergeTreeStringSerializationVersion version_)
    : version(version_)
{
}

namespace
{

void serializeStringSizes(const IColumn & column, WriteBuffer & ostr, UInt64 offset, UInt64 limit)
{
    const auto & column_string = typeid_cast<const ColumnString &>(column);
    const auto & offset_values = column_string.getOffsets();
    size_t size = offset_values.size();

    if (!size)
        return;

    size_t end = limit && (offset + limit < size) ? offset + limit : size;
    UInt64 prev_offset = offset_values[offset - 1];

    for (size_t i = offset; i < end; ++i)
    {
        UInt64 current_offset = offset_values[i];
        writeBinaryLittleEndian(current_offset - prev_offset, ostr);
        prev_offset = current_offset;
    }
}

void appendStringSizesToColumnStringOffsets(ColumnString & column_string, const UInt64 * sizes, size_t start, size_t rows)
{
    auto & offsets = column_string.getOffsets();
    IColumn::Offset prev_offset = offsets.empty() ? 0 : offsets.back();

    offsets.reserve(offsets.size() + rows);

    for (size_t i = 0; i < rows; ++i)
    {
        prev_offset += sizes[start + i];
        offsets.push_back(prev_offset);
    }
}

}

void SerializationString::enumerateStreamsWithSize(
    EnumerateStreamsSettings & settings,
    const StreamCallback & callback,
    const SubstreamData & data) const
{
    const auto * type_string = data.type ? &assert_cast<const DataTypeString &>(*data.type) : nullptr;

    auto sizes_serialization = SerializationStringSize::create(version);

    /// Size stream. Same lazy pattern as in `enumerateStreamsWithoutSize`.
    settings.path.push_back(Substream::StringSizes);

    auto substream_data = SubstreamData(sizes_serialization)
                              .withType(type_string ? std::make_shared<DataTypeUInt64>() : nullptr)
                              .withColumn(nullptr)
                              .withSerializationInfo(data.serialization_info);

    if (data.column)
    {
        substream_data.withLazyColumnCreator([parent_col = data.column]() -> ColumnPtr
        {
            return assert_cast<const ColumnString &>(*parent_col).createSizeSubcolumn();
        });
    }

    settings.path.back().data = std::move(substream_data);
    callback(settings.path);

    /// Regular stream
    settings.path.back() = Substream::Regular;
    settings.path.back().data = data;
    callback(settings.path);
    settings.path.pop_back();
}

void SerializationString::serializeBinaryBulkWithSizeStream(
    const IColumn & column,
    size_t offset,
    size_t limit,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & /* state */) const
{
    if (const size_t size = column.size(); limit == 0 || offset + limit > size)
        limit = size - offset;

    settings.path.push_back(Substream::StringSizes);
    auto * size_stream = settings.getter(settings.path);
    if (!size_stream)
    {
        settings.path.pop_back();
        return;
    }

    const auto & column_string = typeid_cast<const ColumnString &>(column);
    const auto & offsets = column_string.getOffsets();

    /// On disk (position independent) the stream carries per-row sizes so a granule reads on its own.
    /// Over the network it carries the cumulative offsets directly (like Array offsets) for exact
    /// reader preallocation.
    if (settings.position_independent_encoding)
        serializeStringSizes(column, *size_stream, offset, limit);
    else
        SerializationNumber<ColumnString::Offset>::serializeBinaryBulk(offsets, *size_stream, offset, limit);

    settings.path.back() = Substream::Regular;
    auto * stream = settings.getter(settings.path);
    if (!stream)
        throw Exception(ErrorCodes::INCORRECT_DATA, "String stream is missing when try to serialize string with separate size stream");

    /// Serialize string data
    size_t begin = (offset == 0) ? 0 : offsets[offset - 1];
    size_t end = offsets[offset + limit - 1];
    size_t bytes = end - begin;
    stream->write(reinterpret_cast<const char *>(&column_string.getChars()[begin]), bytes);
    settings.path.pop_back();
}

struct DeserializeBinaryBulkStateStringWithSizeStream : public ISerialization::DeserializeBinaryBulkState
{
    ColumnPtr size_column;

    ISerialization::DeserializeBinaryBulkStatePtr clone() const override
    {
        auto res = std::make_shared<DeserializeBinaryBulkStateStringWithSizeStream>();
        res->size_column = size_column;
        return res;
    }

    void forEachColumn(const std::function<void(const ColumnPtr &)> & callback) const override
    {
        if (size_column)
            callback(size_column);
    }
};

void SerializationString::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings, DeserializeBinaryBulkStatePtr & state, SubstreamsDeserializeStatesCache * cache) const
{
    settings.path.push_back(Substream::Regular);
    if (auto cached_state = getFromSubstreamsDeserializeStatesCache(cache, settings.path))
    {
        state = cached_state;
        if (version == MergeTreeStringSerializationVersion::SINGLE_STREAM)
        {
            auto * string_state = checkAndGetState<DeserializeBinaryBulkStateStringWithoutSizeStream>(state);
            string_state->need_string_data = true;
        }
    }
    else
    {
        if (version == MergeTreeStringSerializationVersion::SINGLE_STREAM)
        {
            auto string_state = std::make_shared<DeserializeBinaryBulkStateStringWithoutSizeStream>();
            string_state->need_string_data = true;
            state = string_state;
        }
        else
        {
            state = std::make_shared<DeserializeBinaryBulkStateStringWithSizeStream>();
        }

        addToSubstreamsDeserializeStatesCache(cache, settings.path, state);
    }
    settings.path.pop_back();
}

std::pair<size_t, size_t> SerializationString::deserializeStringOffsetsAndGetDataRange(
    ColumnString & column,
    size_t rows_offset,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    auto & offsets = column.getOffsets();
    const IColumn::Offset prev_last_offset = offsets.empty() ? 0 : offsets.back();

    /// Over the network the stream carries cumulative byte offsets.
    if (!settings.position_independent_encoding)
    {
        chassert(rows_offset == 0 && prev_last_offset == 0);

        ReadBuffer * offsets_stream = settings.getter(settings.path);
        if (!offsets_stream)
            return {0, 0};

        SerializationNumber<ColumnString::Offset>::deserializeBinaryBulk(offsets, *offsets_stream, 0, limit);

        /// Offsets must be monotonically increasing.
        auto * const it = std::adjacent_find(offsets.begin(), offsets.end(), std::greater<>());
        if (it != offsets.end())
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "String offsets stream is not monotonically increasing (at index {}, value {})",
                std::distance(offsets.begin(), it),
                *it);

        return {0, offsets.back()};
    }

    /// On disk the stream carries per-row sizes, read through the state's sizes column and the cache.
    size_t num_read_rows = 0;
    auto * string_state = checkAndGetState<DeserializeBinaryBulkStateStringWithSizeStream>(state);
    if (auto cached_column_with_num_read_rows = getColumnWithNumReadRowsFromSubstreamsCache(cache, settings.path))
    {
        std::tie(string_state->size_column, num_read_rows) = *cached_column_with_num_read_rows;
    }
    else if (ReadBuffer * size_stream = settings.getter(settings.path))
    {
        /// If we started to read a new column, reinitialize sizes column in the state.
        if (!string_state->size_column || column.empty())
            string_state->size_column = ColumnUInt64::create();

        const size_t prev_size = string_state->size_column->size();
        SerializationNumber<UInt64>::create()->deserializeBinaryBulk(
            *string_state->size_column->assumeMutable(), *size_stream, 0, rows_offset + limit, 0);
        num_read_rows = string_state->size_column->size() - prev_size;
        /// The sizes column has rows_offset unapplied, so it can be cached as is.
        addColumnWithNumReadRowsToSubstreamsCache(cache, settings.path, string_state->size_column, num_read_rows);
    }
    else
    {
        return {0, 0};
    }

    const auto & sizes_data = assert_cast<const ColumnUInt64 &>(*string_state->size_column).getData();
    const size_t sizes_begin = sizes_data.size() - num_read_rows;
    size_t bytes_to_skip = 0;
    for (size_t i = sizes_begin; i != sizes_begin + rows_offset; ++i)
        bytes_to_skip += sizes_data[i];

    appendStringSizesToColumnStringOffsets(column, sizes_data.data(), sizes_begin + rows_offset, num_read_rows - rows_offset);
    return std::make_pair(bytes_to_skip, offsets.back() - prev_last_offset);
}

void SerializationString::deserializeBinaryBulkWithSizeStream(
    ColumnPtr & column,
    size_t rows_offset,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    /// Check if the whole String column is already deserialized and we have it in the cache.
    settings.path.push_back(Substream::Regular);
    if (insertDataFromSubstreamsCacheIfAny(cache, settings, column))
    {
        settings.path.pop_back();
        return;
    }

    settings.path.back() = Substream::StringSizes;
    auto mutable_column = column->assumeMutable();
    auto & string_column = assert_cast<ColumnString &>(*mutable_column);
    const size_t prev_num_rows = string_column.getOffsets().size();

    auto [bytes_to_skip, bytes_to_read] = deserializeStringOffsetsAndGetDataRange(string_column, rows_offset, limit, settings, state, cache);

    settings.path.back() = Substream::Regular;
    auto * stream = settings.getter(settings.path);

    if (!stream)
    {
        settings.path.pop_back();
        return;
    }

    if (bytes_to_read > MAX_TOTAL_STRING_SIZE)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Total size of String column is too large ({}): most likely the data is corrupted",bytes_to_read);

    auto & data = string_column.getChars();
    const size_t initial_size = data.size();
    data.resize(initial_size + bytes_to_read);
    stream->ignore(bytes_to_skip);
    stream->readBigStrict(reinterpret_cast<char *>(&data[initial_size]), bytes_to_read);

    column = std::move(mutable_column);
    addColumnWithNumReadRowsToSubstreamsCache(cache, settings.path, column, string_column.getOffsets().size() - prev_num_rows);
    settings.path.pop_back();
}

}
