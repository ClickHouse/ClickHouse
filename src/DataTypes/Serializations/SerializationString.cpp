#include <algorithm>

#include <Common/SipHash.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/Serializations/SerializationString.h>

#include <Columns/ColumnReplicated.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Field.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/Serializations/SerializationNumber.h>
#include <DataTypes/Serializations/SerializationStringSize.h>
#include <DataTypes/Serializations/StringGranuleDictionary.h>
#include <Formats/FormatSettings.h>
#include <Formats/ParseError.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>
#include <IO/WriteHelpers.h>
#include <base/unit.h>
#include <Common/StringValueFilter.h>
#include <Common/assert_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_ALL_DATA;
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
    extern const int TOO_LARGE_STRING_SIZE;
}

/// Guards a size stream against requesting an unbounded String data allocation.
static constexpr UInt64 MAX_TOTAL_STRING_SIZE = 1ULL << 48;

/// The size of a string comes from the data, so it has to be validated before it is used to resize anything.
/// `format_binary_max_string_size` is a user-facing limit that can be disabled by setting it to `0`,
/// while `MAX_STRING_SIZE` is a hard limit that is always enforced.
static void checkStringSize(UInt64 size, const FormatSettings & settings)
{
    if (settings.binary.max_binary_string_size && size > settings.binary.max_binary_string_size)
        throw Exception(
            ErrorCodes::TOO_LARGE_STRING_SIZE,
            "Too large string size: {}. The maximum is: {}. To increase the maximum, use setting "
            "format_binary_max_string_size",
            size,
            settings.binary.max_binary_string_size);

    if (size > SerializationString::MAX_STRING_SIZE)
        throw Exception(
            ErrorCodes::TOO_LARGE_STRING_SIZE,
            "Too large string size: {}. The maximum is: {}.",
            size,
            SerializationString::MAX_STRING_SIZE);
}

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

bool SerializationString::isStringSizesSubcolumn(const SubstreamPath & path)
{
    return !path.empty() && (path.back().type == Substream::StringSizes || path.back().type == Substream::InlinedStringSizes);
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
    checkStringSize(size, settings);

    field = String();
    String & s = field.safeGet<String>();
    if (size <= istr.available())
    {
        s.resize(size);
        istr.readStrict(s.data(), size);
        return;
    }
    readStringGrowing(s, size, istr);
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


void SerializationString::deserializeBinary(std::string_view & value, std::string_view & input, const FormatSettings & settings) const
{
    UInt64 size = 0;
    const char * data = readVarUInt(size, input.data(), input.size());
    checkStringSize(size, settings);

    const size_t remaining = input.size() - (data - input.data());
    if (size > remaining)
        throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Cannot read a string of {} bytes, only {} bytes are left", size, remaining);

    value = std::string_view(data, size);
    input.remove_prefix(input.size() - remaining + size);
}

void SerializationString::deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    ColumnString & column_string = assert_cast<ColumnString &>(column);
    ColumnString::Chars & data = column_string.getChars();
    ColumnString::Offsets & offsets = column_string.getOffsets();

    UInt64 size = 0;
    readVarUInt(size, istr);
    checkStringSize(size, settings);

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

        if (size > SerializationString::MAX_STRING_SIZE)
            throw Exception(
                ErrorCodes::TOO_LARGE_STRING_SIZE,
                "Too large string size: {}. The maximum is: {}.",
                size,
                SerializationString::MAX_STRING_SIZE);

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

/// Reads one string value of a known size from the buffer and appends it to `data` (updating `offset`)
/// if it matches the filter; a non-matching value is consumed from the buffer but not appended.
/// Returns true if the value matched.
static ALWAYS_INLINE bool readFilteredStringValue(
    ColumnString::Chars & data, size_t & offset, ReadBuffer & istr, size_t size, const StringValueFilter & filter)
{
    /// Note: `istr.available()` instead of `istr.position() + size <= istr.buffer().end()`,
    /// because the buffer may be empty and its pointers null, and offsetting a null pointer is UB.
    if (size <= istr.available())
    {
        /// The value is fully in the buffer: check it in place and copy only if it matches.
        bool matched = filter.match(istr.position(), size);
        if (matched)
        {
            if (unlikely(offset + size > data.size()))
                data.resize_exact(roundUpToPowerOfTwoOrZero(std::max(offset + size, data.size() * 2)));
            memcpy(&data[offset], istr.position(), size);
            offset += size;
        }
        istr.position() += size;
        return matched;
    }

    /// The value crosses the buffer boundary: read it into the column and roll back if it does not match.
    if (unlikely(offset + size > data.size()))
        data.resize_exact(roundUpToPowerOfTwoOrZero(std::max(offset + size, data.size() * 2)));
    istr.readStrict(reinterpret_cast<char *>(&data[offset]), size);

    bool matched = filter.match(reinterpret_cast<const char *>(&data[offset]), size);
    if (matched)
        offset += size;
    return matched;
}

/// Filtered deserialization of the data stream when the value sizes are already known:
/// `offsets[first_row .. offsets.size())` contain the original cumulative offsets of the values,
/// and this function rewrites them to the filtered ones. Values that do not match the filter
/// are replaced with empty strings without copying their data (see `StringValueFilter`).
///
/// When the filter has a substring condition, the values are checked in bulk: each contiguous
/// buffer of concatenated values is scanned for the needle only once, and the found occurrences
/// are mapped to values. This is much faster than a search per value, and together with the
/// skipped copying it is faster than reading all the values.
static NO_INLINE void deserializeBinaryFilteredWithKnownSizes(
    ColumnString::Chars & data,
    ColumnString::Offsets & offsets,
    ReadBuffer & istr,
    size_t first_row,
    const StringValueFilter & filter)
{
    const size_t num_rows = offsets.size();
    size_t offset = data.size();

    size_t values_checked = 0;
    size_t values_replaced = 0;
    size_t bytes_skipped = 0;

    /// The values decided by the bulk scan are accounted separately: they are not checked one by one,
    /// so filtering them pays off even when they are short. Only the found candidates are checked
    /// one by one against the other conditions of the filter, if there are any.
    size_t bulk_values_checked = 0;
    size_t bulk_values_replaced = 0;
    size_t bulk_bytes_skipped = 0;
    size_t bulk_values_checked_individually = 0;

    const bool use_bulk_scan = filter.hasBulkScanCondition();
    const bool has_other_conditions = filter.hasOtherConditions();

    /// The original (unfiltered) cumulative offset of the current stream position.
    /// The invariant `chars.size() == offsets.back()` held before the new offsets were appended.
    size_t original_pos = offset;
    /// Values in the current buffer that contain the bulk-scanned needle.
    std::vector<size_t> matched_values;

    size_t i = first_row;
    try
    {
        while (i < num_rows)
        {
            /// Find the values that fit into the current buffer entirely.
            /// (Note that `eof` refills the buffer when it is exhausted.)
            size_t j = i;
            const char * buffer_begin = nullptr;
            if (use_bulk_scan && !istr.eof())
            {
                buffer_begin = istr.position();
                size_t buffer_original_end = original_pos + (istr.buffer().end() - istr.position());
                j = std::upper_bound(offsets.begin() + i, offsets.begin() + num_rows, buffer_original_end) - offsets.begin();
            }

            if (j == i)
            {
                /// Either there is no substring condition to scan for in bulk, or the current value
                /// crosses the buffer boundary: check this one value directly.
                size_t value_size = offsets[i] - original_pos;

                ++values_checked;

                /// An empty string never matches the filter.
                if (value_size == 0 || !readFilteredStringValue(data, offset, istr, value_size, filter))
                {
                    ++values_replaced;
                    bytes_skipped += value_size;
                }

                original_pos += value_size;
                offsets[i] = offset;
                ++i;
                continue;
            }

            /// Scan the buffer once and map the found occurrences of the needle to values.
            const size_t scan_original_start = original_pos;
            const char * scan_end = buffer_begin + (offsets[j - 1] - scan_original_start);

            matched_values.clear();
            filter.findBulkScanMatches(buffer_begin, scan_original_start, offsets.data(), i, j, matched_values);

            /// Materialize the values: copy the matched ones (checking the remaining conditions
            /// of the filter), replace the rest with empty strings.
            size_t next_matched = 0;
            for (; i < j; ++i)
            {
                size_t value_size = offsets[i] - original_pos;
                const char * value_data = buffer_begin + (original_pos - scan_original_start);
                original_pos = offsets[i];

                ++bulk_values_checked;

                bool matches = false;
                if (next_matched < matched_values.size() && matched_values[next_matched] == i)
                {
                    ++next_matched;
                    matches = filter.matchOtherConditions(value_data, value_size);
                    bulk_values_checked_individually += has_other_conditions;
                }

                if (matches)
                {
                    if (unlikely(offset + value_size > data.size()))
                        data.resize_exact(roundUpToPowerOfTwoOrZero(std::max(offset + value_size, data.size() * 2)));
                    memcpy(&data[offset], value_data, value_size);
                    offset += value_size;
                }
                else
                {
                    ++bulk_values_replaced;
                    bulk_bytes_skipped += value_size;
                }

                offsets[i] = offset;
            }

            istr.position() += scan_end - buffer_begin;
        }
    }
    catch (...)
    {
        /// Drop the rows whose values were not fully read to keep the column consistent.
        offsets.resize(i);
        data.resize_exact(offset);
        throw;
    }

    data.resize_exact(offset);
    filter.updateStats(values_checked, values_replaced, bytes_skipped);
    filter.updateStatsOfBulkScan(bulk_values_checked, bulk_values_replaced, bulk_bytes_skipped, bulk_values_checked_individually);
}

/// Deserialization which checks every value against the filter and reads non-matching values as empty strings.
/// See the comment for `StringValueFilter`: it is only correct because the rows with non-matching values
/// are guaranteed to be filtered out later by PREWHERE.
static NO_INLINE void deserializeBinaryFiltered(
    ColumnString::Chars & data, ColumnString::Offsets & offsets, ReadBuffer & istr, size_t limit, const StringValueFilter & filter)
try
{
    size_t offset = data.size();
    size_t values_checked = 0;
    size_t values_replaced = 0;
    size_t bytes_skipped = 0;

    for (size_t i = 0; i < limit; ++i)
    {
        if (istr.eof())
            break;

        UInt64 size = 0;
        readVarUInt(size, istr);

        if (size > SerializationString::MAX_STRING_SIZE)
            throw Exception(
                ErrorCodes::TOO_LARGE_STRING_SIZE,
                "Too large string size: {}. The maximum is: {}.",
                size,
                SerializationString::MAX_STRING_SIZE);

        ++values_checked;

        /// An empty string never matches the filter.
        if (size == 0 || !readFilteredStringValue(data, offset, istr, size, filter))
        {
            ++values_replaced;
            bytes_skipped += size;
        }

        offsets.push_back(offset);
    }

    data.resize_exact(offset);
    filter.updateStats(values_checked, values_replaced, bytes_skipped);
}
catch (...)
{
    /// We could have resized `data` beyond the last complete value, restore consistency (even in case of exceptions).
    /// `offsets` can be empty if an exception was thrown before the first value was fully read.
    data.resize_exact(offsets.empty() ? 0 : offsets.back());
    throw;
}


void SerializationString::deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint) const
{
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
        case MergeTreeStringSerializationVersion::WITH_DICTIONARIES:
            enumerateStreamsWithDictionaries(settings, callback, data);
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
        case MergeTreeStringSerializationVersion::WITH_DICTIONARIES:
            serializeBinaryBulkWithDictionaries(column, offset, limit, settings, state);
            break;
    }
}

void SerializationString::deserializeBinaryBulkWithMultipleStreams(
    IColumn & column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    switch (version)
    {
        case MergeTreeStringSerializationVersion::SINGLE_STREAM:
            deserializeBinaryBulkWithoutSizeStream(column, limit, settings, state, cache);
            break;
        case MergeTreeStringSerializationVersion::WITH_SIZE_STREAM:
            deserializeBinaryBulkWithSizeStream(column, limit, settings, cache);
            break;
        case MergeTreeStringSerializationVersion::WITH_DICTIONARIES:
            deserializeBinaryBulkWithDictionaries(column, limit, settings, state, cache);
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

ISerialization::DeserializeBinaryBulkStatePtr DeserializeBinaryBulkStateStringWithoutSizeStream::cloneImpl(ISerialization::DeserializeBinaryBulkStateCloneContext &) const
{
    auto res = std::make_shared<DeserializeBinaryBulkStateStringWithoutSizeStream>();
    res->need_string_data = need_string_data;
    return res;
}

void SerializationString::deserializeBinaryBulkWithoutSizeStream(
    IColumn & column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    if (!settings.string_value_filter || !settings.string_value_filter->isEnabled())
    {
        ISerialization::deserializeBinaryBulkWithMultipleStreams(column, limit, settings, state, cache);
        return;
    }

    /// The same as the default implementation above, but values that do not match the filter
    /// are replaced with empty strings without copying their data into the column.
    settings.path.push_back(Substream::Regular);

    if (insertDataFromSubstreamsCacheIfAny(cache, settings, column))
    {
        /// Data was inserted from substreams cache.
    }
    else if (ReadBuffer * stream = settings.getter(settings.path))
    {
        size_t prev_size = column.size();
        auto & column_string = assert_cast<ColumnString &>(column);

        column_string.getOffsets().reserve(column_string.getOffsets().size() + limit);
        deserializeBinaryFiltered(column_string.getChars(), column_string.getOffsets(), *stream, limit, *settings.string_value_filter);

        addColumnWithNumReadRowsToSubstreamsCache(cache, settings.path, column.getPtr(), column.size() - prev_size);
        /// Note: we intentionally do not update the average value size hint from a filtered column.
    }

    settings.path.pop_back();
}

void SerializationString::serializeTextHive(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeString(assert_cast<const ColumnString &>(column).getDataAt(row_num), ostr);
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

/// Writes `count` lengths, `UInt64` each, little-endian, as the stream of substream `StringSizes` holds them.
void writeStringSizes(const UInt64 * sizes, size_t count, WriteBuffer & ostr)
{
    if constexpr (std::endian::native == std::endian::big)
    {
        for (size_t i = 0; i < count; ++i)
            writeBinaryLittleEndian(sizes[i], ostr);
    }
    else
    {
        ostr.write(reinterpret_cast<const char *>(sizes), sizeof(UInt64) * count);
    }
}

void serializeStringSizes(const IColumn & column, WriteBuffer & ostr, UInt64 offset, UInt64 limit)
{
    const auto & column_string = typeid_cast<const ColumnString &>(column);
    const auto & offset_values = column_string.getOffsets();
    size_t size = offset_values.size();

    if (!size)
        return;

    size_t end = limit && (offset + limit < size) ? offset + limit : size;
    size_t count = end - offset;
    PaddedPODArray<UInt64> sizes;
    sizes.resize(count);

    UInt64 prev_offset = offset_values[offset - 1];
    for (size_t i = 0; i < count; ++i)
    {
        UInt64 current_offset = offset_values[offset + i];
        sizes[i] = current_offset - prev_offset;
        prev_offset = current_offset;
    }

    writeStringSizes(sizes.data(), count, ostr);
}

void serializeStringOffsets(const ColumnString::Offsets & offsets, WriteBuffer & ostr, size_t offset, size_t limit)
{
    const ColumnString::Offset base = offsets[offset - 1];
    if (base == 0)
    {
        SerializationNumber<ColumnString::Offset>::serializeBinaryBulk(offsets, ostr, offset, limit);
        return;
    }

    if (limit == 0)
        return;

    if constexpr (std::endian::native == std::endian::big)
    {
        for (size_t i = offset; i < offset + limit; ++i)
            writeBinaryLittleEndian(offsets[i] - base, ostr);
    }
    else
    {
        PaddedPODArray<ColumnString::Offset> rebased;
        rebased.resize(limit);

        for (size_t i = 0; i < limit; ++i)
            rebased[i] = offsets[offset + i] - base;

        ostr.write(reinterpret_cast<const char *>(rebased.data()), sizeof(ColumnString::Offset) * limit);
    }
}

void appendStringSizesToColumnStringOffsets(ColumnString & column_string, const UInt64 * sizes, size_t start, size_t rows)
{
    auto & offsets = column_string.getOffsets();

    /// The sizes come from a separate stream, so nothing bounds them by the data that follows them and
    /// their sum can overflow the offsets. A 128-bit accumulator cannot overflow, so one check of the
    /// total is enough: below it every offset is exact.
    unsigned __int128 offset = offsets.empty() ? 0 : offsets.back();

    /// Resize once and write through a pointer: `push_back` checks the capacity on every row.
    const size_t old_size = offsets.size();
    offsets.resize(old_size + rows);
    IColumn::Offset * out = offsets.data() + old_size;
    const UInt64 * in = sizes + start;
    for (size_t i = 0; i < rows; ++i)
    {
        offset += in[i];
        out[i] = static_cast<IColumn::Offset>(offset);
    }

    if (unlikely(offset > MAX_TOTAL_STRING_SIZE))
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Total size of String column is too large: the sizes stream declares more than {} bytes, "
            "most likely the data is corrupted",
            MAX_TOTAL_STRING_SIZE);
}

}

namespace
{

/// The data of the substream that the `.size` subcolumn is attached to in `WITH_SIZE_STREAM` and `WITH_DICTIONARIES`. The
/// column is created lazily from the parent column, which `WITH_DICTIONARIES` reads as a `ColumnReplicated`.
ISerialization::SubstreamData createSizesSubstreamData(MergeTreeStringSerializationVersion version, const ISerialization::SubstreamData & data)
{
    auto substream_data = ISerialization::SubstreamData(SerializationStringSize::create(version))
                              .withType(data.type ? std::make_shared<DataTypeUInt64>() : nullptr)
                              .withColumn(nullptr)
                              .withSerializationInfo(data.serialization_info);

    if (data.column)
    {
        substream_data.withLazyColumnCreator([parent_col = data.column]() -> ColumnPtr
        {
            if (const auto * replicated = typeid_cast<const ColumnReplicated *>(parent_col.get()))
            {
                const auto & values = assert_cast<const ColumnString &>(*replicated->getNestedColumn());
                return values.createSizeSubcolumn()->index(*replicated->getIndexesColumn(), 0);
            }
            return assert_cast<const ColumnString &>(*parent_col).createSizeSubcolumn();
        });
    }

    return substream_data;
}

}

void SerializationString::enumerateStreamsWithSize(
    EnumerateStreamsSettings & settings,
    const StreamCallback & callback,
    const SubstreamData & data) const
{
    /// Size stream. Same lazy pattern as in `enumerateStreamsWithoutSize`.
    settings.path.push_back(Substream::StringSizes);
    settings.path.back().data = createSizesSubstreamData(version, data);
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
    /// Over the network it carries cumulative offsets counted from the first written row, for exact reader preallocation.
    if (settings.position_independent_encoding)
        serializeStringSizes(column, *size_stream, offset, limit);
    else
        serializeStringOffsets(offsets, *size_stream, offset, limit);

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

void SerializationString::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings, DeserializeBinaryBulkStatePtr & state, SubstreamsDeserializeStatesCache * cache) const
{
    if (version == MergeTreeStringSerializationVersion::WITH_DICTIONARIES)
    {
        state = getStringGranulesDeserializeState(settings, cache);
        checkAndGetState<DeserializeBinaryBulkStateStringWithDictionaries>(state)->need_string_data = true;
        return;
    }

    /// WITH_SIZE_STREAM is stateless.
    if (version != MergeTreeStringSerializationVersion::SINGLE_STREAM)
        return;

    settings.path.push_back(Substream::Regular);
    if (auto cached_state = getFromSubstreamsDeserializeStatesCache(cache, settings.path))
    {
        state = cached_state;
        auto * string_state = checkAndGetState<DeserializeBinaryBulkStateStringWithoutSizeStream>(state);
        string_state->need_string_data = true;
    }
    else
    {
        auto string_state = std::make_shared<DeserializeBinaryBulkStateStringWithoutSizeStream>();
        string_state->need_string_data = true;
        state = string_state;
        addToSubstreamsDeserializeStatesCache(cache, settings.path, state);
    }
    settings.path.pop_back();
}

size_t SerializationString::deserializeStringOffsetsAndGetDataSize(
    ColumnString & column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    SubstreamsCache * cache) const
{
    auto & offsets = column.getOffsets();
    const size_t prev_last_offset = offsets.back();

    /// Over the network the stream carries cumulative byte offsets (the same layout Array uses):
    /// read them straight into the column's offsets for exact preallocation and a single bulk copy.
    if (!settings.position_independent_encoding)
    {
        ReadBuffer * offsets_stream = settings.getter(settings.path);
        if (!offsets_stream)
            return 0;

        const size_t prev_num_rows = offsets.size();
        SerializationNumber<ColumnString::Offset>::deserializeBinaryBulk(offsets, *offsets_stream, limit);

        /// The offsets come from untrusted input and address the characters of the column, so they have to
        /// increase monotonically; their total is checked by the caller. Everything below `prev_num_rows`
        /// was verified by the previous call, and starting one element earlier covers the pair across the
        /// boundary.
        auto * const scan_begin = offsets.begin() + (prev_num_rows ? prev_num_rows - 1 : 0);
        auto * const it = std::adjacent_find(scan_begin, offsets.end(), std::greater<>());
        if (it != offsets.end())
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "String offsets are not monotonically increasing (starting at {}, value {})",
                std::distance(offsets.begin(), it),
                *it);

        return offsets.back() - prev_last_offset;
    }

    /// On disk the stream carries per-row sizes; read them into a sizes column reused via the substreams
    /// cache (so the `.size` subcolumn reader can reuse it), then turn them into offsets.
    size_t num_read_rows = 0;
    ColumnPtr size_column;
    if (auto cached_column_with_num_read_rows = getColumnWithNumReadRowsFromSubstreamsCache(cache, settings.path))
    {
        std::tie(size_column, num_read_rows) = *cached_column_with_num_read_rows;
    }
    else if (ReadBuffer * size_stream = settings.getter(settings.path))
    {
        auto mutable_size_column = ColumnUInt64::create();
        SerializationNumber<UInt64>::create()->deserializeBinaryBulk(*mutable_size_column, *size_stream, limit, 0);
        num_read_rows = mutable_size_column->size();
        size_column = std::move(mutable_size_column);
        addColumnWithNumReadRowsToSubstreamsCache(cache, settings.path, size_column, num_read_rows);
    }
    else
    {
        return 0;
    }

    const auto & sizes_data = assert_cast<const ColumnUInt64 &>(*size_column).getData();
    const size_t prev_size = sizes_data.size() - num_read_rows;
    appendStringSizesToColumnStringOffsets(column, sizes_data.data(), prev_size, num_read_rows);
    return offsets.back() - prev_last_offset;
}

void SerializationString::deserializeBinaryBulkWithSizeStream(
    IColumn & column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    SubstreamsCache * cache) const
{
    /// Check if the whole String column is already deserialized and we have it in the cache.
    settings.path.push_back(Substream::Regular);

    if (insertDataFromSubstreamsCacheIfAny(cache, settings, column))
    {
        settings.path.pop_back();
        return;
    }

    auto & string_column = assert_cast<ColumnString &>(column);
    const size_t prev_num_rows = string_column.size();

    settings.path.back() = Substream::StringSizes;
    const size_t bytes_to_read = deserializeStringOffsetsAndGetDataSize(string_column, limit, settings, cache);

    settings.path.back() = Substream::Regular;
    auto * stream = settings.getter(settings.path);
    /// A null getter means the data substream is absent; the size stream above then read nothing
    /// either, so leave the column empty.
    if (!stream)
    {
        settings.path.pop_back();
        return;
    }

    if (bytes_to_read > MAX_TOTAL_STRING_SIZE)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Total size of String column is too large ({}): most likely the data is corrupted", bytes_to_read);

    auto & data = string_column.getChars();
    const size_t initial_size = data.size();

    if (settings.string_value_filter && settings.string_value_filter->isEnabled())
    {
        /// Check every value against the filter and read non-matching values as empty strings,
        /// rewriting the just-appended offsets accordingly. See the comment for `StringValueFilter`.
        deserializeBinaryFilteredWithKnownSizes(data, string_column.getOffsets(), *stream, prev_num_rows, *settings.string_value_filter);
    }
    else
    {
        data.resize(initial_size + bytes_to_read);
        stream->readBigStrict(reinterpret_cast<char*>(&data[initial_size]), bytes_to_read);
    }

    /// Nothing is shared across reads, so the rows read equal the growth of the offsets column.
    addColumnWithNumReadRowsToSubstreamsCache(cache, settings.path, column.getPtr(), string_column.size() - prev_num_rows);
    settings.path.pop_back();
}

void SerializationString::enumerateStreamsWithDictionaries(
    EnumerateStreamsSettings & settings,
    const StreamCallback & callback,
    const SubstreamData & data) const
{
    settings.path.push_back(Substream::StringDictionaryIndexes);
    callback(settings.path);

    /// The lengths of the rows of plain granules. The `.size` subcolumn is attached here, as in `WITH_SIZE_STREAM`, and
    /// its serialization reads the records and the lengths of the dictionaries as well.
    settings.path.back() = Substream::StringSizes;
    settings.path.back().data = createSizesSubstreamData(version, data);
    callback(settings.path);

    settings.path.back() = Substream::Regular;
    settings.path.back().data = data;
    callback(settings.path);

    settings.path.back() = Substream::StringDictionarySizes;
    callback(settings.path);

    settings.path.back() = Substream::StringDictionaryElements;
    callback(settings.path);
    settings.path.pop_back();
}

namespace
{

/// The streams of a column with dictionary granules, in the order of `enumerateStreams`.
struct StringGranuleOutputStreams
{
    WriteBuffer * indexes = nullptr;
    WriteBuffer * sizes = nullptr;
    WriteBuffer * data = nullptr;
    WriteBuffer * dictionary_sizes = nullptr;
    WriteBuffer * dictionary_data = nullptr;
};

/// Requests every stream, also those the call does not write: the writer learns the files of a column from the order
/// of these requests in a call without rows (`MergeTreeDataPartWriterOnDisk::initColumnsSubstreamsIfNeeded`).
StringGranuleOutputStreams getStringGranuleOutputStreams(ISerialization::SerializeBinaryBulkSettings & settings)
{
    using Substream = ISerialization::Substream;
    StringGranuleOutputStreams streams;
    settings.path.push_back(Substream::StringDictionaryIndexes);
    streams.indexes = settings.getter(settings.path);
    settings.path.back() = Substream::StringSizes;
    streams.sizes = settings.getter(settings.path);
    settings.path.back() = Substream::Regular;
    streams.data = settings.getter(settings.path);
    settings.path.back() = Substream::StringDictionarySizes;
    streams.dictionary_sizes = settings.getter(settings.path);
    settings.path.back() = Substream::StringDictionaryElements;
    streams.dictionary_data = settings.getter(settings.path);
    settings.path.pop_back();
    return streams;
}

void writeChars(const ColumnString & column, size_t offset, size_t rows, WriteBuffer & out)
{
    const auto & offsets = column.getOffsets();
    const size_t begin = offsets[static_cast<ssize_t>(offset) - 1];
    const size_t end = offsets[offset + rows - 1];
    out.write(reinterpret_cast<const char *>(column.getChars().data() + begin), end - begin);
}

/// Writes the lengths and the bytes of the rows of `slice` as a plain granule stores them.
void writePlainSlice(const StringGranuleDictionaryBuilder::Slice & slice, WriteBuffer & sizes_out, WriteBuffer & data_out)
{
    const auto * replicated = typeid_cast<const ColumnReplicated *>(slice.column.get());
    if (!replicated)
    {
        const auto & column = assert_cast<const ColumnString &>(*slice.column);
        serializeStringSizes(column, sizes_out, slice.offset, slice.rows);
        writeChars(column, slice.offset, slice.rows, data_out);
        return;
    }

    const auto & nested = assert_cast<const ColumnString &>(*replicated->getNestedColumn());
    PaddedPODArray<UInt64> sizes(slice.rows);
    replicated->getIndexes().callForIndexesData([&](const auto & indexes)
    {
        for (size_t i = 0; i < slice.rows; ++i)
            sizes[i] = nested.getDataAt(indexes[slice.offset + i]).size();
        writeStringSizes(sizes.data(), sizes.size(), sizes_out);
        for (size_t i = 0; i < slice.rows; ++i)
        {
            const std::string_view value = nested.getDataAt(indexes[slice.offset + i]);
            data_out.write(value.data(), value.size());
        }
    });
}

/// Writes the granule whose rows `state` holds, if any, as a dictionary or plain.
void writeStringGranule(const StringGranuleOutputStreams & streams, SerializeBinaryBulkStateStringWithDictionaries & state)
{
    if (state.rows == 0)
        return;

    StringGranuleHeader header{.kind = StringGranuleHeader::Kind::Plain, .rows = state.rows};
    auto & builder = state.builder;
    if (builder.build(state.slices, state.rows, state.settings))
    {
        const auto & dictionary = builder.getDictionary();
        header.kind = StringGranuleHeader::Kind::Dictionary;
        header.dictionary_size = dictionary.size();
        header.code_bits = builder.getCodeBits();
        header.write(*streams.indexes);
        builder.writeCodes(*streams.indexes);
        serializeStringSizes(dictionary, *streams.dictionary_sizes, 0, dictionary.size());
        writeChars(dictionary, 0, dictionary.size(), *streams.dictionary_data);
    }
    else
    {
        header.write(*streams.indexes);
        for (const auto & slice : state.slices)
            writePlainSlice(slice, *streams.sizes, *streams.data);
    }

    state.slices.clear();
    state.rows = 0;
}

}

void SerializationString::serializeBinaryBulkStatePrefix(
    const IColumn & /*column*/, SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const
{
    if (version != MergeTreeStringSerializationVersion::WITH_DICTIONARIES)
        return;

    if (settings.data_part_type != MergeTreeDataPartType::Wide)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "String granules with dictionaries can be written only to Wide parts");

    auto granules_state = std::make_shared<SerializeBinaryBulkStateStringWithDictionaries>();
    granules_state->settings.min_size_reduction = settings.string_dictionary_min_size_reduction;
    granules_state->settings.max_distinct_ratio = settings.string_dictionary_max_distinct_ratio;
    state = std::move(granules_state);
}

void SerializationString::serializeBinaryBulkStateSuffix(SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const
{
    if (version != MergeTreeStringSerializationVersion::WITH_DICTIONARIES)
        return;

    /// Rows left belong to the last granule of the part, which the writer completes without a data call.
    const auto streams = getStringGranuleOutputStreams(settings);
    writeStringGranule(streams, *checkAndGetState<SerializeBinaryBulkStateStringWithDictionaries>(state));
}

void SerializationString::serializeBinaryBulkWithDictionaries(
    const IColumn & column,
    size_t offset,
    size_t limit,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    auto & granules_state = *checkAndGetState<SerializeBinaryBulkStateStringWithDictionaries>(state);
    const auto streams = getStringGranuleOutputStreams(settings);

    if (const size_t size = column.size(); limit == 0 || offset + limit > size)
        limit = size - offset;

    /// Nothing is written before the granule is complete, so the marks that the writer takes at the start of the granule
    /// point to where its data begins in every stream.
    if (limit != 0)
    {
        granules_state.slices.push_back({.column = column.getPtr(), .offset = offset, .rows = limit});
        granules_state.rows += limit;
    }

    if (settings.granule_is_complete)
        writeStringGranule(streams, granules_state);
}

SerializationPtr SerializationString::resolveForEmptySubstreams(const SubstreamIsEmptyCallback & substream_is_empty) const
{
    /// Every dictionary granule writes at least one dictionary value, so an empty stream of their lengths means that all
    /// granules are plain: the streams `StringSizes` and `Regular` then hold the column as `WITH_SIZE_STREAM` writes it.
    SubstreamPath dictionary_sizes;
    dictionary_sizes.push_back(Substream::StringDictionarySizes);
    if (version == MergeTreeStringSerializationVersion::WITH_DICTIONARIES && substream_is_empty(dictionary_sizes))
        return SerializationString::create(MergeTreeStringSerializationVersion::WITH_SIZE_STREAM);
    return getPtr();
}

MutableColumnPtr SerializationString::createColumnForDeserialization(const IDataType & type) const
{
    if (version == MergeTreeStringSerializationVersion::WITH_DICTIONARIES)
        return ColumnReplicated::create(type.createColumn(), ColumnUInt8::create());
    return type.createColumn();
}

namespace
{

/// Reads up to `limit` strings into `column`: their lengths (`UInt64` each) from `sizes_stream` and their bytes from
/// `data_stream`. Values that do not match `filter` are read as empty strings. Returns the number of strings read,
/// which is less than `limit` only at the end of the streams.
size_t readStringsWithSizes(
    ColumnString & column,
    size_t limit,
    ReadBuffer & sizes_stream,
    ReadBuffer & data_stream,
    PaddedPODArray<UInt64> & sizes,
    const StringValueFilter * filter)
{
    sizes.clear();
    SerializationNumber<UInt64>::deserializeBinaryBulk(sizes, sizes_stream, limit);

    const size_t prev_num_rows = column.size();
    auto & chars = column.getChars();
    const size_t prev_bytes = chars.size();
    appendStringSizesToColumnStringOffsets(column, sizes.data(), 0, sizes.size());

    if (filter && filter->isEnabled())
    {
        deserializeBinaryFilteredWithKnownSizes(chars, column.getOffsets(), data_stream, prev_num_rows, *filter);
    }
    else
    {
        const size_t bytes = column.getOffsets().back() - prev_bytes;
        chars.resize(prev_bytes + bytes);
        data_stream.readBigStrict(reinterpret_cast<char *>(&chars[prev_bytes]), bytes);
    }
    return sizes.size();
}

/// Appends the values of `dictionary` at the codes of rows `[start, start + length)` to `column`.
void insertDictionaryValues(ColumnString & column, const ColumnString & dictionary, const IColumn & codes, size_t start, size_t length)
{
    ColumnIndex::callForIndexesData(codes, [&](const auto & data)
    {
        for (size_t row = start; row < start + length; ++row)
            column.insertFrom(dictionary, data[row]);
    });
}

/// The streams of a column with dictionary granules, as the reader requests them.
struct StringGranuleInputStreams
{
    ReadBuffer * indexes = nullptr;
    ReadBuffer * sizes = nullptr;
    ReadBuffer * data = nullptr;
    ReadBuffer * dictionary_sizes = nullptr;
    ReadBuffer * dictionary_data = nullptr;
};

/// Reads up to `limit` rows into `column`, a `ColumnReplicated` over a `ColumnString`.
///
/// The records and codes of the granules come first. Then the dictionaries of the granules that start in this
/// call are read with one request to each of their streams, and so are the rows of the plain granules, so
/// that whole compressed blocks decompress straight into the nested column instead of going through the
/// buffer of the stream. The nested column gets the dictionaries first and the rows of plain granules after
/// them, and the indexes of the rows come last, in the order of the rows. A granule that the call does not
/// finish leaves its codes and a copy of its dictionary in the state for the next call.
void readGranulesIntoReplicated(
    ColumnReplicated & column,
    size_t limit,
    const StringGranuleInputStreams & streams,
    DeserializeBinaryBulkStateStringWithDictionaries & state,
    const StringValueFilter * filter)
{
    auto & nested = assert_cast<ColumnString &>(*column.getNestedColumn());

    struct Dictionary
    {
        size_t size = 0;
        MutableColumnPtr codes;
        size_t position = 0;
    };

    /// The rows of one granule that the call reads.
    struct Piece
    {
        size_t rows = 0;
        /// For a dictionary granule: its index in `dictionaries` and the row of the granule the piece starts
        /// at.
        std::optional<size_t> dictionary;
        size_t first_row = 0;
    };

    std::vector<Dictionary> dictionaries;
    std::vector<Piece> pieces;

    /// A dictionary granule that an earlier call began: its values, kept in the state, come first. The state
    /// forgets them when the next granule starts, so the call holds them itself.
    const bool continues_dictionary = state.rows_left_in_granule != 0 && state.granule.kind == StringGranuleHeader::Kind::Dictionary;
    const ColumnPtr kept_dictionary = continues_dictionary ? state.dictionary : nullptr;
    if (continues_dictionary)
        dictionaries.push_back({.size = state.granule.dictionary_size, .codes = std::move(state.granule_codes), .position = 0});

    size_t new_dictionary_values = 0;
    size_t plain_rows = 0;
    size_t rows_read = 0;
    while (rows_read < limit)
    {
        if (state.rows_left_in_granule == 0)
        {
            if (!state.startGranule(*streams.indexes))
                break;
            if (state.granule.kind == StringGranuleHeader::Kind::Dictionary)
            {
                dictionaries.push_back({.size = state.granule.dictionary_size, .codes = std::move(state.granule_codes), .position = 0});
                new_dictionary_values += state.granule.dictionary_size;
            }
        }

        const size_t rows = std::min(limit - rows_read, state.rows_left_in_granule);
        Piece piece{.rows = rows, .dictionary = std::nullopt, .first_row = 0};
        if (state.granule.kind == StringGranuleHeader::Kind::Dictionary)
        {
            piece.dictionary = dictionaries.size() - 1;
            piece.first_row = state.firstUnreadRow();
        }
        else
        {
            plain_rows += rows;
        }
        pieces.push_back(piece);

        state.rows_left_in_granule -= rows;
        rows_read += rows;
    }

    PaddedPODArray<UInt64> sizes;
    size_t position = nested.size();
    if (continues_dictionary)
    {
        dictionaries[0].position = position;
        nested.insertRangeFrom(*kept_dictionary, 0, dictionaries[0].size);
        position += dictionaries[0].size;
    }

    if (new_dictionary_values != 0)
    {
        if (readStringsWithSizes(nested, new_dictionary_values, *streams.dictionary_sizes, *streams.dictionary_data, sizes, filter)
            != new_dictionary_values)
            throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Cannot read the dictionaries of String granules");
        for (size_t i = continues_dictionary ? 1 : 0; i < dictionaries.size(); ++i)
        {
            dictionaries[i].position = position;
            position += dictionaries[i].size;
        }
    }

    size_t plain_position = position;
    if (plain_rows != 0 && readStringsWithSizes(nested, plain_rows, *streams.sizes, *streams.data, sizes, filter) != plain_rows)
        throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Cannot read {} rows of plain String granules", plain_rows);

    auto & indexes = column.getIndexes();
    indexes.reserve(indexes.size() + rows_read);
    for (const auto & piece : pieces)
    {
        if (!piece.dictionary)
        {
            indexes.insertIndexesRange(plain_position, piece.rows);
            plain_position += piece.rows;
            continue;
        }

        const auto & dictionary = dictionaries[*piece.dictionary];
        indexes.insertIndexesRangeWithShift(
            *dictionary.codes, piece.first_row, piece.rows, dictionary.position, dictionary.position + dictionary.size);
    }

    /// A call that ends inside a dictionary granule leaves its codes, and the values of a granule that began
    /// in this call, in the state for the next call.
    if (state.rows_left_in_granule != 0 && state.granule.kind == StringGranuleHeader::Kind::Dictionary)
    {
        auto & last = dictionaries.back();
        const bool last_began_in_this_call = !continues_dictionary || dictionaries.size() > 1;
        if (last_began_in_this_call)
            state.dictionary = nested.cut(last.position, last.size);
        state.granule_codes = std::move(last.codes);
    }

    for (auto & dictionary : dictionaries)
        if (dictionary.codes)
            state.recycleCodes(std::move(dictionary.codes));
}

/// Reads up to `limit` rows into `column`, a `ColumnString`, with the rows of dictionary granules
/// materialized.
void readGranulesMaterialized(
    ColumnString & column,
    size_t limit,
    const StringGranuleInputStreams & streams,
    DeserializeBinaryBulkStateStringWithDictionaries & state,
    const StringValueFilter * filter)
{
    PaddedPODArray<UInt64> sizes;
    size_t rows_read = 0;
    while (rows_read < limit)
    {
        if (state.rows_left_in_granule == 0)
        {
            if (!state.startGranule(*streams.indexes))
                break;
            if (state.granule.kind == StringGranuleHeader::Kind::Dictionary)
            {
                auto dictionary = ColumnString::create();
                if (readStringsWithSizes(*dictionary, state.granule.dictionary_size, *streams.dictionary_sizes, *streams.dictionary_data, sizes, filter)
                    != state.granule.dictionary_size)
                    throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Cannot read the dictionary of a String granule");
                state.dictionary = std::move(dictionary);
            }
        }

        const size_t rows = std::min(limit - rows_read, state.rows_left_in_granule);
        if (state.granule.kind == StringGranuleHeader::Kind::Plain)
        {
            if (readStringsWithSizes(column, rows, *streams.sizes, *streams.data, sizes, filter) != rows)
                throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Cannot read {} rows of a plain String granule", rows);
        }
        else
        {
            insertDictionaryValues(
                column, assert_cast<const ColumnString &>(*state.dictionary), *state.granule_codes, state.firstUnreadRow(), rows);
        }

        state.rows_left_in_granule -= rows;
        rows_read += rows;
    }
}

}

void SerializationString::deserializeBinaryBulkWithDictionaries(
    IColumn & column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    settings.path.push_back(Substream::Regular);
    if (insertDataFromSubstreamsCacheIfAny(cache, settings, column))
    {
        settings.path.pop_back();
        return;
    }

    /// Each stream is requested once per call: in a read that does not continue the previous one, the getter seeks the
    /// stream to the mark on every request, and every stream has to be sought then, also those this call does not read.
    StringGranuleInputStreams streams;
    streams.data = settings.getter(settings.path);
    settings.path.back() = Substream::StringDictionaryIndexes;
    streams.indexes = settings.getter(settings.path);
    settings.path.back() = Substream::StringSizes;
    streams.sizes = settings.getter(settings.path);
    settings.path.back() = Substream::StringDictionarySizes;
    streams.dictionary_sizes = settings.getter(settings.path);
    settings.path.back() = Substream::StringDictionaryElements;
    streams.dictionary_data = settings.getter(settings.path);
    settings.path.pop_back();

    /// The column is not in the part.
    if (!streams.indexes)
        return;

    auto & granules_state = *checkAndGetState<DeserializeBinaryBulkStateStringWithDictionaries>(state);
    /// The streams were sought to a mark, which is the start of a granule.
    if (!settings.continuous_reading)
        granules_state.rows_left_in_granule = 0;

    const StringValueFilter * filter = settings.string_value_filter.get();
    const size_t prev_size = column.size();

    /// A caller that creates the column from the type instead of `createColumnForDeserialization` gets the
    /// rows of dictionary granules materialized.
    if (auto * column_replicated = typeid_cast<ColumnReplicated *>(&column))
        readGranulesIntoReplicated(*column_replicated, limit, streams, granules_state, filter);
    else
        readGranulesMaterialized(assert_cast<ColumnString &>(column), limit, streams, granules_state, filter);

    settings.path.push_back(Substream::Regular);
    addColumnWithNumReadRowsToSubstreamsCache(cache, settings.path, column.getPtr(), column.size() - prev_size);
    settings.path.pop_back();
}

}
