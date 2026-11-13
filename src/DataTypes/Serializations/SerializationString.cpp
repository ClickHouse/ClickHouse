#include <algorithm>
#include <bit>

#include <Common/SipHash.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/Serializations/SerializationString.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Compression/CompressedReadBufferFromFile.h>
#include <Compression/CompressionCodecFSST.h>
#include <Compression/FSST.h>
#include <IO/WriteBufferWithValueBoundaries.h>
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
#include <base/unaligned.h>
#include <Common/StringValueFilter.h>
#include <Common/assert_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_ALL_DATA;
    extern const int INCORRECT_DATA;
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

namespace
{

/** A scan of the codes of an aligned block encoded by `FSST` (`CompressionCodecFSST::Format::ENCODED_ALIGNED`) for the
  * values that can contain the needles of the `Substring` and `Suffix` conditions of `StringValueFilter`: they must
  * contain every bigram (pair of adjacent bytes) of the needles, or the byte of a needle of one byte. The gram that occurs
  * the least in a sample of the codes of the block is chosen for every block. (Checking two grams makes fewer candidates,
  * but the scan and the bookkeeping of the found codes cost more than decoding the extra candidates.)
  *
  * A bigram is either inside a symbol, or consists of the last byte of a symbol and the first byte of the next one,
  * so every code is mapped to a class: whether its symbol contains the gram, ends with the first byte of the gram,
  * and starts with the second byte of the gram. The whole block is scanned in one pass,
  * 8 codes at a time, which has no other dependency between the codes than the class of the previous code, and the found
  * codes are mapped to the values by the index of the values. Unlike checking the values one by one, there is no work
  * per value, and the scan is faster than decoding the codes.
  *
  * An escaped byte is classified as a symbol of one byte. The escape code is rare, so it is a branch, which does not make
  * the position of the next code depend on the value of the current one.
  */
class FSSTBlockScanner
{
public:
    explicit FSSTBlockScanner(const StringValueFilter & filter)
    {
        using Type = StringValueFilter::Condition::Type;
        for (const auto & condition : filter.getConditions())
        {
            if (condition.type != Type::Substring && condition.type != Type::Suffix)
                continue;
            const auto & needle = condition.needle;
            if (needle.size() == 1)
                addGram({static_cast<UInt8>(needle[0]), 0, false});
            for (size_t i = 0; i + 1 < needle.size(); ++i)
                addGram({static_cast<UInt8>(needle[i]), static_cast<UInt8>(needle[i + 1]), true});
        }
    }

    bool empty() const { return grams.empty(); }

    /// Chooses the gram that occurs the least in a sample of the codes of the block (from its beginning),
    /// and builds the classes of the codes for it.
    void build(const FSST::SymbolTable & table, const UInt8 * codes, size_t codes_size)
    {
        /// The masks of all grams: the bigrams that start and end with a byte, and the grams of one byte.
        std::array<UInt32, 256> first_is{};
        std::array<UInt32, 256> second_is{};
        std::array<UInt32, 256> single_is{};
        for (size_t i = 0; i < grams.size(); ++i)
        {
            if (grams[i].is_bigram)
            {
                first_is[grams[i].first] |= 1u << i;
                second_is[grams[i].second] |= 1u << i;
            }
            else
            {
                single_is[grams[i].first] |= 1u << i;
            }
        }

        /// For every code: the grams inside its symbol, that its last byte starts, and that its first byte ends.
        std::array<GramMasks, 256> code_masks{};
        for (size_t code = 0; code < table.num_symbols; ++code)
        {
            const size_t length = table.lengths[code];
            GramMasks & masks = code_masks[code];
            for (size_t i = 0; i < length; ++i)
            {
                masks.inside |= single_is[symbolByte(table, code, i)];
                if (i + 1 < length)
                    masks.inside |= first_is[symbolByte(table, code, i)] & second_is[symbolByte(table, code, i + 1)];
            }
            masks.ends_with_first = first_is[symbolByte(table, code, length - 1)];
            masks.starts_with_second = second_is[symbolByte(table, code, 0)];
        }
        std::array<GramMasks, 256> literal_masks{};
        for (size_t byte = 0; byte < 256; ++byte)
            literal_masks[byte] = {single_is[byte], first_is[byte], second_is[byte]};

        /// The number of occurrences of every gram in a sample of the codes.
        static constexpr size_t SAMPLE_SIZE = 1024;
        const size_t sample_size = std::min(codes_size, SAMPLE_SIZE);
        std::array<UInt32, MAX_GRAMS> counts{};
        UInt32 prev_ends = 0;
        for (size_t pos = 0; pos + 1 < sample_size; ++pos)
        {
            const GramMasks & masks = codes[pos] == FSST::ESCAPE_CODE ? literal_masks[codes[++pos]] : code_masks[codes[pos]];
            UInt32 found = masks.inside | (prev_ends & masks.starts_with_second);
            prev_ends = masks.ends_with_first;
            while (found)
            {
                ++counts[std::countr_zero(found)];
                found &= found - 1;
            }
        }

        size_t chosen = 0;
        for (size_t i = 1; i < grams.size(); ++i)
            if (counts[i] < counts[chosen])
                chosen = i;

        const UInt32 bit = 1u << chosen;
        for (size_t code = 0; code < 256; ++code)
            classes[code] = classOf(code_masks[code], bit);
        for (size_t byte = 0; byte < 256; ++byte)
            literal_classes[byte] = classOf(literal_masks[byte], bit);
    }

    /// Appends the positions of the codes in `[begin, end)` that can be (the end of) an occurrence of the chosen gram.
    /// `begin` is a boundary of the values, so a bigram with the previous code is not in the same value.
    void findCandidateCodes(const CompressionCodecFSST::EncodedBlock & block, size_t begin, size_t end, std::vector<UInt32> & result) const
    {
        const UInt8 * codes = block.codes;
        /// The class of the previous code.
        UInt64 prev = 0;
        size_t pos = begin;

        /// One code, or an escaped byte.
        auto scalar_step = [&]
        {
            const size_t code_pos = pos;
            UInt64 cls;
            if (codes[pos] == FSST::ESCAPE_CODE)
            {
                ++pos;
                if (pos >= end)
                    throw Exception(block.error_code, "Cannot decode FSST data: the escape code at the end of the data");
                cls = literal_classes[codes[pos]];
            }
            else
            {
                cls = classes[codes[pos]];
            }
            ++pos;

            if (foundBits(cls, prev) & 1)
                result.push_back(static_cast<UInt32>(code_pos));
            prev = cls;
        };

        /// 8 codes at once, if none of them is the escape code: their classes are packed into the bytes of a word,
        /// and the conditions are checked for all of them by a few operations on it.
        static constexpr UInt64 ONES = 0x0101010101010101ULL;
        while (end - pos >= 8)
        {
            const UInt64 word = unalignedLoadLittleEndian<UInt64>(codes + pos);
            const UInt64 inverted = ~word;
            if (unlikely(((inverted - ONES) & word & (ONES << 7)) != 0))
            {
                /// There is a byte 0xFF (the escape code): process these codes one by one.
                const size_t chunk_end = pos + 8;
                while (pos < chunk_end)
                    scalar_step();
                continue;
            }

            UInt64 packed = 0;
            for (size_t i = 0; i < 8; ++i)
                packed |= UInt64(classes[(word >> (8 * i)) & 0xFF]) << (8 * i);

            /// The class of the previous code for every code.
            const UInt64 prev_packed = (packed << 8) | prev;
            const UInt64 found = foundBits(packed, prev_packed) & ONES;
            if (unlikely(found != 0))
            {
                for (size_t i = 0; i < 8; ++i)
                    if ((found >> (8 * i)) & 1)
                        result.push_back(static_cast<UInt32>(pos + i));
            }

            prev = packed >> 56;
            pos += 8;
        }

        while (pos < end)
            scalar_step();
    }

private:
    struct Gram
    {
        UInt8 first;
        UInt8 second;
        bool is_bigram;

        bool operator==(const Gram &) const = default;
    };

    struct GramMasks
    {
        UInt32 inside = 0;
        UInt32 ends_with_first = 0;
        UInt32 starts_with_second = 0;
    };

    /// The masks of the grams are 32-bit, so more grams are not considered: it is a necessary condition anyway.
    static constexpr size_t MAX_GRAMS = 32;

    void addGram(const Gram & gram)
    {
        if (grams.size() < MAX_GRAMS && std::find(grams.begin(), grams.end(), gram) == grams.end())
            grams.push_back(gram);
    }

    /// The class bits for one gram: whether the symbol contains it, ends with its first byte, starts with its second byte.
    static constexpr UInt8 INSIDE = 1;
    static constexpr UInt8 ENDS_WITH_FIRST = 2;
    static constexpr UInt8 STARTS_WITH_SECOND = 4;

    static UInt8 classOf(const GramMasks & masks, UInt32 bit)
    {
        return static_cast<UInt8>(
            ((masks.inside & bit) ? INSIDE : 0) | ((masks.ends_with_first & bit) ? ENDS_WITH_FIRST : 0)
            | ((masks.starts_with_second & bit) ? STARTS_WITH_SECOND : 0));
    }

    /// For the classes of codes (one per byte) and the classes of the codes before them: bit 0 of a byte is set
    /// if the gram is found at the code. Other bits are garbage.
    static UInt64 foundBits(UInt64 cls, UInt64 prev_cls)
    {
        return cls | ((prev_cls >> 1) & (cls >> 2));
    }

    static UInt8 symbolByte(const FSST::SymbolTable & table, size_t code, size_t i)
    {
        return static_cast<UInt8>(table.symbols[code] >> (8 * i));
    }

    std::vector<Gram> grams;
    std::array<UInt8, 256> classes{};
    /// The same for the escaped bytes.
    std::array<UInt8, 256> literal_classes{};
};

/** The conditions of `StringValueFilter` that are checked exactly on the codes of a block encoded by `FSST` with the
  * boundaries of the values (`CompressionCodecFSST::Format::ENCODED_ALIGNED`). Every value that starts in such a block
  * is encoded on its own, exactly as `FSST::encodeValue` encodes it, so:
  * - `Equals`: the value is equal to the needle if and only if its codes are equal to the codes of the needle;
  * - `Prefix`: the encoder chooses a symbol by the next 8 bytes, so the codes of the needle that start at least 8 bytes
  *   before its end are the same in every value that starts with it. They are compared without decoding, and only the
  *   few bytes after them are decoded and compared with the rest of the needle.
  */
class FSSTBlockExactConditions
{
public:
    explicit FSSTBlockExactConditions(const StringValueFilter & filter)
    {
        for (const auto & condition : filter.getConditions())
        {
            if (condition.type == StringValueFilter::Condition::Type::Equals || condition.type == StringValueFilter::Condition::Type::Prefix)
                conditions.push_back({&condition, {}, 0, 0});
            else
                all_conditions_are_exact = false;
        }
    }

    bool empty() const { return conditions.empty(); }

    /// Whether checking these conditions decides the whole filter.
    bool decideFilter() const { return all_conditions_are_exact; }

    void build(const FSST::SymbolTable & table)
    {
        for (auto & condition : conditions)
        {
            const auto & needle = condition.condition->needle;
            condition.codes.resize(FSST::maxEncodedSize(needle.size()));
            condition.codes.resize(FSST::encodeValue(table, needle.data(), needle.size(), condition.codes.data()));

            if (condition.condition->type == StringValueFilter::Condition::Type::Prefix)
            {
                /// The codes that start at least `MAX_SYMBOL_LENGTH` bytes before the end of the needle.
                size_t num_codes = 0;
                size_t num_bytes = 0;
                while (num_codes < condition.codes.size() && num_bytes + FSST::MAX_SYMBOL_LENGTH <= needle.size())
                {
                    const UInt8 code = condition.codes[num_codes];
                    num_bytes += code == FSST::ESCAPE_CODE ? 1 : table.lengths[code];
                    num_codes += code == FSST::ESCAPE_CODE ? 2 : 1;
                }
                condition.num_prefix_codes = num_codes;
                condition.num_prefix_bytes = num_bytes;
            }
        }
    }

    /// Checks the value of `size` bytes, whose codes are `[begin, end)` (from the index of the values, within the codes).
    /// The codes that are compared are not decoded, so they are not validated: a malformed block can only make
    /// the comparison fail. The decoded part is validated by `FSST::decode`.
    bool match(const CompressionCodecFSST::EncodedBlock & block, size_t begin, size_t end, size_t size) const
    {
        const UInt8 * codes = block.codes;
        for (const auto & condition : conditions)
        {
            const auto & needle = condition.condition->needle;
            if (condition.condition->type == StringValueFilter::Condition::Type::Equals)
            {
                if (size != needle.size() || end - begin != condition.codes.size()
                    || memcmp(codes + begin, condition.codes.data(), condition.codes.size()) != 0)
                    return false;
                continue;
            }

            if (size < needle.size() || end - begin < condition.num_prefix_codes
                || memcmp(codes + begin, condition.codes.data(), condition.num_prefix_codes) != 0)
                return false;

            /// The rest of the needle is shorter than `2 * MAX_SYMBOL_LENGTH`.
            const size_t rest_size = needle.size() - condition.num_prefix_bytes;
            char rest[2 * FSST::MAX_SYMBOL_LENGTH];
            FSST::Cursor cursor{begin + condition.num_prefix_codes, 0};
            FSST::decode(block.table, codes, block.codes_size, cursor, rest, rest_size, block.error_code);
            if (memcmp(rest, needle.data() + condition.num_prefix_bytes, rest_size) != 0)
                return false;
        }
        return true;
    }

private:
    struct Condition
    {
        const StringValueFilter::Condition * condition;
        /// The codes of the needle in the current block.
        std::vector<UInt8> codes;
        /// For `Prefix`: the number of the codes of the needle that are compared, and of the bytes they encode.
        size_t num_prefix_codes;
        size_t num_prefix_bytes;
    };

    std::vector<Condition> conditions;
    bool all_conditions_are_exact = true;
};

}

/// The same as `deserializeBinaryFilteredWithKnownSizes`, for a data stream compressed by the `FSST` codec, with the
/// boundaries of the values (`CompressionCodecFSST::Format::ENCODED_ALIGNED`): the values are checked against the filter
/// on the encoded data, exactly for `Equals` and `Prefix` (see `FSSTBlockExactConditions`), by a necessary condition for
/// `Substring` and `Suffix` (see `FSSTBlockScanner`), and only the values that may match are decoded and checked against
/// the filter. As soon as the next block is not such a block (e.g. a block stored as is, or encoded without the boundaries
/// of the values), the rest of the values are read by `deserializeBinaryFilteredWithKnownSizes`.
static NO_INLINE void deserializeBinaryFilteredFromFSST(
    ColumnString::Chars & data,
    ColumnString::Offsets & offsets,
    CompressedReadBufferFromFile & istr,
    size_t first_row,
    const StringValueFilter & filter)
{
    const size_t num_rows = offsets.size();
    size_t offset = data.size();
    /// The original (unfiltered) cumulative offset of the current stream position, as in `deserializeBinaryFilteredWithKnownSizes`.
    size_t original_pos = offset;

    size_t values_checked = 0;
    size_t values_replaced = 0;
    size_t bytes_skipped = 0;
    /// The values that may match are decoded and checked one by one.
    size_t values_checked_individually = 0;

    FSSTBlockExactConditions exact_conditions(filter);
    FSSTBlockScanner scanner(filter);
    std::vector<UInt32> candidate_codes;

    /// The values that are too short for a needle (or not of its size, for `Equals`) are rejected by their size.
    size_t min_size = 0;
    std::optional<size_t> exact_size;
    for (const auto & condition : filter.getConditions())
    {
        min_size = std::max(min_size, condition.needle.size());
        if (condition.type == StringValueFilter::Condition::Type::Equals)
            exact_size = condition.needle.size();
    }

    auto reserve = [&](size_t size)
    {
        if (unlikely(offset + size > data.size()))
            data.resize_exact(roundUpToPowerOfTwoOrZero(std::max(offset + size, data.size() * 2)));
    };

    /// Accounts the value at `data[offset]`, which is decoded entirely, and checks it against the filter.
    auto check_decoded_value = [&](size_t value_size)
    {
        ++values_checked_individually;
        if (filter.match(reinterpret_cast<const char *>(&data[offset]), value_size))
        {
            offset += value_size;
        }
        else
        {
            ++values_replaced;
            bytes_skipped += value_size;
        }
    };

    auto throw_not_aligned = []
    {
        throw Exception(ErrorCodes::INCORRECT_DATA, "A value is not aligned with the codes in an aligned block encoded by FSST");
    };

    size_t i = first_row;
    try
    {
        while (i < num_rows)
        {
            const FSSTBlock * block = istr.readFSSTBlock();
            /// A block without the boundaries of the values stays pending, and it is decoded and read as usual.
            if (!block || !block->encoded.aligned)
                break;

            const auto & encoded = block->encoded;
            const auto & table = encoded.table;
            if (!exact_conditions.empty())
                exact_conditions.build(table);

            FSST::Cursor cursor = block->start_cursor;
            size_t available = block->decoded_size - block->start_offset;

            /// The index gives the end of every value that ends in the block, except the last one, which ends with the codes:
            /// the next boundary after the current position is the end of the current value.
            const auto & boundaries = encoded.boundary_code_positions;
            size_t next_boundary = std::upper_bound(boundaries.begin(), boundaries.end(), cursor.code_pos) - boundaries.begin();

            /// The codes that can be in an occurrence of a needle, and whether a value has one of them (for the values in order).
            candidate_codes.clear();
            size_t next_candidate_code = 0;
            if (!scanner.empty())
            {
                scanner.build(table, encoded.codes, encoded.codes_size);
                scanner.findCandidateCodes(encoded, cursor.code_pos, encoded.codes_size, candidate_codes);
            }
            auto has_candidate_code = [&](size_t begin, size_t end)
            {
                while (next_candidate_code < candidate_codes.size() && candidate_codes[next_candidate_code] < begin)
                    ++next_candidate_code;
                return next_candidate_code < candidate_codes.size() && candidate_codes[next_candidate_code] < end;
            };

            /// The values that end in this block.
            while (i < num_rows)
            {
                const size_t value_size = offsets[i] - original_pos;
                if (value_size > available)
                    break;

                ++values_checked;

                /// The codes of the value: `[begin, end)`.
                const size_t begin = cursor.code_pos;
                size_t end = begin;
                if (value_size)
                    end = next_boundary < boundaries.size() ? boundaries[next_boundary++] : encoded.codes_size;
                /// A value that starts in an aligned block starts at a code, and its codes are in the block.
                if (cursor.offset_in_symbol != 0 || end < begin)
                    throw_not_aligned();
                cursor = {end, 0};

                /// Whether the value matches the filter, does not match, or has to be decoded and checked against the filter.
                enum class Decision : UInt8
                {
                    REJECT,
                    ACCEPT,
                    CHECK,
                };
                Decision decision = Decision::CHECK;

                /// An empty string never matches the filter.
                if (value_size == 0 || value_size < min_size || (exact_size && value_size != *exact_size))
                    decision = Decision::REJECT;
                else if (!exact_conditions.empty() && !exact_conditions.match(encoded, begin, end, value_size))
                    decision = Decision::REJECT;
                else if (exact_conditions.decideFilter())
                    decision = Decision::ACCEPT;
                else if (!has_candidate_code(begin, end))
                    decision = Decision::REJECT;

                if (decision == Decision::REJECT)
                {
                    ++values_replaced;
                    bytes_skipped += value_size;
                }
                else
                {
                    reserve(value_size);
                    FSST::Cursor decode_cursor{begin, 0};
                    FSST::decode(table, encoded.codes, encoded.codes_size, decode_cursor, reinterpret_cast<char *>(&data[offset]), value_size, encoded.error_code);
                    if (decode_cursor.code_pos != end || decode_cursor.offset_in_symbol != 0)
                        throw_not_aligned();
                    if (decision == Decision::ACCEPT)
                        offset += value_size;
                    else
                        check_decoded_value(value_size);
                }

                available -= value_size;
                original_pos += value_size;
                offsets[i] = offset;
                ++i;
            }

            if (i == num_rows || available == 0)
            {
                istr.finishFSSTBlock(block->decoded_size - available, cursor);
                continue;
            }

            /// The value continues in the next blocks: decode it entirely and check it as usual.
            const size_t value_size = offsets[i] - original_pos;
            reserve(value_size);
            char * value_data = reinterpret_cast<char *>(&data[offset]);
            FSST::decode(table, encoded.codes, encoded.codes_size, cursor, value_data, available, encoded.error_code);
            istr.finishFSSTBlock(block->decoded_size, cursor);

            size_t value_bytes_read = available;
            while (value_bytes_read < value_size)
            {
                const FSSTBlock * next_block = istr.readFSSTBlock();
                if (!next_block)
                {
                    istr.readStrict(value_data + value_bytes_read, value_size - value_bytes_read);
                    break;
                }

                const size_t n = std::min(value_size - value_bytes_read, next_block->decoded_size - next_block->start_offset);
                FSST::Cursor next_cursor = next_block->start_cursor;
                const auto & next_encoded = next_block->encoded;
                FSST::decode(next_encoded.table, next_encoded.codes, next_encoded.codes_size, next_cursor, value_data + value_bytes_read, n, next_encoded.error_code);
                istr.finishFSSTBlock(next_block->start_offset + n, next_cursor);
                value_bytes_read += n;
            }

            ++values_checked;
            check_decoded_value(value_size);
            original_pos += value_size;
            offsets[i] = offset;
            ++i;
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
    /// The rejected values are not decoded, which costs less than decoding them, so filtering them pays off even when
    /// they are short. The values checked one by one participate in the minimum average size of skipped values.
    filter.updateStatsOfBulkScan(values_checked, values_replaced, bytes_skipped, values_checked_individually);

    if (i < num_rows)
    {
        /// The offsets of the rest of the values continue the original (unfiltered) offsets: rebase them onto the filtered data.
        if (const size_t shift = original_pos - offset)
            for (size_t k = i; k < num_rows; ++k)
                offsets[k] -= shift;

        deserializeBinaryFilteredWithKnownSizes(data, offsets, istr, i, filter);
    }
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

void serializeStringSizes(const IColumn & column, WriteBuffer & ostr, UInt64 offset, UInt64 limit)
{
    const auto & column_string = typeid_cast<const ColumnString &>(column);
    const auto & offset_values = column_string.getOffsets();
    size_t size = offset_values.size();

    if (!size)
        return;

    size_t end = limit && (offset + limit < size) ? offset + limit : size;
    UInt64 prev_offset = offset_values[offset - 1];
    if constexpr (std::endian::native == std::endian::big)
    {
        for (size_t i = offset; i < end; ++i)
        {
            UInt64 current_offset = offset_values[i];
            writeBinaryLittleEndian(current_offset - prev_offset, ostr);
            prev_offset = current_offset;
        }
    }
    else
    {
        size_t count = end - offset;
        PaddedPODArray<UInt64> sizes;
        sizes.resize(count);

        for (size_t i = 0; i < count; ++i)
        {
            UInt64 current_offset = offset_values[offset + i];
            sizes[i] = current_offset - prev_offset;
            prev_offset = current_offset;
        }

        ostr.write(reinterpret_cast<const char *>(sizes.data()), sizeof(UInt64) * count);
    }
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
    const char * chars = reinterpret_cast<const char *>(column_string.getChars().data());

    /// If the stream is compressed by a codec that uses the boundaries of the values (e.g. `FSST`, which then encodes
    /// every value on its own, so that the values can be checked without decoding them), report them: before the first
    /// value (the stream may contain other data before) and after every value.
    auto * stream_with_value_boundaries = dynamic_cast<IWriteBufferWithValueBoundaries *>(stream);
    if (stream_with_value_boundaries && stream_with_value_boundaries->needsValueBoundaries())
    {
        stream_with_value_boundaries->addValueBoundary(stream->offset());
        size_t value_begin = begin;
        for (size_t i = offset; i < offset + limit; ++i)
        {
            stream->write(chars + value_begin, offsets[i] - value_begin);
            stream_with_value_boundaries->addValueBoundary(stream->offset());
            value_begin = offsets[i];
        }
    }
    else
    {
        stream->write(chars + begin, bytes);
    }
    settings.path.pop_back();
}

void SerializationString::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings, DeserializeBinaryBulkStatePtr & state, SubstreamsDeserializeStatesCache * cache) const
{
    /// Only SINGLE_STREAM serialization keeps a deserialize state; WITH_SIZE_STREAM is stateless.
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
        /// If the data is compressed by `FSST`, the values are checked without decoding them.
        const auto & filter = *settings.string_value_filter;
        auto * compressed_stream = dynamic_cast<CompressedReadBufferFromFile *>(stream);
        if (compressed_stream)
            deserializeBinaryFilteredFromFSST(data, string_column.getOffsets(), *compressed_stream, prev_num_rows, filter);
        else
            deserializeBinaryFilteredWithKnownSizes(data, string_column.getOffsets(), *stream, prev_num_rows, filter);
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

}
