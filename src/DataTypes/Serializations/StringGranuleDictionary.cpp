#include <DataTypes/Serializations/StringGranuleDictionary.h>

#include <Columns/ColumnReplicated.h>
#include <Columns/ColumnsNumber.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Common/assert_cast.h>
#include <base/constexpr_helpers.h>
#include <base/sort.h>
#include <base/unaligned.h>

#include <bit>
#include <cstring>
#include <limits>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_ALL_DATA;
    extern const int INCORRECT_DATA;
}

namespace
{

/// Returns the bytes that `count` values of `bits` bits take packed together.
size_t bitPackedBytes(size_t count, UInt8 bits)
{
    return (count * bits + 7) / 8;
}

UInt8 bitWidth(UInt64 value)
{
    return static_cast<UInt8>(std::bit_width(value));
}

/// Packing and unpacking access the 8 bytes that hold a code, which the padding of `PaddedPODArray` covers
/// after the last packed byte.
static_assert(PADDING_FOR_SIMD > sizeof(UInt64));

}

UInt8 StringGranuleHeader::codeBitsFor(UInt64 dictionary_size)
{
    return bitWidth(dictionary_size - 1);
}

size_t StringGranuleHeader::codesByteSize() const
{
    return bitPackedBytes(rows, code_bits);
}

void StringGranuleHeader::write(WriteBuffer & out) const
{
    writeBinary(static_cast<UInt8>(kind), out);
    writeVarUInt(rows, out);
    if (kind == Kind::Dictionary)
    {
        writeVarUInt(dictionary_size, out);
        writeBinary(code_bits, out);
    }
}

void StringGranuleHeader::read(ReadBuffer & in)
{
    UInt8 kind_value = 0;
    readBinary(kind_value, in);
    if (kind_value > static_cast<UInt8>(Kind::Dictionary))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unknown kind {} of a String granule", static_cast<UInt32>(kind_value));
    kind = static_cast<Kind>(kind_value);

    readVarUInt(rows, in);
    if (rows == 0)
        throw Exception(ErrorCodes::INCORRECT_DATA, "A String granule has no rows");

    dictionary_size = 0;
    code_bits = 0;
    if (kind == Kind::Plain)
        return;

    readVarUInt(dictionary_size, in);
    readBinary(code_bits, in);
    if (dictionary_size == 0 || dictionary_size > rows || dictionary_size > std::numeric_limits<UInt32>::max())
        throw Exception(ErrorCodes::INCORRECT_DATA, "A String granule of {} rows has a dictionary of {} values", rows, dictionary_size);
    if (code_bits != codeBitsFor(dictionary_size))
        throw Exception(ErrorCodes::INCORRECT_DATA, "A String granule with a dictionary of {} values has codes of {} bits",
            dictionary_size, static_cast<UInt32>(code_bits));
}

namespace
{

/// Adds the length of every row of `slice` to `bytes` and raises `longest` to the longest one.
void measureSlice(const StringGranuleDictionaryBuilder::Slice & slice, size_t & bytes, size_t & longest)
{
    auto measure = [&](const IColumn::Offsets & offsets, auto && slot_of)
    {
        for (size_t i = slice.offset; i < slice.offset + slice.rows; ++i)
        {
            const size_t slot = slot_of(i);
            const size_t length = offsets[slot] - offsets[static_cast<ssize_t>(slot) - 1];
            bytes += length;
            longest = std::max(longest, length);
        }
    };

    if (const auto * replicated = typeid_cast<const ColumnReplicated *>(slice.column.get()))
    {
        const auto & offsets = assert_cast<const ColumnString &>(*replicated->getNestedColumn()).getOffsets();
        replicated->getIndexes().callForIndexesData(
            [&](const auto & indexes) { measure(offsets, [&](size_t i) { return static_cast<size_t>(indexes[i]); }); });
    }
    else
    {
        measure(assert_cast<const ColumnString &>(*slice.column).getOffsets(), [](size_t i) { return i; });
    }
}

}

bool StringGranuleDictionaryBuilder::build(const std::vector<Slice> & slices, size_t rows, const Settings & settings)
{
    positions.clear();
    values.clear();
    row_codes.resize(rows);

    size_t plain_bytes = 0;
    size_t longest = 0;
    for (const auto & slice : slices)
        measureSlice(slice, plain_bytes, longest);

    granule_rows = rows;
    dictionary_bytes = 0;
    length_bits = bitWidth(longest);
    max_dictionary_size = static_cast<double>(plain_bytes + bitPackedBytes(rows, length_bits)) * (1.0 - settings.min_size_reduction);
    max_distinct = static_cast<double>(rows) * settings.max_distinct_ratio;

    size_t row = 0;
    for (const auto & slice : slices)
    {
        const auto * replicated = typeid_cast<const ColumnReplicated *>(slice.column.get());
        const bool stays_dictionary = replicated
            ? addReplicatedRows(*replicated, slice.offset, slice.rows, row)
            : addPlainRows(assert_cast<const ColumnString &>(*slice.column), slice.offset, slice.rows, row);
        if (!stays_dictionary)
            return false;
    }
    chassert(row == rows);

    /// Sort the distinct values and give each the code of its place in the sorted order. Values that come in
    /// sorted order, as the slots of one sorted dictionary of a replicated slice do, sort in linear time.
    const size_t distinct = values.size();
    code_bits = StringGranuleHeader::codeBitsFor(distinct);
    sorted_positions.resize(distinct);
    for (size_t i = 0; i < distinct; ++i)
        sorted_positions[i] = static_cast<UInt32>(i);
    ::sort(sorted_positions.begin(), sorted_positions.end(), [&](UInt32 lhs, UInt32 rhs) { return values[lhs] < values[rhs]; });

    codes_of_positions.resize(distinct);
    dictionary = ColumnString::create();
    dictionary->getChars().reserve(dictionary_bytes);
    dictionary->getOffsets().reserve(distinct);
    for (size_t code = 0; code < distinct; ++code)
    {
        const UInt32 position = sorted_positions[code];
        codes_of_positions[position] = static_cast<UInt32>(code);
        dictionary->insertData(values[position].data(), values[position].size());
    }

    for (auto & code : row_codes)
        code = codes_of_positions[code];

    return true;
}

bool StringGranuleDictionaryBuilder::addValue(std::string_view value, UInt32 & position)
{
    Positions::LookupResult it = nullptr;
    bool inserted = false;
    positions.emplace(value, it, inserted);
    if (inserted)
    {
        it->getMapped() = static_cast<UInt32>(values.size());
        values.push_back(value);
        dictionary_bytes += value.size();

        /// The size the dictionary form takes with the distinct values so far, the least it can still take.
        const size_t distinct = values.size();
        const size_t dictionary_size = dictionary_bytes + bitPackedBytes(distinct, length_bits)
            + bitPackedBytes(granule_rows, StringGranuleHeader::codeBitsFor(distinct));
        if (static_cast<double>(distinct) > max_distinct || static_cast<double>(dictionary_size) > max_dictionary_size)
            return false;
    }
    position = it->getMapped();
    return true;
}

bool StringGranuleDictionaryBuilder::addPlainRows(const ColumnString & column, size_t offset, size_t rows, size_t & row)
{
    /// A row equal to the previous one takes its position without the hash table, which pays in sorted data.
    std::string_view previous;
    UInt32 previous_position = 0;
    bool has_previous = false;
    for (size_t i = offset; i < offset + rows; ++i)
    {
        const std::string_view value = column.getDataAt(i);
        if (!has_previous || value != previous)
        {
            if (!addValue(value, previous_position))
                return false;
            previous = value;
            has_previous = true;
        }
        row_codes[row++] = previous_position;
    }
    return true;
}

bool StringGranuleDictionaryBuilder::addReplicatedRows(const ColumnReplicated & column, size_t offset, size_t rows, size_t & row)
{
    const auto & nested = assert_cast<const ColumnString &>(*column.getNestedColumn());
    if (slot_positions.size() < nested.size())
    {
        slot_positions.resize(nested.size());
        slot_generations.resize_fill(nested.size(), 0);
    }
    if (++generation == 0)
    {
        std::fill(slot_generations.begin(), slot_generations.end(), 0);
        generation = 1;
    }

    return column.getIndexes().callForIndexesData([&](const auto & indexes)
    {
        for (size_t i = offset; i < offset + rows; ++i)
        {
            const size_t slot = indexes[i];
            if (slot_generations[slot] != generation)
            {
                if (!addValue(nested.getDataAt(slot), slot_positions[slot]))
                    return false;
                slot_generations[slot] = generation;
            }
            row_codes[row++] = slot_positions[slot];
        }
        return true;
    });
}

void StringGranuleDictionaryBuilder::writeCodes(WriteBuffer & out)
{
    if (code_bits == 0)
        return;

    /// The codes are collected in `pending`, lowest bits first, and stored 4 bytes at a time: fewer than 32
    /// bits wait there, and a code adds at most 32, so they fit in 64 bits. The last store writes into the
    /// padding.
    const size_t bytes = bitPackedBytes(row_codes.size(), code_bits);
    packed_codes.resize(bytes);
    UInt8 * data = packed_codes.data();
    UInt64 pending = 0;
    size_t pending_bits = 0;
    size_t position = 0;
    for (const UInt32 code : row_codes)
    {
        pending |= static_cast<UInt64>(code) << pending_bits;
        pending_bits += code_bits;
        if (pending_bits >= 32)
        {
            unalignedStoreLittleEndian<UInt32>(data + position, static_cast<UInt32>(pending));
            position += sizeof(UInt32);
            pending >>= 32;
            pending_bits -= 32;
        }
    }
    unalignedStoreLittleEndian<UInt64>(data + position, pending);
    out.write(reinterpret_cast<const char *>(data), bytes);
}

namespace
{

/// Decodes `count` codes of `bits` bits from `in`, which has 8 readable bytes after its last code.
template <size_t bits, typename T>
void unpackCodes(const UInt8 * in, T * out, size_t count)
{
    static constexpr UInt64 mask = (UInt64{1} << bits) - 1;
    for (size_t row = 0; row < count; ++row)
    {
        const size_t bit = row * bits;
        out[row] = static_cast<T>((unalignedLoadLittleEndian<UInt64>(in + bit / 8) >> (bit % 8)) & mask);
    }
}

/// Reads the packed codes of all rows of the dictionary granule `granule` and decodes them into `codes`, a
/// column of the unsigned type of the fewest bytes that hold `code_bits`, reused when it has that type.
/// Rejects a code outside the dictionary.
void readCodes(ReadBuffer & in, const StringGranuleHeader & granule, MutableColumnPtr & codes, PaddedPODArray<UInt8> & packed)
{
    auto decode = [&]<typename T>()
    {
        if (!codes || !typeid_cast<ColumnVector<T> *>(codes.get()))
            codes = ColumnVector<T>::create();
        auto & data = assert_cast<ColumnVector<T> &>(*codes).getData();
        data.resize(granule.rows);

        if (granule.code_bits == 0)
        {
            memset(data.data(), 0, data.size() * sizeof(T));
            return;
        }

        const size_t bytes = granule.codesByteSize();
        packed.resize(bytes);
        const size_t read_bytes = in.read(reinterpret_cast<char *>(packed.data()), bytes);
        if (read_bytes != bytes)
            throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Cannot read the codes of a String granule: read {} of {} bytes", read_bytes, bytes);

        /// The width becomes a template argument, so that the shifts and the mask of the decoding loop are
        /// constants.
        static_for<size_t{1}, sizeof(T) * 8 + 1>([&](auto bits)
        {
            if (granule.code_bits != bits)
                return false;
            unpackCodes<bits, T>(packed.data(), data.data(), data.size());
            return true;
        });

        T max_code = 0;
        for (T code : data)
            max_code = std::max(max_code, code);
        if (max_code >= granule.dictionary_size)
            throw Exception(ErrorCodes::INCORRECT_DATA, "A String granule has code {} for a dictionary of {} values",
                static_cast<UInt64>(max_code), granule.dictionary_size);
    };

    if (granule.code_bits <= 8)
        decode.template operator()<UInt8>();
    else if (granule.code_bits <= 16)
        decode.template operator()<UInt16>();
    else
        decode.template operator()<UInt32>();
}

}

bool DeserializeBinaryBulkStateStringWithDictionaries::startGranule(ReadBuffer & indexes)
{
    if (indexes.eof())
        return false;

    granule.read(indexes);
    rows_left_in_granule = granule.rows;
    dictionary.reset();
    if (granule.kind == StringGranuleHeader::Kind::Dictionary)
    {
        if (!granule_codes && !spare_codes.empty())
        {
            granule_codes = std::move(spare_codes.back());
            spare_codes.pop_back();
        }
        readCodes(indexes, granule, granule_codes, packed_codes);
    }
    return true;
}

void DeserializeBinaryBulkStateStringWithDictionaries::recycleCodes(MutableColumnPtr && codes)
{
    spare_codes.push_back(std::move(codes));
}

ISerialization::DeserializeBinaryBulkStatePtr DeserializeBinaryBulkStateStringWithDictionaries::cloneImpl(
    ISerialization::DeserializeBinaryBulkStateCloneContext &) const
{
    /// Prefixes are cloned before anything is read, so there is no position to copy.
    auto res = std::make_shared<DeserializeBinaryBulkStateStringWithDictionaries>();
    res->need_string_data = need_string_data;
    return res;
}

ISerialization::DeserializeBinaryBulkStatePtr getStringGranulesDeserializeState(
    ISerialization::DeserializeBinaryBulkSettings & settings, ISerialization::SubstreamsDeserializeStatesCache * cache)
{
    settings.path.push_back(ISerialization::Substream::StringDictionaryIndexes);
    auto state = ISerialization::getFromSubstreamsDeserializeStatesCache(cache, settings.path);
    if (!state)
    {
        state = std::make_shared<DeserializeBinaryBulkStateStringWithDictionaries>();
        ISerialization::addToSubstreamsDeserializeStatesCache(cache, settings.path, state);
    }
    settings.path.pop_back();
    return state;
}

}
