#include <DataTypes/Serializations/StringGranuleDictionary.h>

#include <Columns/ColumnsNumber.h>
#include <DataTypes/Serializations/SerializationNumber.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Common/assert_cast.h>
#include <base/sort.h>

#include <limits>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_ALL_DATA;
    extern const int INCORRECT_DATA;
}

UInt8 StringGranuleHeader::codeWidthFor(UInt64 dictionary_size)
{
    const UInt64 max_code = dictionary_size - 1;
    if (max_code <= std::numeric_limits<UInt8>::max())
        return 1;
    if (max_code <= std::numeric_limits<UInt16>::max())
        return 2;
    return 4;
}

void StringGranuleHeader::write(WriteBuffer & out) const
{
    writeBinary(static_cast<UInt8>(kind), out);
    writeVarUInt(rows, out);
    if (kind == Kind::Dictionary)
    {
        writeVarUInt(dictionary_size, out);
        writeBinary(code_width, out);
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
    code_width = 0;
    if (kind == Kind::Plain)
        return;

    readVarUInt(dictionary_size, in);
    readBinary(code_width, in);
    if (dictionary_size == 0 || dictionary_size > rows || dictionary_size > std::numeric_limits<UInt32>::max())
        throw Exception(ErrorCodes::INCORRECT_DATA, "A String granule of {} rows has a dictionary of {} values", rows, dictionary_size);
    if (code_width != codeWidthFor(dictionary_size))
        throw Exception(ErrorCodes::INCORRECT_DATA, "A String granule with a dictionary of {} values has codes of {} bytes",
            dictionary_size, static_cast<UInt32>(code_width));
}

size_t StringGranuleHeader::byteSize() const
{
    size_t size = 1 + getLengthOfVarUInt(rows);
    if (kind == Kind::Dictionary)
        size += getLengthOfVarUInt(dictionary_size) + 1;
    return size;
}

bool StringGranuleDictionaryBuilder::build(const std::vector<Slice> & slices, size_t rows, const Settings & settings)
{
    positions.clear();
    values.clear();
    row_codes.resize(rows);

    size_t plain_bytes = 0;
    size_t dictionary_bytes = 0;
    size_t row = 0;
    for (const auto & slice : slices)
    {
        const auto & column = assert_cast<const ColumnString &>(*slice.column);
        for (size_t i = slice.offset; i < slice.offset + slice.rows; ++i)
        {
            const std::string_view value = column.getDataAt(i);
            plain_bytes += value.size();

            Positions::LookupResult it = nullptr;
            bool inserted = false;
            positions.emplace(value, it, inserted);
            if (inserted)
            {
                it->getMapped() = static_cast<UInt32>(values.size());
                values.push_back(value);
                dictionary_bytes += value.size();
            }
            row_codes[row++] = it->getMapped();
        }
    }
    chassert(row == rows);

    const size_t distinct = values.size();
    code_width = StringGranuleHeader::codeWidthFor(distinct);

    const StringGranuleHeader header{
        .kind = StringGranuleHeader::Kind::Dictionary, .rows = rows, .dictionary_size = distinct, .code_width = code_width};
    const double plain_size = static_cast<double>(plain_bytes + sizeof(UInt64) * rows);
    const double dictionary_size
        = static_cast<double>(dictionary_bytes + sizeof(UInt64) * distinct + code_width * rows + header.byteSize());

    if (dictionary_size > plain_size * (1.0 - settings.min_size_reduction)
        || static_cast<double>(distinct) > static_cast<double>(rows) * settings.max_distinct_ratio)
        return false;

    /// Sort the distinct values and give each the code of its place in the sorted order.
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

namespace
{

template <typename T>
void writeCodesOfWidth(const PaddedPODArray<UInt32> & codes, WriteBuffer & out)
{
    if constexpr (std::is_same_v<T, UInt32>)
    {
        SerializationNumber<UInt32>::serializeBinaryBulk(codes, out, 0, codes.size());
    }
    else
    {
        PaddedPODArray<T> narrow(codes.size());
        for (size_t i = 0; i < codes.size(); ++i)
            narrow[i] = static_cast<T>(codes[i]);
        SerializationNumber<T>::serializeBinaryBulk(narrow, out, 0, narrow.size());
    }
}

}

void StringGranuleDictionaryBuilder::writeCodes(WriteBuffer & out) const
{
    switch (code_width)
    {
        case 1: writeCodesOfWidth<UInt8>(row_codes, out); break;
        case 2: writeCodesOfWidth<UInt16>(row_codes, out); break;
        default: writeCodesOfWidth<UInt32>(row_codes, out); break;
    }
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

namespace
{

template <typename T>
MutableColumnPtr readCodesOfWidth(ReadBuffer & in, size_t rows, UInt64 dictionary_size)
{
    auto column = ColumnVector<T>::create();
    auto & codes = column->getData();
    SerializationNumber<T>::deserializeBinaryBulk(codes, in, rows);
    if (codes.size() != rows)
        throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Cannot read the codes of a String granule: read {} of {}", codes.size(), rows);

    T max_code = 0;
    for (T code : codes)
        max_code = std::max(max_code, code);
    if (max_code >= dictionary_size)
        throw Exception(ErrorCodes::INCORRECT_DATA, "A String granule has code {} for a dictionary of {} values",
            static_cast<UInt64>(max_code), dictionary_size);

    return column;
}

}

MutableColumnPtr readStringGranuleCodes(ReadBuffer & in, const StringGranuleHeader & granule, size_t rows)
{
    switch (granule.code_width)
    {
        case 1: return readCodesOfWidth<UInt8>(in, rows, granule.dictionary_size);
        case 2: return readCodesOfWidth<UInt16>(in, rows, granule.dictionary_size);
        default: return readCodesOfWidth<UInt32>(in, rows, granule.dictionary_size);
    }
}

}
