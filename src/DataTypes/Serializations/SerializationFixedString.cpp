#include <DataTypes/Serializations/SerializationFixedString.h>

#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnConst.h>

#include <Formats/FormatSettings.h>

#include <IO/WriteBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/VarInt.h>

#include <Common/PODArray.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>
#include <base/types.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_ALL_DATA;
    extern const int TOO_LARGE_STRING_SIZE;
}

static constexpr size_t MAX_STRINGS_SIZE = 1ULL << 30;

void SerializationFixedString::serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings &) const
{
    const String & s = field.safeGet<String>();
    ostr.write(s.data(), std::min(s.size(), n));
    if (s.size() < n)
        for (size_t i = s.size(); i < n; ++i)
            ostr.write(0);
}


void SerializationFixedString::deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings &) const
{
    field = String();
    String & s = field.safeGet<String>();
    s.resize(n);
    istr.readStrict(s.data(), n);
}


void SerializationFixedString::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    ostr.write(reinterpret_cast<const char *>(&assert_cast<const ColumnFixedString &>(column).getChars()[n * row_num]), n);
}


void SerializationFixedString::deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    ColumnFixedString::Chars & data = assert_cast<ColumnFixedString &>(column).getChars();
    size_t old_size = data.size();
    data.resize(old_size + n);
    try
    {
        istr.readStrict(reinterpret_cast<char *>(data.data() + old_size), n);
    }
    catch (...)
    {
        data.resize_assume_reserved(old_size);
        throw;
    }
}


void SerializationFixedString::serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const
{
    const ColumnFixedString::Chars & data = typeid_cast<const ColumnFixedString &>(column).getChars();

    size_t size = data.size() / n;

    if (limit == 0 || offset + limit > size)
        limit = size - offset;

    if (limit)
        ostr.write(reinterpret_cast<const char *>(&data[n * offset]), n * limit);
}


void SerializationFixedString::deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t rows_offset, size_t limit, double /*avg_value_size_hint*/) const
{
    ColumnFixedString::Chars & data = typeid_cast<ColumnFixedString &>(column).getChars();

    size_t skipped_bytes;

    if (unlikely(__builtin_mul_overflow(rows_offset, n, &skipped_bytes)))
        throw Exception(ErrorCodes::TOO_LARGE_STRING_SIZE, "Deserializing FixedString will lead to overflow");
    istr.ignore(skipped_bytes);

    size_t initial_size = data.size();
    size_t max_bytes;
    size_t new_data_size;

    if (unlikely(__builtin_mul_overflow(limit, n, &max_bytes)))
        throw Exception(ErrorCodes::TOO_LARGE_STRING_SIZE, "Deserializing FixedString will lead to overflow");
    if (unlikely(max_bytes > MAX_STRINGS_SIZE))
        throw Exception(ErrorCodes::TOO_LARGE_STRING_SIZE, "Too large sizes of FixedString to deserialize: {}", max_bytes);
    if (unlikely(__builtin_add_overflow(initial_size, max_bytes, &new_data_size)))
        throw Exception(ErrorCodes::TOO_LARGE_STRING_SIZE, "Deserializing FixedString will lead to overflow");

    data.resize(new_data_size);
    size_t read_bytes = istr.readBig(reinterpret_cast<char *>(&data[initial_size]), max_bytes);

    if (read_bytes % n != 0)
        throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Cannot read all data of type FixedString. "
            "Bytes read:{}. String size:{}.", read_bytes, toString(n));

    data.resize(initial_size + read_bytes);
}


void SerializationFixedString::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeString(reinterpret_cast<const char *>(&assert_cast<const ColumnFixedString &>(column).getChars()[n * row_num]), n, ostr);
}


void SerializationFixedString::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    const char * pos = reinterpret_cast<const char *>(&assert_cast<const ColumnFixedString &>(column).getChars()[n * row_num]);
    writeAnyEscapedString<'\''>(pos, pos + n, ostr);
}


void SerializationFixedString::alignStringLength(size_t n, PaddedPODArray<UInt8> & data, size_t string_start)
{
    size_t length = data.size() - string_start;
    if (length < n)
    {
        data.resize_fill(string_start + n);
    }
    else if (length > n)
    {
        data.resize_assume_reserved(string_start);
        throw Exception(ErrorCodes::TOO_LARGE_STRING_SIZE, "Too large value for FixedString({})", n);
    }
}

template <typename Reader>
static inline void read(const SerializationFixedString & self, IColumn & column, Reader && reader)
{
    ColumnFixedString::Chars & data = typeid_cast<ColumnFixedString &>(column).getChars();
    size_t prev_size = data.size();
    try
    {
        reader(data);
        SerializationFixedString::alignStringLength(self.getN(), data, prev_size);
    }
    catch (...)
    {
        data.resize_assume_reserved(prev_size);
        throw;
    }
}

bool SerializationFixedString::tryAlignStringLength(size_t n, PaddedPODArray<UInt8> & data, size_t string_start)
{
    size_t length = data.size() - string_start;
    if (length < n)
    {
        data.resize_fill(string_start + n);
    }
    else if (length > n)
    {
        data.resize_assume_reserved(string_start);
        return false;
    }

    return true;
}

template <typename Reader>
static inline bool tryRead(const SerializationFixedString & self, IColumn & column, Reader && reader)
{
    ColumnFixedString::Chars & data = typeid_cast<ColumnFixedString &>(column).getChars();
    size_t prev_size = data.size();
    try
    {
        return reader(data) && SerializationFixedString::tryAlignStringLength(self.getN(), data, prev_size);
    }
    catch (...)
    {
        data.resize_assume_reserved(prev_size);
        return false;
    }
}


void SerializationFixedString::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    read(*this, column, [&istr, &settings](ColumnFixedString::Chars & data)
    {
        settings.tsv.crlf_end_of_line_input ? readEscapedStringInto<ColumnFixedString::Chars,true>(data, istr) : readEscapedStringInto<ColumnFixedString::Chars,false>(data, istr);
    });
}

bool SerializationFixedString::tryDeserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    return tryRead(*this, column, [&istr](ColumnFixedString::Chars & data) { readEscapedStringInto<PaddedPODArray<UInt8>,false>(data, istr); return true; });
}


void SerializationFixedString::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    const char * pos = reinterpret_cast<const char *>(&assert_cast<const ColumnFixedString &>(column).getChars()[n * row_num]);
    writeAnyQuotedString<'\''>(pos, pos + n, ostr);
}


void SerializationFixedString::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    read(*this, column, [&istr](ColumnFixedString::Chars & data) { readQuotedStringInto<true>(data, istr); });
}

bool SerializationFixedString::tryDeserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    return tryRead(*this, column, [&istr](ColumnFixedString::Chars & data) { return tryReadQuotedStringInto<true>(data, istr); });
}


void SerializationFixedString::deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    read(*this, column, [&istr](ColumnFixedString::Chars & data) { readStringUntilEOFInto(data, istr); });
}

bool SerializationFixedString::tryDeserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    return tryRead(*this, column, [&istr](ColumnFixedString::Chars & data) { readStringUntilEOFInto(data, istr); return true; });
}


void SerializationFixedString::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const char * pos = reinterpret_cast<const char *>(&assert_cast<const ColumnFixedString &>(column).getChars()[n * row_num]);
    writeJSONString(pos, pos + n, ostr, settings);
}


void SerializationFixedString::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    read(*this, column, [&istr, &settings](ColumnFixedString::Chars & data) { readJSONStringInto(data, istr, settings.json); });
}

bool SerializationFixedString::tryDeserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    return tryRead(*this, column, [&istr, &settings](ColumnFixedString::Chars & data) { return tryReadJSONStringInto(data, istr, settings.json); });
}

void SerializationFixedString::serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    const char * pos = reinterpret_cast<const char *>(&assert_cast<const ColumnFixedString &>(column).getChars()[n * row_num]);
    writeXMLStringForTextElement(pos, pos + n, ostr);
}


void SerializationFixedString::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    const char * pos = reinterpret_cast<const char *>(&assert_cast<const ColumnFixedString &>(column).getChars()[n * row_num]);
    writeCSVString(pos, pos + n, ostr);
}


void SerializationFixedString::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    read(*this, column, [&istr, &csv = settings.csv](ColumnFixedString::Chars & data) { readCSVStringInto(data, istr, csv); });
}

bool SerializationFixedString::tryDeserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    return tryRead(*this, column, [&istr, &csv = settings.csv](ColumnFixedString::Chars & data) { readCSVStringInto<ColumnFixedString::Chars, false, false>(data, istr, csv); return true; });
}

void SerializationFixedString::serializeTextMarkdown(
    const DB::IColumn & column, size_t row_num, DB::WriteBuffer & ostr, const DB::FormatSettings & settings) const
{
    if (settings.markdown.escape_special_characters)
    {
        writeMarkdownEscapedString(
            reinterpret_cast<const char *>(&(assert_cast<const ColumnFixedString &>(column).getChars()[n * row_num])), n, ostr);
    }
    else
        serializeTextEscaped(column, row_num, ostr, settings);
}

}
