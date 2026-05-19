#include <DataTypes/Serializations/SerializationFixedEncodedText.h>

#include <Columns/ColumnFixedString.h>
#include <Common/Base58.h>
#include <Common/Base64.h>
#include <Common/Exception.h>
#include <Common/SipHash.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>
#include <DataTypes/Serializations/SerializationFixedString.h>
#include <Formats/FormatSettings.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <base/types.h>

#include <cstring>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

SerializationFixedEncodedText::SerializationFixedEncodedText(FixedEncodedTextKind kind_, size_t n_)
    : kind(kind_)
    , n(n_)
    , nested(SerializationFixedString::create(n_))
{
}

UInt128 SerializationFixedEncodedText::getHash(FixedEncodedTextKind kind_, size_t n_)
{
    SipHash hash;
    hash.update(kind_ == FixedEncodedTextKind::Base58 ? "FixedBase58" : "FixedBase64");
    hash.update(n_);
    return hash.get128();
}

SerializationPtr SerializationFixedEncodedText::create(FixedEncodedTextKind kind_, size_t n_)
{
    return ISerialization::pooled(getHash(kind_, n_), [=] { return new SerializationFixedEncodedText(kind_, n_); });
}

String SerializationFixedEncodedText::encodeValue(const IColumn & column, size_t row_num) const
{
    const auto & data = assert_cast<const ColumnFixedString &>(column).getChars();
    const auto * pos = data.data() + n * row_num;

    if (kind == FixedEncodedTextKind::Base64)
        return base64Encode(String(reinterpret_cast<const char *>(pos), n));

    String res;
    if (n == 32)
        res.resize(BASE58_ENCODED_32_LEN);
    else if (n == 64)
        res.resize(BASE58_ENCODED_64_LEN);
    else
        res.resize(n * 2 + 1);

    size_t written = 0;
    if (n == 32)
        written = encodeBase58_32(pos, reinterpret_cast<UInt8 *>(res.data()));
    else if (n == 64)
        written = encodeBase58_64(pos, reinterpret_cast<UInt8 *>(res.data()));
    else
        written = encodeBase58(pos, n, reinterpret_cast<UInt8 *>(res.data()));

    res.resize(written);
    return res;
}

void SerializationFixedEncodedText::decodeAndAppend(IColumn & column, std::string_view encoded) const
{
    auto & data = assert_cast<ColumnFixedString &>(column).getChars();
    const size_t old_size = data.size();
    data.resize(old_size + n);

    try
    {
        if (kind == FixedEncodedTextKind::Base64)
        {
            String decoded = base64Decode(String(encoded.data(), encoded.size()));
            if (decoded.size() != n)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Decoded {} value has {} bytes, expected {} bytes", getFixedEncodedTextTypeName(kind, n), decoded.size(), n);
            memcpy(data.data() + old_size, decoded.data(), n);
        }
        else
        {
            std::optional<size_t> decoded_size;
            if (n == 32)
                decoded_size = decodeBase58_32(reinterpret_cast<const UInt8 *>(encoded.data()), encoded.size(), data.data() + old_size);
            else if (n == 64)
                decoded_size = decodeBase58_64(reinterpret_cast<const UInt8 *>(encoded.data()), encoded.size(), data.data() + old_size);
            else
                decoded_size = decodeBase58(reinterpret_cast<const UInt8 *>(encoded.data()), encoded.size(), data.data() + old_size);

            if (!decoded_size || *decoded_size != n)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot decode {} value of length {} as exactly {} bytes", getFixedEncodedTextTypeName(kind, n), encoded.size(), n);
        }
    }
    catch (...)
    {
        data.resize_assume_reserved(old_size);
        throw;
    }
}

void SerializationFixedEncodedText::serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings & settings) const
{
    nested->serializeBinary(field, ostr, settings);
}

void SerializationFixedEncodedText::deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings & settings) const
{
    nested->deserializeBinary(field, istr, settings);
}

void SerializationFixedEncodedText::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    nested->serializeBinary(column, row_num, ostr, settings);
}

void SerializationFixedEncodedText::deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    nested->deserializeBinary(column, istr, settings);
}

void SerializationFixedEncodedText::serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const
{
    nested->serializeBinaryBulk(column, ostr, offset, limit);
}

void SerializationFixedEncodedText::deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t rows_offset, size_t limit, double avg_value_size_hint) const
{
    nested->deserializeBinaryBulk(column, istr, rows_offset, limit, avg_value_size_hint);
}

void SerializationFixedEncodedText::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeString(encodeValue(column, row_num), ostr);
}

void SerializationFixedEncodedText::deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    String encoded;
    readStringUntilEOF(encoded, istr);
    decodeAndAppend(column, encoded);
}

bool SerializationFixedEncodedText::tryDeserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    try
    {
        deserializeWholeText(column, istr, settings);
        return true;
    }
    catch (...)
    {
        return false;
    }
}

void SerializationFixedEncodedText::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    String encoded = encodeValue(column, row_num);
    writeAnyEscapedString<'\''>(encoded.data(), encoded.data() + encoded.size(), ostr);
}

void SerializationFixedEncodedText::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String encoded;
    if (settings.tsv.crlf_end_of_line_input)
        readEscapedStringCRLF(encoded, istr);
    else
        readEscapedString(encoded, istr);
    decodeAndAppend(column, encoded);
}

bool SerializationFixedEncodedText::tryDeserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    try
    {
        deserializeTextEscaped(column, istr, settings);
        return true;
    }
    catch (...)
    {
        return false;
    }
}

void SerializationFixedEncodedText::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    String encoded = encodeValue(column, row_num);
    if (settings.values.escape_quote_with_quote)
    {
        writeChar('\'', ostr);
        writeAnyEscapedString<'\'', true, false>(encoded.data(), encoded.data() + encoded.size(), ostr);
        writeChar('\'', ostr);
    }
    else
        writeAnyQuotedString<'\''>(encoded.data(), encoded.data() + encoded.size(), ostr);
}

void SerializationFixedEncodedText::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    String encoded;
    readQuotedStringInto<true>(encoded, istr);
    decodeAndAppend(column, encoded);
}

bool SerializationFixedEncodedText::tryDeserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    try
    {
        String encoded;
        if (!tryReadQuotedStringInto<true>(encoded, istr))
            return false;
        decodeAndAppend(column, encoded);
        return true;
    }
    catch (...)
    {
        return false;
    }
}

void SerializationFixedEncodedText::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeJSONString(encodeValue(column, row_num), ostr, settings);
}

void SerializationFixedEncodedText::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String encoded;
    readJSONStringInto(encoded, istr, settings.json);
    decodeAndAppend(column, encoded);
}

bool SerializationFixedEncodedText::tryDeserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    try
    {
        String encoded;
        if (!tryReadJSONStringInto(encoded, istr, settings.json))
            return false;
        decodeAndAppend(column, encoded);
        return true;
    }
    catch (...)
    {
        return false;
    }
}

void SerializationFixedEncodedText::serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    String encoded = encodeValue(column, row_num);
    writeXMLStringForTextElement(encoded.data(), encoded.data() + encoded.size(), ostr);
}

void SerializationFixedEncodedText::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeCSVString(encodeValue(column, row_num), ostr);
}

void SerializationFixedEncodedText::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String encoded;
    readCSVStringInto(encoded, istr, settings.csv);
    decodeAndAppend(column, encoded);
}

bool SerializationFixedEncodedText::tryDeserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    try
    {
        String encoded;
        readCSVStringInto<String, false, false>(encoded, istr, settings.csv);
        decodeAndAppend(column, encoded);
        return true;
    }
    catch (...)
    {
        return false;
    }
}

void SerializationFixedEncodedText::serializeTextMarkdown(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    String encoded = encodeValue(column, row_num);
    if (settings.markdown.escape_special_characters)
        writeMarkdownEscapedString(encoded, ostr);
    else
        writeAnyEscapedString<'\''>(encoded.data(), encoded.data() + encoded.size(), ostr);
}

}
