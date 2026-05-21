#include <DataTypes/Serializations/SerializationFixedStringWithTextRepresentation.h>

#include <Columns/ColumnFixedString.h>
#include <Common/Base58.h>
#include <Common/Base64.h>
#include <Common/Exception.h>
#include <Common/SipHash.h>
#include <Common/assert_cast.h>
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

namespace
{

UInt8 decodeHexDigit(char c)
{
    if (c >= '0' && c <= '9')
        return c - '0';
    if (c >= 'a' && c <= 'f')
        return c - 'a' + 10;
    if (c >= 'A' && c <= 'F')
        return c - 'A' + 10;

    throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid hexadecimal digit '{}'", c);
}

String normalizeHexInput(std::string_view encoded)
{
    if (encoded.size() >= 2 && encoded[0] == '0' && (encoded[1] == 'x' || encoded[1] == 'X'))
        encoded.remove_prefix(2);

    return String(encoded.data(), encoded.size());
}

}

SerializationFixedStringWithTextRepresentation::SerializationFixedStringWithTextRepresentation(FixedStringTextRepresentation text_representation_, size_t n_)
    : text_representation(text_representation_)
    , n(n_)
    , nested(SerializationFixedString::create(n_))
{
}

UInt128 SerializationFixedStringWithTextRepresentation::getHash(FixedStringTextRepresentation text_representation_, size_t n_)
{
    SipHash hash;
    hash.update("FixedStringTextRepresentation");
    hash.update(static_cast<UInt8>(text_representation_));
    hash.update(n_);
    return hash.get128();
}

SerializationPtr SerializationFixedStringWithTextRepresentation::create(FixedStringTextRepresentation text_representation_, size_t n_)
{
    return ISerialization::pooled(getHash(text_representation_, n_), [=] { return new SerializationFixedStringWithTextRepresentation(text_representation_, n_); });
}

String SerializationFixedStringWithTextRepresentation::encodeValue(const IColumn & column, size_t row_num) const
{
    const auto & data = assert_cast<const ColumnFixedString &>(column).getChars();
    const auto * pos = data.data() + n * row_num;

    if (text_representation == FixedStringTextRepresentation::Base64)
        return base64Encode(String(reinterpret_cast<const char *>(pos), n));

    if (text_representation == FixedStringTextRepresentation::Base64URL)
        return base64Encode(String(reinterpret_cast<const char *>(pos), n), /* url_encoding */ true, /* no_padding */ true);

    if (text_representation == FixedStringTextRepresentation::Hex)
    {
        static constexpr char hex[] = "0123456789abcdef";
        String res;
        res.resize(n * 2);
        for (size_t i = 0; i != n; ++i)
        {
            res[2 * i] = hex[pos[i] >> 4];
            res[2 * i + 1] = hex[pos[i] & 0x0F];
        }
        return res;
    }

    if (text_representation == FixedStringTextRepresentation::Base58)
    {
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

    return String(reinterpret_cast<const char *>(pos), n);
}

void SerializationFixedStringWithTextRepresentation::decodeAndAppend(IColumn & column, std::string_view encoded) const
{
    auto & data = assert_cast<ColumnFixedString &>(column).getChars();
    const size_t old_size = data.size();
    data.resize(old_size + n);

    try
    {
        if (text_representation == FixedStringTextRepresentation::Base64 || text_representation == FixedStringTextRepresentation::Base64URL)
        {
            const bool url_encoding = text_representation == FixedStringTextRepresentation::Base64URL;
            const bool no_padding = text_representation == FixedStringTextRepresentation::Base64URL;
            String decoded = base64Decode(String(encoded.data(), encoded.size()), url_encoding, no_padding);
            if (decoded.size() != n)
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "Decoded FixedString({}, '{}') value has {} bytes, expected {} bytes",
                    n, fixedStringTextRepresentationToString(text_representation), decoded.size(), n);
            memcpy(data.data() + old_size, decoded.data(), n);
        }
        else if (text_representation == FixedStringTextRepresentation::Hex)
        {
            String hex = normalizeHexInput(encoded);
            if (hex.size() != n * 2)
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "Decoded FixedString({}, 'Hex') value must be represented by {} hex digits, got {}",
                    n, n * 2, hex.size());

            for (size_t i = 0; i != n; ++i)
                data[old_size + i] = static_cast<UInt8>((decodeHexDigit(hex[2 * i]) << 4) | decodeHexDigit(hex[2 * i + 1]));
        }
        else if (text_representation == FixedStringTextRepresentation::Base58)
        {
            std::optional<size_t> decoded_size;
            if (n == 32)
                decoded_size = decodeBase58_32(reinterpret_cast<const UInt8 *>(encoded.data()), encoded.size(), data.data() + old_size);
            else if (n == 64)
                decoded_size = decodeBase58_64(reinterpret_cast<const UInt8 *>(encoded.data()), encoded.size(), data.data() + old_size);
            else
                decoded_size = decodeBase58(reinterpret_cast<const UInt8 *>(encoded.data()), encoded.size(), data.data() + old_size);

            if (!decoded_size || *decoded_size != n)
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "Cannot decode FixedString({}, 'Base58') value of length {} as exactly {} bytes", n, encoded.size(), n);
        }
        else
        {
            if (encoded.size() != n)
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "FixedString({}, 'Raw') value has {} bytes, expected {} bytes", n, encoded.size(), n);
            memcpy(data.data() + old_size, encoded.data(), n);
        }
    }
    catch (...)
    {
        data.resize_assume_reserved(old_size);
        throw;
    }
}

void SerializationFixedStringWithTextRepresentation::serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings & settings) const
{
    nested->serializeBinary(field, ostr, settings);
}

void SerializationFixedStringWithTextRepresentation::deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings & settings) const
{
    nested->deserializeBinary(field, istr, settings);
}

void SerializationFixedStringWithTextRepresentation::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    nested->serializeBinary(column, row_num, ostr, settings);
}

void SerializationFixedStringWithTextRepresentation::deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    nested->deserializeBinary(column, istr, settings);
}

void SerializationFixedStringWithTextRepresentation::serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const
{
    nested->serializeBinaryBulk(column, ostr, offset, limit);
}

void SerializationFixedStringWithTextRepresentation::deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t rows_offset, size_t limit, double avg_value_size_hint) const
{
    nested->deserializeBinaryBulk(column, istr, rows_offset, limit, avg_value_size_hint);
}

void SerializationFixedStringWithTextRepresentation::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeString(encodeValue(column, row_num), ostr);
}

void SerializationFixedStringWithTextRepresentation::deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    String encoded;
    readStringUntilEOF(encoded, istr);
    decodeAndAppend(column, encoded);
}

bool SerializationFixedStringWithTextRepresentation::tryDeserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
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

void SerializationFixedStringWithTextRepresentation::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    String encoded = encodeValue(column, row_num);
    writeAnyEscapedString<'\''>(encoded.data(), encoded.data() + encoded.size(), ostr);
}

void SerializationFixedStringWithTextRepresentation::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String encoded;
    if (settings.tsv.crlf_end_of_line_input)
        readEscapedStringCRLF(encoded, istr);
    else
        readEscapedString(encoded, istr);
    decodeAndAppend(column, encoded);
}

bool SerializationFixedStringWithTextRepresentation::tryDeserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
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

void SerializationFixedStringWithTextRepresentation::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
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

void SerializationFixedStringWithTextRepresentation::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    String encoded;
    readQuotedStringInto<true>(encoded, istr);
    decodeAndAppend(column, encoded);
}

bool SerializationFixedStringWithTextRepresentation::tryDeserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
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

void SerializationFixedStringWithTextRepresentation::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeJSONString(encodeValue(column, row_num), ostr, settings);
}

void SerializationFixedStringWithTextRepresentation::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String encoded;
    readJSONStringInto(encoded, istr, settings.json);
    decodeAndAppend(column, encoded);
}

bool SerializationFixedStringWithTextRepresentation::tryDeserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
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

void SerializationFixedStringWithTextRepresentation::serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    String encoded = encodeValue(column, row_num);
    writeXMLStringForTextElement(encoded.data(), encoded.data() + encoded.size(), ostr);
}

void SerializationFixedStringWithTextRepresentation::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeCSVString(encodeValue(column, row_num), ostr);
}

void SerializationFixedStringWithTextRepresentation::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String encoded;
    readCSVStringInto(encoded, istr, settings.csv);
    decodeAndAppend(column, encoded);
}

bool SerializationFixedStringWithTextRepresentation::tryDeserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
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

void SerializationFixedStringWithTextRepresentation::serializeTextMarkdown(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    String encoded = encodeValue(column, row_num);
    if (settings.markdown.escape_special_characters)
        writeMarkdownEscapedString(encoded, ostr);
    else
        writeAnyEscapedString<'\''>(encoded.data(), encoded.data() + encoded.size(), ostr);
}

}
