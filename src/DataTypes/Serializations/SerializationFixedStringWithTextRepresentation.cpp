#include <DataTypes/Serializations/SerializationFixedStringWithTextRepresentation.h>

#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Common/Base58.h>
#include <Common/Base64.h>
#include <Common/Exception.h>
#include <Common/PODArray.h>
#include <Common/SipHash.h>
#include <Common/StringUtils.h>
#include <Common/assert_cast.h>
#include <DataTypes/Serializations/SerializationFixedString.h>
#include <Formats/FormatSettings.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <base/hex.h>

#include "config.h"

#if USE_SIMDUTF
#    include <simdutf.h>
#else
#    include <Poco/Exception.h>
#endif

#include <cstring>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

namespace
{

/// Encoded values of up to this size are formatted on the stack.
constexpr size_t STACK_BUFFER_SIZE = 256;

/// Invalid values in exception messages are truncated to this size.
constexpr size_t MAX_VALUE_SIZE_IN_EXCEPTION = 128;

bool tryDecodeHex(std::string_view encoded, size_t n, UInt8 * dst)
{
    if (encoded.size() >= 2 && encoded[0] == '0' && (encoded[1] == 'x' || encoded[1] == 'X'))
        encoded.remove_prefix(2);

    if (encoded.size() != n * 2)
        return false;

    for (size_t i = 0; i != n; ++i)
    {
        const char high = encoded[2 * i];
        const char low = encoded[2 * i + 1];
        if (!isHexDigit(high) || !isHexDigit(low))
            return false;
        dst[i] = static_cast<UInt8>((unhex(high) << 4) | unhex(low));
    }
    return true;
}

bool tryDecodeBase64(std::string_view encoded, size_t n, UInt8 * dst, bool url_encoding)
{
#if USE_SIMDUTF
    /// Same accepted syntax as the base64Decode / base64URLDecode functions:
    /// Base64 requires a complete (padded) final chunk, Base64URL accepts both alphabets and optional padding.
    const auto options = url_encoding ? simdutf::base64_default_or_url : simdutf::base64_default;
    const auto last_chunk = url_encoding ? simdutf::loose : simdutf::strict;

    size_t written = n;
    auto res = simdutf::base64_to_binary_safe(encoded.data(), encoded.size(), reinterpret_cast<char *>(dst), written, options, last_chunk);
    if (res.error == simdutf::BASE64_EXTRA_BITS && !url_encoding)
    {
        /// Like base64Decode, ignore non-zero leftover bits in a complete, properly padded final chunk.
        written = n;
        res = simdutf::base64_to_binary_safe(encoded.data(), encoded.size(), reinterpret_cast<char *>(dst), written, options, simdutf::loose);
    }

    /// OUTPUT_BUFFER_TOO_SMALL means that the decoded value is longer than N bytes.
    return res.error == simdutf::SUCCESS && written == n;
#else
    std::string decoded;
    try
    {
        decoded = base64Decode(std::string(encoded), url_encoding, /* no_padding */ url_encoding);
    }
    catch (const Poco::Exception &)
    {
        return false;
    }
    if (decoded.size() != n)
        return false;
    memcpy(dst, decoded.data(), n);
    return true;
#endif
}

bool tryDecodeBase58(std::string_view encoded, size_t n, UInt8 * dst)
{
    const auto * src = reinterpret_cast<const UInt8 *>(encoded.data());

    /// The decoder writes up to one byte per input character without any bound, and it is quadratic.
    /// An N bytes value is encoded with at most maxBase58EncodedLength(N) characters, so longer inputs are invalid.
    if (encoded.size() > maxBase58EncodedLength(n))
        return false;

    PODArrayWithStackMemory<UInt8, STACK_BUFFER_SIZE> buffer(encoded.size());
    const auto decoded_size = decodeBase58(src, encoded.size(), buffer.data());
    if (decoded_size.value_or(0) != n)
        return false;

    memcpy(dst, buffer.data(), n);
    return true;
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

size_t SerializationFixedStringWithTextRepresentation::maxEncodedSize(FixedStringTextRepresentation text_representation, size_t n)
{
    switch (text_representation)
    {
        case FixedStringTextRepresentation::Raw:
            return n;
        case FixedStringTextRepresentation::Hex:
            return n * 2;
        case FixedStringTextRepresentation::Base64:
        case FixedStringTextRepresentation::Base64URL:
            return (n + 2) / 3 * 4;
        case FixedStringTextRepresentation::Base58:
            if (n == 32)
                return BASE58_ENCODED_32_LEN;
            if (n == 64)
                return BASE58_ENCODED_64_LEN;
            return n * 2 + 1;
    }
}

size_t SerializationFixedStringWithTextRepresentation::encode(FixedStringTextRepresentation text_representation, size_t n, const UInt8 * src, char * dst)
{
    switch (text_representation)
    {
        case FixedStringTextRepresentation::Raw:
        {
            memcpy(dst, src, n);
            return n;
        }
        case FixedStringTextRepresentation::Hex:
        {
            for (size_t i = 0; i != n; ++i)
                writeHexByteLowercase(src[i], dst + 2 * i);
            return n * 2;
        }
        case FixedStringTextRepresentation::Base64:
        case FixedStringTextRepresentation::Base64URL:
        {
            const bool url_encoding = text_representation == FixedStringTextRepresentation::Base64URL;
#if USE_SIMDUTF
            /// simdutf emits the base64url alphabet without padding for the URL variant, like base64URLEncode.
            return simdutf::binary_to_base64(reinterpret_cast<const char *>(src), n, dst, url_encoding ? simdutf::base64_url : simdutf::base64_default);
#else
            std::string encoded = base64Encode(std::string(reinterpret_cast<const char *>(src), n), url_encoding, /* no_padding */ url_encoding);
            memcpy(dst, encoded.data(), encoded.size());
            return encoded.size();
#endif
        }
        case FixedStringTextRepresentation::Base58:
        {
            auto * out = reinterpret_cast<UInt8 *>(dst);
            if (n == 32)
                return encodeBase58_32(src, out);
            if (n == 64)
                return encodeBase58_64(src, out);
            return encodeBase58(src, n, out);
        }
    }
}

MutableColumnPtr SerializationFixedStringWithTextRepresentation::encodeColumn(FixedStringTextRepresentation text_representation, const ColumnFixedString & column)
{
    const size_t n = column.getN();
    const size_t rows = column.size();
    const auto * src = column.getChars().data();

    auto result = ColumnString::create();
    auto & chars = result->getChars();
    auto & offsets = result->getOffsets();

    chars.resize(rows * maxEncodedSize(text_representation, n));
    offsets.resize(rows);

    size_t pos = 0;
    for (size_t i = 0; i < rows; ++i)
    {
        pos += encode(text_representation, n, src + i * n, reinterpret_cast<char *>(chars.data() + pos));
        offsets[i] = pos;
    }
    chars.resize(pos);

    return result;
}

bool SerializationFixedStringWithTextRepresentation::tryDecode(FixedStringTextRepresentation text_representation, size_t n, std::string_view encoded, UInt8 * dst)
{
    switch (text_representation)
    {
        case FixedStringTextRepresentation::Raw:
        {
            if (encoded.size() != n)
                return false;
            memcpy(dst, encoded.data(), n);
            return true;
        }
        case FixedStringTextRepresentation::Hex:
            return tryDecodeHex(encoded, n, dst);
        case FixedStringTextRepresentation::Base64:
            return tryDecodeBase64(encoded, n, dst, /* url_encoding */ false);
        case FixedStringTextRepresentation::Base64URL:
            return tryDecodeBase64(encoded, n, dst, /* url_encoding */ true);
        case FixedStringTextRepresentation::Base58:
            return tryDecodeBase58(encoded, n, dst);
    }
}

template <typename Callback>
void SerializationFixedStringWithTextRepresentation::withEncodedValue(const IColumn & column, size_t row_num, Callback && callback) const
{
    const auto * src = assert_cast<const ColumnFixedString &>(column).getChars().data() + n * row_num;

    PODArrayWithStackMemory<char, STACK_BUFFER_SIZE> buffer(maxEncodedSize(text_representation, n));
    const size_t size = encode(text_representation, n, src, buffer.data());
    callback(std::string_view(buffer.data(), size));
}

void SerializationFixedStringWithTextRepresentation::writeEncodedValue(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    const size_t max_size = maxEncodedSize(text_representation, n);
    ostr.nextIfAtEnd();
    if (ostr.available() < max_size)
    {
        withEncodedValue(column, row_num, [&](std::string_view encoded) { writeString(encoded, ostr); });
        return;
    }

    const auto * src = assert_cast<const ColumnFixedString &>(column).getChars().data() + n * row_num;
    ostr.position() += encode(text_representation, n, src, ostr.position());
}

bool SerializationFixedStringWithTextRepresentation::tryDecodeAndAppend(IColumn & column, std::string_view encoded) const
{
    auto & data = assert_cast<ColumnFixedString &>(column).getChars();
    const size_t old_size = data.size();
    data.resize(old_size + n);

    if (!tryDecode(text_representation, n, encoded, data.data() + old_size))
    {
        data.resize_assume_reserved(old_size);
        return false;
    }
    return true;
}

void SerializationFixedStringWithTextRepresentation::decodeAndAppend(IColumn & column, std::string_view encoded) const
{
    if (!tryDecodeAndAppend(column, encoded))
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "Cannot parse '{}{}' as FixedString({}, '{}'): expected a valid {} representation of exactly {} bytes",
            encoded.substr(0, MAX_VALUE_SIZE_IN_EXCEPTION), encoded.size() > MAX_VALUE_SIZE_IN_EXCEPTION ? "..." : "", n, fixedStringTextRepresentationToString(text_representation),
            fixedStringTextRepresentationToString(text_representation), n);
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

void SerializationFixedStringWithTextRepresentation::deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint) const
{
    nested->deserializeBinaryBulk(column, istr, limit, avg_value_size_hint);
}

/// The encoded alphabets (hex digits, Base64, Base64URL and Base58) do not contain characters
/// that must be escaped in the TSV, Values, CSV and XML formats, so the encoded text is written as is.

void SerializationFixedStringWithTextRepresentation::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeEncodedValue(column, row_num, ostr);
}

void SerializationFixedStringWithTextRepresentation::deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    String encoded;
    readStringUntilEOF(encoded, istr);
    decodeAndAppend(column, encoded);
}

bool SerializationFixedStringWithTextRepresentation::tryDeserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    String encoded;
    readStringUntilEOF(encoded, istr);
    return tryDecodeAndAppend(column, encoded);
}

void SerializationFixedStringWithTextRepresentation::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeEncodedValue(column, row_num, ostr);
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
    String encoded;
    if (settings.tsv.crlf_end_of_line_input)
        readEscapedStringCRLF(encoded, istr);
    else
        readEscapedString(encoded, istr);
    return tryDecodeAndAppend(column, encoded);
}

void SerializationFixedStringWithTextRepresentation::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeChar('\'', ostr);
    writeEncodedValue(column, row_num, ostr);
    writeChar('\'', ostr);
}

void SerializationFixedStringWithTextRepresentation::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    String encoded;
    readQuotedStringInto<true>(encoded, istr);
    decodeAndAppend(column, encoded);
}

bool SerializationFixedStringWithTextRepresentation::tryDeserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    String encoded;
    return tryReadQuotedStringInto<true>(encoded, istr) && tryDecodeAndAppend(column, encoded);
}

void SerializationFixedStringWithTextRepresentation::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    /// Only '/' of Base64 may need escaping, depending on output_format_json_escape_forward_slashes.
    if (text_representation == FixedStringTextRepresentation::Base64 && settings.json.escape_forward_slashes)
    {
        withEncodedValue(column, row_num, [&](std::string_view encoded) { writeJSONString(encoded, ostr, settings); });
        return;
    }

    writeChar('"', ostr);
    writeEncodedValue(column, row_num, ostr);
    writeChar('"', ostr);
}

void SerializationFixedStringWithTextRepresentation::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String encoded;
    readJSONStringInto(encoded, istr, settings.json);
    decodeAndAppend(column, encoded);
}

bool SerializationFixedStringWithTextRepresentation::tryDeserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String encoded;
    return tryReadJSONStringInto(encoded, istr, settings.json) && tryDecodeAndAppend(column, encoded);
}

void SerializationFixedStringWithTextRepresentation::serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeEncodedValue(column, row_num, ostr);
}

void SerializationFixedStringWithTextRepresentation::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    /// Quoted like String and FixedString values. The encoded alphabets do not contain '"'.
    writeChar('"', ostr);
    writeEncodedValue(column, row_num, ostr);
    writeChar('"', ostr);
}

void SerializationFixedStringWithTextRepresentation::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String encoded;
    readCSVStringInto(encoded, istr, settings.csv);
    decodeAndAppend(column, encoded);
}

bool SerializationFixedStringWithTextRepresentation::tryDeserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String encoded;
    readCSVStringInto<String, false, false>(encoded, istr, settings.csv);
    return tryDecodeAndAppend(column, encoded);
}

void SerializationFixedStringWithTextRepresentation::serializeTextMarkdown(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    /// Base64 and Base64URL contain '+', '-' and '_', which are special in Markdown.
    withEncodedValue(column, row_num, [&](std::string_view encoded)
    {
        if (settings.markdown.escape_special_characters)
            writeMarkdownEscapedString(encoded, ostr);
        else
            writeString(encoded, ostr);
    });
}

}
