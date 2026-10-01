#pragma once

#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/Serializations/ISerialization.h>

#include <string_view>

namespace DB
{

class ColumnFixedString;

/** Serialization of FixedString(N, 'representation') where representation is not 'Raw'.
  *
  * The value is always stored as exactly N raw bytes, so all binary serializations
  * (including the on-disk format) are delegated to SerializationFixedString and are identical
  * to plain FixedString(N).
  *
  * Only the text formats differ: the value is encoded to Hex / Base64 / Base64URL / Base58 on output,
  * and text input is decoded and must produce exactly N bytes, otherwise an exception is thrown
  * (or false is returned by the tryDeserialize* methods).
  */
class SerializationFixedStringWithTextRepresentation final : public ISerialization
{
private:
    FixedStringTextRepresentation text_representation;
    size_t n;
    SerializationPtr nested;

    SerializationFixedStringWithTextRepresentation(FixedStringTextRepresentation text_representation_, size_t n_);

    /// Encodes the value and passes the encoded text to `callback`. Does not allocate for small N.
    template <typename Callback>
    void withEncodedValue(const IColumn & column, size_t row_num, Callback && callback) const;

    /// Writes the encoded value as is, directly into the buffer if it has enough space.
    void writeEncodedValue(const IColumn & column, size_t row_num, WriteBuffer & ostr) const;

    /// Decodes `encoded` and appends it to the column. Returns false if the text is not a valid
    /// representation of exactly N bytes; in this case the column is left unchanged.
    bool tryDecodeAndAppend(IColumn & column, std::string_view encoded) const;
    void decodeAndAppend(IColumn & column, std::string_view encoded) const;

public:
    static UInt128 getHash(FixedStringTextRepresentation text_representation_, size_t n_);
    static SerializationPtr create(FixedStringTextRepresentation text_representation_, size_t n_);

    /// Decodes `encoded` into exactly `n` bytes at `dst`. Never writes more than `n` bytes.
    /// Returns false if `encoded` is not a valid representation of exactly `n` bytes.
    static bool tryDecode(FixedStringTextRepresentation text_representation, size_t n, std::string_view encoded, UInt8 * dst);

    /// Upper bound of the encoded length of an `n` bytes value.
    static size_t maxEncodedSize(FixedStringTextRepresentation text_representation, size_t n);

    /// Encodes `n` bytes from `src` into `dst` which must have at least maxEncodedSize() bytes. Returns the encoded length.
    static size_t encode(FixedStringTextRepresentation text_representation, size_t n, const UInt8 * src, char * dst);

    /// Encodes all values of the column into a ColumnString, e.g. for toString.
    static MutableColumnPtr encodeColumn(FixedStringTextRepresentation text_representation, const ColumnFixedString & column);

    void serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings & settings) const override;
    void serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;

    void serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const override;
    void deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint) const override;

    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;
    bool tryDeserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;

    void serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;
    bool tryDeserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;

    void serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;
    bool tryDeserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;

    void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;
    bool tryDeserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;

    void serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;

    void serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;
    bool tryDeserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;

    void serializeTextMarkdown(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;
};

}
