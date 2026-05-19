#pragma once

#include <DataTypes/FixedEncodedText.h>
#include <DataTypes/Serializations/ISerialization.h>

namespace DB
{

class SerializationFixedEncodedText final : public ISerialization
{
private:
    FixedEncodedTextKind kind;
    size_t n;
    SerializationPtr nested;

    SerializationFixedEncodedText(FixedEncodedTextKind kind_, size_t n_);

    String encodeValue(const IColumn & column, size_t row_num) const;
    void decodeAndAppend(IColumn & column, std::string_view encoded) const;

public:
    static UInt128 getHash(FixedEncodedTextKind kind_, size_t n_);
    static SerializationPtr create(FixedEncodedTextKind kind_, size_t n_);

    void serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings & settings) const override;
    void serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;

    void serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const override;
    void deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t rows_offset, size_t limit, double avg_value_size_hint) const override;

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
