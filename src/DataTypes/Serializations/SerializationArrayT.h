#pragma once

#include <DataTypes/Serializations/ISerialization.h>
#include <DataTypes/Serializations/SerializationTuple.h>


namespace DB
{

class SerializationArrayT final : public SimpleTextSerialization
{
private:
    friend class ColumnArrayT;
    SerializationPtr nested_serialization;
    size_t size;
    UInt64 n;

public:
    SerializationArrayT(size_t size, UInt64 n_);

    void serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings & settings) const override;

    void deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings & settings) const override;

    void serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;

    void deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;

    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;

    /* Deserializes the string argument passed to ArrayT(...) and inserts the values in a column */
    void deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, bool whole) const override;

    template <typename FloatType>
    void readFloatsAndExtractBytes(ReadBuffer & istr, std::vector<char> & value_bytes) const;

    const SerializationPtr & getElementSerialization() const { return nested_serialization; }
};

}
