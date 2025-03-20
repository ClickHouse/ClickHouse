#pragma once

#include <DataTypes/Serializations/ISerialization.h>
#include <DataTypes/Serializations/SerializationTuple.h>


namespace DB
{

class SerializationArrayT final : public SimpleTextSerialization
{
public:
    using ElementSerializationPtr = std::shared_ptr<const SerializationNamed>;
    using ElementSerializations = std::vector<ElementSerializationPtr>;

private:
    friend class ColumnArrayT;

    /* Separate serialization for each FixedString column */
    ElementSerializations elems;
    /* Size of the vector element: 16, 32, 64 */
    size_t size;
    /* Number of elements in the vector */
    size_t n;

public:
    SerializationArrayT(const ElementSerializations & elems_, size_t size_, size_t n_)
        : elems(elems_)
        , size(size_)
        , n(n_)
    {
    }

    void serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings & settings) const override;

    void deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings & settings) const override;

    void serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;

    void deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;

    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;

    /* Deserializes the string argument passed to ArrayT(...) and inserts the values in a column */
    void deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, bool whole) const override;

    template <typename FloatType>
    void readFloatsAndExtractBytes(ReadBuffer & istr, std::vector<char> & value_bytes) const;

    const ElementSerializations & getElementSerialization() const { return elems; }
};

}
