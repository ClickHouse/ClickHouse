#pragma once

#include <DataTypes/IDataType.h>


namespace DB
{

class DataTypeArrayT final : public IDataType
{
private:
    /* Type of the elements in the vector: BFloat16, Float32, Float64 */
    const DataTypePtr type;
    /* Size of the vector element: 16, 32, 64 */
    const size_t size;
    /* Number of elements in the vector */
    const size_t n;

public:
    DataTypeArrayT(const DataTypePtr & type_, size_t size, size_t n);

    TypeIndex getTypeId() const override { return TypeIndex::ArrayT; }
    std::string doGetName() const override;
    const char * getFamilyName() const override { return "ArrayT"; }

    MutableColumnPtr createColumn() const override;

    void insertDefaultInto(IColumn & column) const override;

    Field getDefault() const override;
    bool equals(const IDataType & rhs) const override;

    bool isParametric() const override { return true; }
    bool haveSubtypes() const override { return true; }

    const DataTypePtr & getType() const { return type; }
    size_t getSize() const { return size; }
    size_t getN() const { return n; }
    size_t getSizeOfValueInMemory() const override { return (size / 8) * n; }

    SerializationPtr doGetDefaultSerialization() const override;
};

}
