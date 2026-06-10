#pragma once

#include <Core/TypeId.h>
#include <DataTypes/Serializations/SimpleTextSerialization.h>

namespace DB
{

/** Serialization for the Vector(T, N) data type.
  *
  * Binary representation is FLAT: the N * sizeof(T) contiguous little-endian bytes of each vector, with
  * no offsets and no per-value size prefix (the dimension is part of the type). This is the storage
  * saving over Array(T), which additionally carries an offsets stream.
  *
  * Text representation is the array-like `[v0, v1, ..., v_{N-1}]`, so the type round-trips with the
  * familiar array literal syntax used on INSERT and SELECT.
  */
class SerializationVector final : public SimpleTextSerialization
{
private:
    /// Type of one vector element: BFloat16, Float32 or Float64.
    TypeIndex element_type;
    /// Number of elements in each vector.
    size_t dimension;

    SerializationVector(TypeIndex element_type_, size_t dimension_)
        : element_type(element_type_)
        , dimension(dimension_)
    {
    }

public:
    static SerializationPtr create(TypeIndex element_type_, size_t dimension_);
    static UInt128 getHash(TypeIndex element_type_, size_t dimension_);

    void serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings &) const override;
    void serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;

    void serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const override;
    void deserializeBinaryBulk(
        IColumn & column, ReadBuffer & istr, size_t rows_offset, size_t limit, double avg_value_size_hint) const override;

    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, bool whole) const override;

    /// CSV cannot hold the `[...]` text as-is: the value is written and read as a quoted CSV string.
    void serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;
};

}
