#pragma once

#include <DataTypes/Serializations/SimpleTextSerialization.h>

namespace DB
{

/** Serialization for the Vector(T, N) data type.
  *
  * Binary representation is FLAT: the N * sizeof(T) contiguous bytes of each vector, with no offsets
  * (a single stream, delegated to the wrapped ColumnFixedString). This is the storage saving over
  * Array(T), which additionally carries an offsets stream.
  *
  * Text representation is the array-like `[v0, v1, ..., v_{N-1}]`, so the type round-trips with the
  * familiar array literal syntax used on INSERT and SELECT.
  */
class SerializationVector final : public SimpleTextSerialization
{
private:
    /// Serialization of the underlying ColumnFixedString(element_size * dimension), used for binary I/O.
    SerializationPtr nested;
    /// Size of one vector element in bytes: 2 (BFloat16), 4 (Float32), 8 (Float64).
    size_t element_size;
    /// Number of elements in each vector.
    size_t dimension;

    SerializationVector(const SerializationPtr & nested_, size_t element_size_, size_t dimension_)
        : nested(nested_)
        , element_size(element_size_)
        , dimension(dimension_)
    {
    }

    /// Dispatch a generic lambda on the concrete element type given element_size.
    template <typename Func>
    void dispatchByElementSize(Func && func) const;

public:
    static SerializationPtr create(size_t element_size_, size_t dimension_);

    void serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings &) const override;
    void serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;

    void serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const override;
    void deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t rows_offset, size_t limit, double avg_value_size_hint) const override;

    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, bool whole) const override;
};

}
