#pragma once

#include <DataTypes/Serializations/SerializationNamed.h>
#include <DataTypes/Serializations/SimpleTextSerialization.h>

#include <Core/Field.h>


namespace DB
{

class SerializationQBit final : public SimpleTextSerialization
{
private:
    friend class ColumnQBit;

    /* Nested tuple serialization that handles all the FixedString columns */
    SerializationPtr nested;
    /* Size of the vector element: 16, 32, 64 */
    size_t element_size;
    /* Number of elements in the vector */
    size_t dimension;

    /// Helper template for serialization from Field tuple. Untransposes tuple data and writes floats
    template <typename Word, typename Val>
    void serializeFloatsFromQBitTuple(const Tuple & tuple, WriteBuffer & ostr) const;

    /// Helper template for deserialization to Field tuple. Reads floats and transposes to tuple
    template <typename FloatType>
    Tuple deserializeFloatsToQBitTuple(ReadBuffer & istr) const;

    /// Helper template for serialization. Untransposes QBit data and writes floats. The writer function allows to write in any format
    /// wanted (i.e. comma-separated for text serialization, binary for binary serialization)
    template <typename Word, typename Val, typename WriteFunc>
    void serializeFloatsFromQBit(const IColumn & column, size_t row_num, WriteFunc && write_func) const;

    /// Helper template for deserialization. Reads floats and transposes to QBit format
    template <typename FloatType, typename ReadFunc>
    void deserializeFloatsToQBit(IColumn & column, ReadFunc && read_func) const;


public:
    SerializationQBit(const SerializationPtr & nested_, size_t element_size_, size_t dimension_)
        : nested(nested_)
        , element_size(element_size_)
        , dimension(dimension_)
    {
    }

    void serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings &) const override;

    void deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings &) const override;

    void serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;

    void deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;

    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;

    /// Deserializes the string argument passed to QBit(...) and inserts the values in a column
    void deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, bool whole) const override;

    /// Delegated to SerializationTuple
    void enumerateStreams(EnumerateStreamsSettings & settings, const StreamCallback & callback, const SubstreamData & data) const override;

    void serializeBinaryBulkStatePrefix(
        const IColumn & column, SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const override;

    void serializeBinaryBulkStateSuffix(SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const override;

    void deserializeBinaryBulkStatePrefix(
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state,
        SubstreamsDeserializeStatesCache * cache) const override;

    void serializeBinaryBulkWithMultipleStreams(
        const IColumn & column,
        size_t offset,
        size_t limit,
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const override;

    void deserializeBinaryBulkWithMultipleStreams(
        ColumnPtr & column,
        size_t rows_offset,
        size_t limit,
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state,
        SubstreamsCache * cache) const override;

    /** Does bit transposition. This is how the bits are mapped
      *     1st bit of 1st element -> 1st bit of 1st word
      *     1st bit of 2nd element -> 2nd bit of 1st word
      *     ...
      *     1st bit of 16th element -> 16th bit of 1st word
      *     2nd bit of 1st element -> 1st bit of 2nd word
      *     etc
      * out_values is 0-ed out in the function, thus 0 initialising it outside is not necessary.
      * The input Floats are little-endian, the output is big-endian s.t. we save correct bits in the correct FixedString streams later.
      * TODO: needs a separate implementation for big-endian machines (Float inputs).
      */
    template <typename T>
    static void transposeBits(const T * __restrict src, T * __restrict dst, size_t length);

    /** For a given row of a FixedString column, reads the packed bytes of that column’s bit-plane and “scatters” each bit into the
      * correct positions of the dst buffer, reconstructing the original row-wise bit layout across all elements.
      * The bit_mask T(1) << (sizeof(T) * 8 - 1 - bit) selects which bit in each T to set.
      */
    template <typename T>
    static void untransposeBitPlane(const UInt8 * __restrict src, T * __restrict dst, size_t stride_len, T bit_mask);
};

}
