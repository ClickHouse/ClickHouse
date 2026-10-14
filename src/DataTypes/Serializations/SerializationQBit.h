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
    template <typename FloatType>
    void serializeFloatsFromQBitTuple(const Tuple & tuple, WriteBuffer & ostr) const;

    /// Helper template for deserialization to Field tuple. Reads floats and transposes to tuple
    template <typename FloatType>
    Tuple deserializeFloatsToQBitTuple(ReadBuffer & istr) const;

    /// Helper template for serialization. Untransposes QBit data and writes floats. The writer function allows to write in any format
    /// wanted (i.e. comma-separated for text serialization, binary for binary serialization)
    template <typename FloatType, typename WriteFunc>
    void serializeFloatsFromQBit(const IColumn & column, size_t row_num, WriteFunc && write_func) const;

    /// Helper template for deserialization. Reads floats and transposes to QBit format
    template <typename FloatType, typename ReadFunc>
    void deserializeFloatsToQBit(IColumn & column, ReadFunc read) const;

    /// Helper function to validate and read QBit size from buffer for binary deserialization
    size_t validateAndReadQBitSize(ReadBuffer & istr, const FormatSettings & settings) const;

    /// Helper function to dispatch based on element_size to the appropriate float type
    template <typename Func>
    void dispatchByElementSize(Func && func) const;


public:
    SerializationQBit(const SerializationPtr & nested_, size_t element_size_, size_t dimension_)
        : nested(nested_)
        , element_size(element_size_)
        , dimension(dimension_)
    {
    }

    void serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings &) const override;

    void deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings & settings) const override;

    void serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;

    void deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;

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

    /** Does bit transposition. This is general (inaccurate) idea of how the bits are mapped
      *     1st bit of 1st element -> 1st bit of 1st word
      *     1st bit of 2nd element -> 2nd bit of 1st word
      *     ...
      *     1st bit of 16th element -> 16th bit of 1st word
      *     2nd bit of 1st element  -> 1st bit of 2nd word
      *     etc.
      *
      * In practice, this is what happens
      *
      *   f_{i,j} = bit j of float i
      *
      *       ◄── First FixedString Column ──►   ◄── Second FixedString Column ──►
      *                                        │
      *     ╔════════════════╤════════════════╗ ╔════════════════╤════════════════╗
      *     ║  Upper Byte    │  Lower Byte    ║ ║  Upper Byte    │  Lower Byte    ║
      * ... ╟────────────────┼────────────────╢ ╟────────────────┼────────────────╢ ...
      *     ║ f₀,₈ ... f₀,₁₅ │ f₀,₀ ... f₀,₇  ║ ║ f₁,₈ ... f₁,₁₅ │ f₁,₀ ... f₁,₇  ║
      *     ╚════════════════╧════════════════╝ ╚════════════════╧════════════════╝
      *
      * Notes:
      *  - transposeBits() is a per-value kernel: it maps one float at row i into the bit planes.
      *  - FixedString columns are MSB first, so first column contains the most significant bits of all Floats.
      *  - Bit planes are emitted MSB→LSB overall (higher j first).
      *  - Within each 8-row pack the row order is flipped (…, 16..23, 8..15, 0..7).
      *  - Motivation: this is faster to unpack in the scalar algorithm.
      */
    template <class Word>
    static void transposeBits(Word src, size_t row_i, size_t total_bits, char * const * __restrict dst);

    /** For a given FixedString column, reads the packed bytes of that column’s bit plane and “scatters” each bit into the
      * correct positions of the dst buffer, reconstructing the original row-wise bit layout across all elements.
      * The bit_mask T(1) << (sizeof(T) * 8 - 1 - bit) selects which bit in each T to set.
      */
    template <typename T>
    static void untransposeBitPlane(const UInt8 * __restrict src, T * __restrict dst, size_t stride_len, T bit_mask);
};

}
