#pragma once

#include <DataTypes/Serializations/SerializationWrapper.h>
#include <Compression/CompressionCodecQuantize.h>

namespace DB
{

class IDataType;

/// Serialization of a dense vector column (e.g. `Array(Float32)`) that carries a `Quantize(...)` codec.
///
/// On top of the normal full-precision array streams it writes one extra on-disk stream holding a compact,
/// data-independent quantized code per row (see `Common/VectorQuantization.h`). The codes are derived from the
/// full-precision data at write time and exposed as the readable subcolumn `<column>.quantized` of type
/// `FixedString(bytesPerVector)`. The full-precision array reconstructs from the float streams alone; the code stream
/// is read only when the subcolumn is explicitly requested.
///
/// This lets a vector search rank cheaply over the small codes (reading only `<column>.quantized`) and rescore the
/// shortlist against the full-precision vectors (reading `<column>`), without a second user-declared column.
class SerializationQuantizedVector final : public SerializationWrapper
{
public:
    SerializationQuantizedVector(const SerializationPtr & nested_, const QuantizeCodecParams & params_);

    static constexpr auto subcolumn_name = "quantized";
    /// For the trained `pq` method only: the per-part codebook, exposed as the readable subcolumn `<column>.pq_codebook`.
    static constexpr auto pq_codebook_subcolumn_name = "pq_codebook";

    void enumerateStreams(
        EnumerateStreamsSettings & settings,
        const StreamCallback & callback,
        const SubstreamData & data) const override;

    /// Overridden only for `pq` (a trained codebook needs per-part write state); other methods keep the stateless path.
    void serializeBinaryBulkStatePrefix(
        const IColumn & column,
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const override;

    void serializeBinaryBulkWithMultipleStreams(
        const IColumn & column,
        size_t offset,
        size_t limit,
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const override;

    void serializeBinaryBulkStateSuffix(
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const override;

private:
    QuantizeCodecParams params;
    bool is_pq;                           /// trained Product Quantization (codebook + codes) vs data-independent codes
    size_t bytes_per_vector;
    DataTypePtr codes_type;               /// FixedString(bytes_per_vector)
    SerializationPtr codes_serialization; /// SerializationNamed(FixedString, "quantized", QuantizedCodes)

    /// `pq` only: the per-part trained codebook, written once per part as the `pq_codebook` substream.
    size_t codebook_bytes = 0;            /// FixedString size of the flat codebook (ProductQuantization::codebookFloats * 4)
    DataTypePtr codebook_type;
    SerializationPtr codebook_serialization; /// SerializationNamed(FixedString, "pq_codebook", PQCodebook)

    /// Encode rows [offset, offset + count) into a FixedString(bytes_per_vector) column. For `pq`, `codebook` is the
    /// trained centroids (`ProductQuantization`); for the data-independent methods it is null.
    ColumnPtr encodeCodes(const IColumn & column, size_t offset, size_t count, const float * codebook) const;

    /// `pq` only: train a codebook from up to `count` vectors starting at `offset` (the part's first block).
    std::vector<float> trainCodebook(const IColumn & column, size_t offset, size_t count) const;
};

}
