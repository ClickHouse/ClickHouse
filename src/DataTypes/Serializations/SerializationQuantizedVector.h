#pragma once

#include <DataTypes/Serializations/SerializationWrapper.h>
#include <Compression/CompressionCodecQuantized.h>

namespace DB
{

class IDataType;

/// Serialization of a dense vector column (e.g. `Array(Float32)`) that is encoded with a `Quantized(...)` codec.
///
/// On top of the normal array streams (that contain the original, full-precision vectors), it writes one extra
/// stream storing a compact quantized code. The codes is exposed as subcolumn `<column>.quantized` of type
/// `FixedString(bytesPerVector)`. The code stream is only read when the subcolumn is explicitly requested.
///
/// Used by vector search to rank vectors quickly by reading only `<column>.quantized` + rescoring the
/// result vectors against the full-precision vectors, i.e. reading `<column>`.
class SerializationQuantizedVector final : public SerializationWrapper
{
public:
    SerializationQuantizedVector(const SerializationPtr & nested_, const QuantizedCodecParams & params_);

    static constexpr auto subcolumn_name = "quantized";

    /// Only for the `pq` method: the codebook, exposed as the subcolumn `<column>.product_quantization_codebook`.
    static constexpr auto product_quantization_subcolumn_name = "product_quantization_codebook";

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
    QuantizedCodecParams params;
    bool is_product_quantization;         /// trained Product Quantization (codebook + codes) vs data-independent codes
    size_t bytes_per_vector;
    DataTypePtr codes_type;               /// FixedString(bytes_per_vector)
    SerializationPtr codes_serialization; /// SerializationNamed(FixedString, "quantized", QuantizedCodes)

    /// `pq` only: the per-part trained codebook, written once per part as the `product_quantization_codebook` substream.
    size_t codebook_bytes = 0;            /// FixedString size of the flat codebook (ProductQuantization::codebookFloats * 4)
    DataTypePtr codebook_type;
    SerializationPtr codebook_serialization; /// SerializationNamed(FixedString, "product_quantization_codebook", ProductQuantizationCodebook)

    /// Encode rows [offset, offset + count) into a FixedString(bytes_per_vector) column. For `pq`, `codebook` is the
    /// trained centroids (`ProductQuantization`); for the data-independent methods it is null.
    ColumnPtr encodeCodes(const IColumn & column, size_t offset, size_t count, const float * codebook) const;

    /// `pq` only: train a codebook from up to `count` vectors starting at `offset` (the part's first block).
    std::vector<float> trainCodebook(const IColumn & column, size_t offset, size_t count) const;
};

}
