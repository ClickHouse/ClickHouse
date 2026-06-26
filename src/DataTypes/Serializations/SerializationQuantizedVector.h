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

    void enumerateStreams(
        EnumerateStreamsSettings & settings,
        const StreamCallback & callback,
        const SubstreamData & data) const override;

    void serializeBinaryBulkWithMultipleStreams(
        const IColumn & column,
        size_t offset,
        size_t limit,
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const override;

private:
    QuantizeCodecParams params;
    size_t bytes_per_vector;
    DataTypePtr codes_type;            /// FixedString(bytes_per_vector)
    SerializationPtr codes_serialization; /// SerializationNamed(FixedString, "quantized", QuantizedCodes)

    /// Encode rows [offset, offset + count) of the dense vector column into a FixedString(bytes_per_vector) column.
    ColumnPtr encodeCodes(const IColumn & column, size_t offset, size_t count) const;
};

}
