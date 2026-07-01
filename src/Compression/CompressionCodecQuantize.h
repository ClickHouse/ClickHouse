#pragma once

#include <Compression/ICompressionCodec.h>

#include <cstddef>
#include <optional>

namespace DB
{

/// Parameters of the `Quantize` column codec. `method` is the quantization method; `dimensions` the vector length.
/// `bits` is the number of bits per sub-quantizer (used by `e8`, and as nbits per subspace by `pq`). `m` is the number
/// of subspaces, used only by the trained `pq` (Product Quantization) method.
struct QuantizeCodecParams
{
    String method;
    size_t dimensions = 0;
    size_t bits = 0;
    size_t m = 0;
};

/// `Quantize(method, dimensions[, bits])` is a column codec for dense vector columns (`Array(Float32)` and friends).
///
/// At the byte level it does nothing - the full-precision data is stored verbatim, exactly like `NONE` (hence
/// `isNone() == true`). Its purpose is purely declarative: its presence on a vector column makes
/// `SerializationQuantizedVector` write a compact, data-independent quantized companion stream alongside the
/// full-precision data, exposed as the readable subcolumn `<column>.quantized`. A two-stage vector search can then rank
/// cheaply over the small codes and rescore the shortlist against the full-precision vectors.
///
/// Because it behaves like `NONE`, it must be used on its own: `CODEC(Quantize('rabitq', 64))`. The full-precision
/// vectors are always stored uncompressed, which is the norm for dense embeddings (they do not compress).
///
/// The codec is gated behind the `allow_experimental_codecs` setting (`isExperimental() == true`).
class CompressionCodecQuantize : public ICompressionCodec
{
public:
    explicit CompressionCodecQuantize(const QuantizeCodecParams & params_);

    uint8_t getMethodByte() const override;
    void updateHash(SipHash & hash) const override;

    const QuantizeCodecParams & getParams() const { return params; }

protected:
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;
    UInt32 doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override;

    bool isCompression() const override { return false; }
    bool isGenericCompression() const override { return false; }
    /// At the byte level the full-precision stream is stored verbatim. The companion code stream is produced by the
    /// serialization, not by this codec.
    bool isNone() const override { return true; }
    /// Gated behind the `allow_experimental_codecs` setting.
    bool isExperimental() const override { return true; }

    String getDescription() const override
    {
        return "Stores a compact quantized companion stream of a dense vector column for fast nearest-neighbour search; "
               "the full-precision data is stored uncompressed.";
    }

private:
    QuantizeCodecParams params;
};

/// If the given column codec description (the `CODEC(...)` AST) contains a `Quantize(...)` codec, return its parameters.
std::optional<QuantizeCodecParams> tryExtractQuantizeCodecParams(const ASTPtr & codec_desc);

}
