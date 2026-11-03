#pragma once

#include <Compression/ICompressionCodec.h>

#include <cstddef>
#include <optional>

namespace DB
{

/// Parameters of the `Quantized` column codec.
struct QuantizedCodecParams
{
    String method;            /// The quantization method
    size_t dimensions = 0;    /// The vector length
    size_t bits = 0;          /// Overloaded: the number of leading dimensions kept by the `prefix_*` methods, and nbits per subspace for `product`.
    size_t m = 0;             /// The number of subspaces, used only by the trained `product` (Product Quantization) method.
};

/// `Quantized(method, dimensions[, bits])` is a column codec for dense vector columns (`Array(Float32)` and friends).
///
/// At the byte level the codec does nothing - the full-precision data is stored verbatim, exactly like `NONE` (hence
/// `isNone() == true`). Its purpose is purely declarative: its presence on a vector column makes
/// `SerializationQuantizedVector` write a compact and data-independent quantized companion stream alongside the
/// full-precision vectors, exposed as a readable subcolumn `<column>.quantized`. Vector search can then rank
/// cheaply over the quantized vectors and rescore the results against the full-precision vectors.
class CompressionCodecQuantized : public ICompressionCodec
{
public:
    explicit CompressionCodecQuantized(const QuantizedCodecParams & params_);

    uint8_t getMethodByte() const override;
    void updateHash(SipHash & hash) const override;

    const QuantizedCodecParams & getParams() const { return params; }

protected:
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;
    UInt32 doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override;

    bool isCompression() const override { return false; }
    bool isGenericCompression() const override { return false; }
    bool isNone() const override { return true; } /// see the class-level comment
    bool isExperimental() const override { return true; }

    String getDescription() const override
    {
        return "Stores a compact quantized companion stream of a dense vector column for fast nearest-neighbour search; "
               "the full-precision data is stored uncompressed.";
    }

private:
    QuantizedCodecParams params;
};

std::optional<QuantizedCodecParams> tryExtractQuantizedCodecParams(const ASTPtr & codec_desc);

}
