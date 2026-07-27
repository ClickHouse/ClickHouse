#pragma once

#include <base/types.h>

#include <cstddef>
#include <memory>
#include <vector>

/// Product Quantization (PQ). A `dimensions`-element vector is split into `m` contiguous subspaces of
/// `d_sub = dimensions / m` coordinates each; per subspace we learn `k = 2^nbits` centroids with Lloyd k-means and
/// encode each sub-vector as the index of its nearest centroid. A vector becomes `m` codes (1 byte each when
/// `nbits <= 8`, else 2). Unlike the data-independent methods in `VectorQuantization`, the codebook is TRAINED from the
/// data.
namespace DB
{

struct ProductQuantizer
{
    /// Number of centroids per subspace (k = 2^nbits).
    static size_t numCentroids(size_t nbits);

    /// Bytes of the flat codebook: m * k * d_sub floats = k * dimensions floats (subspace-major, see `trainCodebook`).
    static size_t codebookFloats(size_t dimensions, size_t m, size_t nbits);

    /// Bytes per encoded vector: `m` code bytes (1 if nbits <= 8, else 2).
    static size_t bytesPerVector(size_t dimensions, size_t m, size_t nbits);

    /// Returns a human-readable non-empty error if an invalid configuration is given, or an empty string if valid.
    static std::string validateParams(size_t dimensions, size_t m, size_t nbits);

    /// Train `m` per-subspace codebooks (k = 2^nbits centroids of d_sub = dimensions/m coordinates each) from `n` sample
    /// vectors via Lloyd k-means. Returns the flat codebook of `codebookFloats` entries, laid out subspace-major:
    /// centroid k of subspace mm, coordinate i, is at `out[(mm * k_count + k) * d_sub + i]`.
    static std::vector<float> trainCodebook(const float * vectors, size_t n, size_t dimensions, size_t m, size_t nbits, UInt64 seed = 0);

    struct Encoder;
    /// Out-of-line deleter (defined in the .cpp) so the opaque `Encoder` can be owned by unique_ptr through this header.
    struct EncoderDeleter { void operator()(Encoder * ptr) const noexcept; };
    using EncoderPtr = std::unique_ptr<Encoder, EncoderDeleter>;

    /// Prepare an encoder for a codebook; the codebook is copied into the encoder, so it need not outlive the encoder.
    static EncoderPtr createEncoder(const float * codebook, size_t dimensions, size_t m, size_t nbits);

    /// Encode one vector into `m` codes (exactly `bytesPerVector` bytes) with a prepared encoder. Use for bulk encoding.
    static void encode(Encoder & encoder, const float * vec, char * dst);

    /// Encode one `dimensions`-element vector into `m` codes written to `dst` (exactly `bytesPerVector` bytes).
    /// Expensive as it builds an `Encoder` internally, prefer `createEncoder` + `encode(encoder, ...)` for many vectors.
    static void encode(const float * codebook, size_t dimensions, size_t m, size_t nbits, const float * vec, char * dst);

    struct Query;
    struct QueryDeleter { void operator()(const Query * ptr) const noexcept; };
    using QueryPtr = std::unique_ptr<const Query, QueryDeleter>;

    /// Prepare the ADC state once for a reference vector and codebook; `is_l2` selects L2Distance vs cosineDistance.
    static QueryPtr
    prepareQuery(const float * codebook, size_t dimensions, size_t m, size_t nbits, const float * query, bool is_l2);

    /// Calculates the approximate distance between the prepared query and one encoded vector (`code` is `bytesPerVector` bytes).
    static float distance(const Query & query, const char * code);
};

}
