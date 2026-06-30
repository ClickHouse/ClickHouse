#pragma once

#include <base/types.h>

#include <cstddef>
#include <memory>
#include <vector>

/// Trained Product Quantization (PQ). A `dimensions`-element vector is split into `m` contiguous subspaces of
/// `d_sub = dimensions / m` coordinates each; per subspace we learn `k = 2^nbits` centroids with Lloyd k-means and
/// encode each sub-vector as the index of its nearest centroid. A vector becomes `m` codes (1 byte each when
/// `nbits <= 8`, else 2). Unlike the data-independent methods in `VectorQuantization`, the codebook is TRAINED from the
/// data, which is what gives PQ its recall on real datasets (e.g. SIFT).
///
/// Distance is asymmetric (ADC): the query is kept full-precision; a per-subspace lookup table of query-to-centroid
/// partial distances is precomputed once per (query, codebook), and each code is then `m` table lookups summed.
namespace DB::ProductQuantization
{

/// Number of centroids per subspace (k = 2^nbits).
size_t numCentroids(size_t nbits);

/// Bytes of the flat codebook: m * k * d_sub floats = k * dimensions floats (subspace-major, see `trainCodebook`).
size_t codebookFloats(size_t dimensions, size_t m, size_t nbits);

/// Bytes per encoded vector: `m` code bytes (1 if nbits <= 8, else 2).
size_t bytesPerVector(size_t dimensions, size_t m, size_t nbits);

/// Validate (dimensions, m, nbits); returns an error message or empty string if valid.
std::string validateParams(size_t dimensions, size_t m, size_t nbits);

/// Train `m` per-subspace codebooks (k = 2^nbits centroids of d_sub = dimensions/m coordinates each) from `n` sample
/// vectors via Lloyd k-means. Returns the flat codebook of `codebookFloats` entries, laid out subspace-major:
/// centroid k of subspace mm, coordinate i, is at `out[(mm * k_count + k) * d_sub + i]`.
std::vector<float> trainCodebook(const float * vectors, size_t n, size_t dimensions, size_t m, size_t nbits, UInt64 seed = 0);

/// Opaque prepared encoder (defined in the .cpp): the codebook in the kernel's column-major layout, the centroid
/// squared norms the reformulated nearest-centroid argmin reuses, and per-vector scratch. Build once per codebook with
/// `prepareEncoder` and reuse for every vector. Not thread-safe: it carries per-vector scratch, so one encoder serves
/// one writer thread.
struct Encoder;

/// Prepare an encoder for a codebook; the codebook is copied into the encoder, so it need not outlive the encoder.
std::shared_ptr<Encoder> prepareEncoder(const float * codebook, size_t dimensions, size_t m, size_t nbits);

/// Encode one vector into `m` codes (exactly `bytesPerVector` bytes) with a prepared encoder. Use this for bulk
/// encoding so the per-codebook setup is amortized across rows.
void encode(Encoder & encoder, const float * vec, char * dst);

/// Encode one `dimensions`-element vector into `m` codes written to `dst` (exactly `bytesPerVector` bytes). Convenience
/// one-shot that builds an `Encoder` internally; prefer `prepareEncoder` + `encode(encoder, ...)` for many vectors.
void encode(const float * codebook, size_t dimensions, size_t m, size_t nbits, const float * vec, char * dst);

/// Opaque prepared query (defined in the .cpp): the per-subspace ADC lookup tables for one query against one codebook.
struct Query;

/// Prepare the ADC state once for a reference vector and codebook; `is_l2` selects L2Distance vs cosineDistance.
std::shared_ptr<const Query>
prepareQuery(const float * codebook, size_t dimensions, size_t m, size_t nbits, const float * query, bool is_l2);

/// Approximate distance between the prepared query and one encoded vector (`code` is `bytesPerVector` bytes).
float distance(const Query & query, const char * code);

}
