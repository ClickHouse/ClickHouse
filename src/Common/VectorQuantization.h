#pragma once

#include <base/types.h>

#include <cstddef>
#include <memory>
#include <string>
#include <string_view>

/// Stateless, data-independent quantization of dense vectors for fast (brute-force) KNN. Each method is a pure per-row
/// function of the vector (a fixed-seed random projection, a truncated prefix; no trained codebook), so the codes can be
/// produced on the write path (as a derived companion stream of the vector column) and the per-row approximate distance
/// computed at query time.
///
/// Methods: "turboquant", "rabitq", "int8", "mrl_int8", "mrl_bf16". "int8" stores one Lloyd-Max Int8 code per coordinate
/// (of the rotated, unit-variance vector) plus the per-vector L2 norm, with an asymmetric (full-precision query)
/// distance. The "mrl_*" (Matryoshka) methods keep only the leading `bits` dimensions (the prefix) - int8 with a
/// per-vector scale, or bfloat16 - and compute the distance on that prefix; here `bits` is the prefix length, not a bit
/// width. "turboquant" and "rabitq" pack their codes 8 coordinates to the byte, so they require `dimensions` to be a
/// multiple of 8; the others do not.
namespace DB::VectorQuantization
{

bool isSupportedMethod(std::string_view method);

/// Returns a human-readable error if (method, dimensions, bits) is not a valid configuration, or an empty string if it
/// is valid. Used to reject bad codec parameters at DDL time (e.g. dimensions not a multiple of 8 for the bit-packed
/// methods, or an out-of-range `bits` for "e8" - which would otherwise build a 2^bits codebook).
std::string validateParams(std::string_view method, size_t dimensions, size_t bits);

/// Size of one encoded vector in bytes for the given method/dimensions/bits.
size_t bytesPerVector(std::string_view method, size_t dimensions, size_t bits);

/// Encode one `dimensions`-element vector into `dst` (exactly `bytesPerVector` bytes).
void encode(std::string_view method, const float * vec, size_t dimensions, size_t bits, char * dst);

/// Opaque prepared query (defined in the .cpp) for computing approximate distances from codes.
struct Query;

/// Prepare the query state once for a reference vector; `is_l2` selects L2Distance vs cosineDistance for 'e8'.
std::shared_ptr<const Query> prepareQuery(std::string_view method, const float * ref, size_t dimensions, size_t bits, bool is_l2);

/// Approximate distance between the prepared query and one encoded vector (`code` is `bytesPerVector` bytes).
float distance(const Query & q, const char * code);

}
