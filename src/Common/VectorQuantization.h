#pragma once

#include <base/types.h>

#include <cstddef>
#include <memory>
#include <string_view>

/// Stateless, data-independent quantization of dense vectors for fast (brute-force) KNN. Each method is a pure per-row
/// function of the vector (a fixed-seed random projection / the data-independent E8 lattice; no trained codebook), so
/// the codes can be produced on the write path (as a derived companion stream of the vector column) and the per-row
/// approximate distance computed at query time.
///
/// Methods: "b1", "b1_projected", "turboquant", "rabitq", "e8", "int8". `bits` is only used by "e8" (bits per 8-dim
/// sub-quantizer, 1..16); ignored otherwise. "int8" stores one Lloyd-Max Int8 code per coordinate (of the rotated,
/// unit-variance vector) plus the per-vector L2 norm, and uses an asymmetric (full-precision query) distance.
namespace DB::VectorQuantization
{

bool isSupportedMethod(std::string_view method);

/// Size of one encoded vector in bytes for the given method/dimensions/bits.
size_t bytesPerVector(std::string_view method, size_t dimensions, size_t bits);

/// Encode one `dimensions`-element vector into `dst` (exactly `bytesPerVector` bytes).
void encode(std::string_view method, const float * vec, size_t dimensions, size_t bits, char * dst);

/// Opaque prepared query (defined in the .cpp) for computing approximate distances from codes.
struct Query;

/// Prepare the query state once for a reference vector; `is_l2` selects L2Distance vs cosineDistance for 'e8'.
std::shared_ptr<const Query> prepareQuery(std::string_view method, const float * ref, size_t dimensions, size_t bits, bool is_l2);

/// Approximate distance between the prepared query and one encoded vector (`code` is `bytesPerVector` bytes).
float distance(const Query & query, const char * code);

}
