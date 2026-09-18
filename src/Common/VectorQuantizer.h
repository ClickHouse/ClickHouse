#pragma once

#include <base/types.h>

#include <cstddef>
#include <memory>
#include <string>
#include <string_view>

namespace DB
{

/// Stateless, data-independent quantization of dense floating-point vectors.
///
/// Methods:
/// - turboquant
/// - rabitq
/// - int8
/// - prefix_int8 (Matryoshka)
/// - prefix_bf16 (Matryoshka)
struct VectorQuantizer
{
    /// Whether the method's approximate distance can rank by L2, i.e. if it retains/estimates the vector norm.
    static bool supportsL2(std::string_view method);

    /// Returns a human-readable non-empty error if an invalid configuration is given, or an empty string if valid.
    static std::string validateParams(std::string_view method, size_t dimensions, size_t bits);

    /// Size of one encoded vector in bytes for the given method/dimensions/bits.
    static size_t bytesPerVector(std::string_view method, size_t dimensions, size_t bits);

    struct Encoder;
    /// Out-of-line deleter (defined in the .cpp) so the opaque `Encoder` can be owned by unique_ptr through this header.
    struct EncoderDeleter { void operator()(Encoder * ptr) const noexcept; };
    using EncoderPtr = std::unique_ptr<Encoder, EncoderDeleter>;

    /// Prepare an encoder for a method/dimensions/bits
    static EncoderPtr createEncoder(std::string_view method, size_t dimensions, size_t bits);

    /// Encode one `dimensions`-element vector into `dst` (exactly `bytesPerVector` bytes) with a prepared encoder.
    static void encode(Encoder & encoder, const float * vec, char * dst);

    /// Encode one `dimensions`-element vector into `dst` (exactly `bytesPerVector` bytes).
    /// Expensive as it builds an `Encoder` internally, prefer `createEncoder` + `encode(encoder, ...)` for many vectors.
    static void encode(std::string_view method, const float * vec, size_t dimensions, size_t bits, char * dst);

    struct Query;
    struct QueryDeleter { void operator()(const Query * ptr) const noexcept; };
    using QueryPtr = std::unique_ptr<const Query, QueryDeleter>;

    /// Prepare the query state once for a reference vector; `is_l2` selects L2Distance vs cosineDistance for the
    /// norm-retaining methods (`int8`, `prefix_int8`, `prefix_bf16`); the cosine-only methods (`rabitq`, `turboquant`) ignore it.
    static QueryPtr prepareQuery(std::string_view method, const float * ref, size_t dimensions, size_t bits, bool is_l2);

    /// Calculates the approximate distance between the prepared query and one encoded vector (`code` is `bytesPerVector` bytes).
    static float distance(const Query & query, const char * code);
};

/// An in-memory index for approximate nearest-neighbour search over a fixed set of vectors.
///
/// Each vector is stored as the `rabitq` code of `VectorQuantizer` - one sign bit per coordinate plus a
/// correction factor - so a query is scored with AND+popcount over `dimensions` bits instead of a
/// `dimensions`-long dot product, about 10x less time per (query, vector) pair at 768 dimensions. Results
/// are approximate; rescore them against the full-precision vectors.
class RaBitQIndex
{
public:
    /// `rabitq` packs sign bits 8 to the byte and needs the dimension to be a multiple of 8.
    static bool supportsDimensions(size_t dimensions);

    /// `vectors` is row-major, `count` x `dimensions`. Only the codes and three scalars per vector are kept;
    /// the caller keeps the full-precision vectors for rescoring.
    RaBitQIndex(const float * vectors, size_t count, size_t dimensions);
    ~RaBitQIndex();

    size_t size() const;

    /// For each row-major query, writes the positions of the `k` vectors with the smallest ESTIMATED squared
    /// L2 distance into `out[query * k .. query * k + k)`, unordered. `k` must be in [1, size()].
    void nearestByL2(const float * queries, size_t num_queries, size_t k, UInt32 * out) const;

    /// Half-open range of positions.
    struct PositionRange
    {
        UInt32 begin = 0;
        UInt32 end = 0;
    };

    /// `nearestByL2` over only the positions the `ranges` cover, for a single query - callers of this give
    /// every query its own ranges, so there is nothing to batch. Returns how many positions were written,
    /// which is below `k` only if the ranges hold fewer. Ranges need not be sorted or aligned, though the
    /// scan works 8 codes at a time and ranges aligned to 8 waste nothing.
    size_t nearestByL2InRanges(const float * query, const PositionRange * ranges, size_t num_ranges, size_t k, UInt32 * out) const;

private:
    struct Data;
    std::unique_ptr<Data> data;
};

}
