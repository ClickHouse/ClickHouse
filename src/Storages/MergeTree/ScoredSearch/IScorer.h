#pragma once
#include <Core/Types.h>
#include <roaring/roaring.hh>
#include <Interpreters/Context_fwd.h>

namespace DB
{

class IMergeTreeDataPart;
using DataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;

struct IMergeTreeIndex;
using MergeTreeIndexPtr = std::shared_ptr<const IMergeTreeIndex>;

/// Rows the scorer may consider for one part stored in a bitmap.
using Prefilter = const roaring::Roaring *;

/// Sort direction of a row-scorer's top-K merge.
enum class ScoreDirection : uint8_t
{
    Ascending,
    Descending,
};

/// Base interface for scorers for Top-K scored-search queries.
/// Can score rows from single part, or aggregates (to be implemented later).
/// Scorer can use prefilter to score only a subset of rows.
/// Scorer typically should use secondary indexes to score rows (e.g. vector or text indexes).
class IScorer
{
public:
    explicit IScorer(size_t top_k_) : top_k(top_k_) {}
    virtual ~IScorer() = default;
    size_t getTopK() const { return top_k; }

    /// Query-level validation of the scorer's arguments and settings against the index metadata.
    virtual void validate(const ContextPtr & /*context*/) const = 0;

    /// Indexes whose persisted data the scorer reads.
    virtual std::vector<MergeTreeIndexPtr> getIndexes() const = 0;

private:
    size_t top_k;
};


/// Abstract base for row-producing scorers (vector, BM25).
class RowScorer : public IScorer
{
public:
    using ScoreResult = std::vector<std::pair<UInt64, Float32>>;
    explicit RowScorer(size_t top_k_) : IScorer(top_k_) {}

    /// Score one part: returns `(row_id, score)` pairs for surviving rows.
    /// Sorted by `score` in `getSortDirection` order with `row_id` ASC as tie-breaker.
    virtual ScoreResult scorePart(const DataPartPtr & part, const Prefilter & prefilter, ContextPtr context) = 0;

    /// Sort direction of the top-K merge:
    /// - `Ascending` keeps the K smallest scores (e.g. vector distances)
    /// - `Descending` the K largest (e.g. BM25 scores).
    virtual ScoreDirection getSortDirection() const = 0;
};

}
