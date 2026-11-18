#pragma once

#include <GPU/GPUTypes.cuh>

#include <cuda/stream_ref>

#include <cuda_runtime_api.h>

namespace DB::GPU
{

/// A `GROUP BY` on the device through one `cudf::groupby::streaming_groupby`, which keeps the groups on the device across
/// the batches, or through partial results of each batch split by the keys into parts, see `splitIntoParts`: of keys of
/// integers and strings, by the aggregates of value columns, several of one column or a `count` of none among them.
///
/// Without keys, the aggregates are of every row, as one group, which there is none of where no row is grouped.
///
/// An interface of plain types: both ClickHouse and the GPU island of cuDF, of another standard library, include it, and
/// the implementation, of the types of cuDF, is in the island alone.
class CudfGroupBy
{
public:
    /// Out of line, so that the vtable is emitted in the island alone, where the implementation is.
    __host__ virtual ~CudfGroupBy();

    /// The columns must be there in the order of the stream of the `CudfGroupBy`, and must stay until it has grouped
    /// them, in its order as well. With `kept`, a mask on the device of a bit per row, set for the rows of the `WHERE`,
    /// in 32-bit words as `cudf::bitmask_type`, the other rows are left out; it stays as the columns do.
    virtual void addBatch(GPUSpan<DeviceColumnView> keys, GPUSpan<DeviceColumnView> values, const uint32_t * kept = nullptr) = 0;

    /// Returns the number of groups.
    virtual size_t finalize() = 0;

    /// After `finalize`: keeps the groups whose bit of `kept`, a mask as of `addBatch`, is set, as a `HAVING` does.
    /// Returns the number of groups kept. The views `key` and `value` gave before are no longer valid.
    virtual size_t keepGroups(const uint32_t * kept) = 0;

    /// Whether the batches are reduced over their runs of equal keys, once the first batch with rows decided it. One told
    /// so before its first batch reduces as it is told, so that several of them can be merged by `absorb`.
    virtual bool decided() const = 0;
    virtual bool reducesRuns() const = 0;
    virtual void reduceRuns(bool reduce) = 0;

    /// Whether the partial results of every aggregate combine by an aggregate: not those of an `avg`.
    virtual bool combinesPartials() const = 0;

    /// Before the first batch, of a `GROUP BY` with keys whose aggregates combine: each batch is reduced into partial
    /// results by a groupby of cuDF and split by a hash of the keys into `num_parts` parts as it comes, as Sirius does,
    /// so that no hash table grows over the batches. See `partition`.
    virtual void splitIntoParts(size_t num_parts) = 0;

    /// Instead of `finalize`, of a `GROUP BY` with keys whose aggregates combine: closes the groups as partial results
    /// split by a hash of their keys into `num_parts` parts, which others merge each on its own, see `absorbPart`:
    /// those split as the batches came, or else the groups so far, split now. Returns about the number of groups.
    virtual size_t partition(size_t num_parts) = 0;

    /// Of one that has grouped no batch: takes part `part` of the groups of `other`, which `partition` split, to combine
    /// at `finalize` with the parts of the same number of others. The parts of one `other` may be taken by several at
    /// once, each on its own stream.
    virtual void absorbPart(const CudfGroupBy & other, size_t part) = 0;

    /// Before `finalize`, of the same keys and aggregates: takes the groups of `other` into these, in the order of the
    /// stream of each; `other` has none left.
    virtual void absorb(CudfGroupBy & other) = 0;

    /// Before `finalize`: how many groups `streaming_groupby` holds so far, which waits for the device.
    virtual size_t numGroups() const = 0;
    /// Before `finalize`: grows `streaming_groupby` at once to hold `groups`, so that `absorb` of others of that many
    /// groups in all does not grow it again for each of them. Only of one that has grouped a batch itself.
    virtual void reserveGroups(size_t groups) = 0;

    virtual DeviceColumnView key(size_t index) const = 0;
    /// The result of aggregate `index`.
    virtual DeviceColumnView value(size_t index) const = 0;
};

/// The caller owns what it returns.
__host__ CudfGroupBy * createCudfGroupBy(
    GPUSpan<GPUElementType> keys, GPUSpan<GPUElementType> values, GPUSpan<GPUGroupByAggregate> aggregates, cuda::stream_ref stream);

}
