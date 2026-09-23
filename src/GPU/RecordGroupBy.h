#pragma once

#include <GPU/IGroupBy.h>

#include <memory>

namespace DB::GPU
{

/** A keyed aggregation in progress on the device: a `cuco::static_set` of packed keys, and beside
  * it a record of eight-byte accumulators per slot, one for each aggregate function.
  *
  * The key columns of a row are packed into one 64-bit key - they are integers of eight bytes
  * between them at most, which `canGroupByReduceOnDevice` sees to - and the slot the set files it
  * in is the group's number for as long as the table lives. The row's values fold into the
  * record of that slot: a `sum` is an atomic add, a `min` or `max` an atomic minimum or maximum,
  * and the aggregates of a row lie side by side, so a row with several touches one cache line
  * rather than a line in each of several arrays. A batch costs one pass over its rows, with no
  * sort, no per-batch table and no merge, and the groups of one batch meet the groups of the next
  * in the same slots.
  *
  * The set cannot grow, so before a chunk of rows goes in, the table is checked to have room for
  * every one of them to be a new key; when it has not, a table twice the size is made and the
  * occupied slots are moved into it, which costs one pass over the groups. The set marks an empty
  * slot with a key of all ones; when the packed key fills all eight bytes, a row can carry that key
  * as well, and such rows are folded into one spare record past the end of the table.
  */
class RecordGroupBy final : public IGroupBy
{
public:
    RecordGroupBy(GPUSpan<GPUElementType> key_element_types_, GPUSpan<GPUGroupByValue> values_);

    ~RecordGroupBy() override;

    double addBatch(
        GPUSpan<DeviceColumnView> keys, GPUSpan<DeviceColumnView> values, GPUSpan<DeviceColumnView> filter_columns, const GPUFilterProgram * filter)
        override;

    size_t finalize() override;

    void copyGroupsOut(GPUSpan<HostColumnView> keys, GPUSpan<HostColumnView> values) override;

private:
    struct State;
    std::unique_ptr<State> state;
};

}
