#pragma once

#include "config.h"

#if USE_GPU

#include <GPU/GPUTypes.h>
#include <GPU/CudfHashJoin.cuh>
#include <GPU/GPUMemory.h>
#include <GPU/GPUUploadPipe.h>

#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <DataTypes/IDataType.h>

#include <atomic>
#include <memory>
#include <mutex>
#include <vector>

namespace DB::GPU
{

/** A hash table over the right table of an `INNER JOIN`, living on the device.
  *
  * The right table's key and payload columns are uploaded as its blocks arrive and stay there. A
  * probe sends only the left block's key column and brings back, for each matching pair, the index
  * of the left row and the right table's payload gathered to match - so the left table, normally
  * the larger one, never crosses the link.
  *
  * The build is not thread-safe: the caller serializes `addBuildBlock` and `finishBuild`. Once
  * built, `probe` may be called from any number of threads at once. Each takes a probe of its own
  * from a pool - a stream and the pinned buffers its keys and matches pass through - so that the
  * probes of different blocks overlap on the device instead of waiting for one another.
  */
class HashTable
{
public:
    /// The result of one probe: one row per matching pair, in the order the device found them.
    struct Matches
    {
        ColumnUInt32::MutablePtr probe_row_indices;
        MutableColumns build_payload_columns;

        size_t size() const { return probe_row_indices->size(); }
    };

    static bool canJoinOnDevice(const IDataType & key_type, const DataTypes & payload_types);

    HashTable(const IDataType & key_type, const DataTypes & payload_types_, size_t stage_bytes = 64 * 1024 * 1024);

    ~HashTable();

    HashTable(const HashTable &) = delete;
    HashTable & operator=(const HashTable &) = delete;

    void addBuildBlock(const IColumn & key_column, const Columns & payload_columns);

    void finishBuild();

    bool isReady() const { return ready.load(std::memory_order_acquire); }

    size_t buildRows() const { return build_rows; }
    size_t buildBytes() const { return build_bytes; }

    Matches probe(const IColumn & key_column);

    /// An empty result, with a column of the right type per payload column.
    Matches noMatches() const;

private:
    /// What one probe at a time passes through: its stream on the device, and pinned host memory
    /// for the keys going up and the matches coming back.
    struct Probe
    {
        CudfHashJoinProbe device;
        PinnedBuffer keys;
        PinnedBuffer probe_row_indices;
        std::vector<PinnedBuffer> payloads;

        Probe(const CudfHashJoin & join, size_t num_payloads) : device(join), payloads(num_payloads) { }
    };

    std::unique_ptr<Probe> takeProbe();
    void returnProbe(std::unique_ptr<Probe> probe);

    const GPUElementType key_element_type;
    const DataTypes payload_types;
    const std::vector<GPUElementType> payload_element_types;
    const size_t row_bytes;

    PlainUploadPipe build_key_pipe;
    std::vector<PlainUploadPipe> build_payload_pipes;

    std::unique_ptr<CudfHashJoin> hash_join;

    size_t build_rows = 0;
    size_t build_bytes = 0;
    bool built = false;
    /// Set once the table is built, and read by the probes without a lock.
    std::atomic<bool> ready = false;

    std::mutex idle_probes_mutex;
    /// Destroyed before `hash_join`, which they probe.
    std::vector<std::unique_ptr<Probe>> idle_probes;
};

}

#endif
