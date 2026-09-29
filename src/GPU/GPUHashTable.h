#pragma once

#include "config.h"

#if USE_GPU

#include <GPU/GPUTypes.cuh>
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

class HashTable
{
public:
    struct Matches
    {
        ColumnUInt32::MutablePtr probe_row_indices;
        MutableColumns build_payload_columns;

        size_t size() const { return probe_row_indices->size(); }
    };

    static bool canJoinOnDevice(const IDataType & key_type, const DataTypes & payload_types);

    HashTable(const IDataType & key_type_, const DataTypes & payload_types_, size_t stage_bytes_ = 64 * 1024 * 1024);

    ~HashTable();

    HashTable(const HashTable &) = delete;
    HashTable & operator=(const HashTable &) = delete;

    void addBuildBlock(const IColumn & key_column, const Columns & payload_columns);

    void finishBuild();

    bool isReady() const { return ready.load(std::memory_order_acquire); }

    size_t buildRows() const { return build_rows; }
    size_t buildBytes() const { return build_bytes; }

    Matches probe(const IColumn & key_column);

    Matches noMatches() const;

private:
    struct Probe
    {
        StreamPtr stream = createStream();
        ColumnUploadPipe keys;
        CudfHashJoinProbe device;

        Probe(const CudfHashJoin & join, const IDataType & key_column_type, size_t key_stage_bytes)
            : keys(key_column_type, key_stage_bytes, rmm::cuda_stream_view{stream.get()})
            , device(join, rmm::cuda_stream_view{stream.get()})
        {
        }
    };

    std::unique_ptr<Probe> takeProbe();
    void returnProbe(std::unique_ptr<Probe> probe);

    const DataTypePtr key_type;
    const GPUElementType key_element_type;
    const DataTypes payload_types;
    const std::vector<GPUElementType> payload_element_types;
    const size_t row_bytes;
    const size_t stage_bytes;

    ColumnUploadPipe build_key_pipe;
    std::vector<ColumnUploadPipe> build_payload_pipes;

    std::unique_ptr<CudfHashJoin> hash_join;

    size_t build_rows = 0;
    size_t build_bytes = 0;
    bool built = false;
    std::atomic<bool> ready = false;

    std::mutex idle_probes_mutex;
    std::vector<std::unique_ptr<Probe>> idle_probes;
};

}

#endif
