#pragma once

#include "config.h"

#if USE_GPU

#include <GPU/GPUTypes.h>
#include <GPU/GPUColumns.h>

#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <DataTypes/IDataType.h>

#include <memory>
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

    HashTable(const IDataType & key_type, const DataTypes & payload_types, size_t stage_bytes = 64 * 1024 * 1024);
    ~HashTable();

    HashTable(const HashTable &) = delete;
    HashTable & operator=(const HashTable &) = delete;

    void addBuildBlock(const IColumn & key_column, const Columns & payload_columns);

    void finishBuild();

    size_t buildRows() const { return build_rows; }
    size_t buildBytes() const { return build_bytes; }

    Matches probe(const IColumn & key_column);

    Matches noMatches() const;

private:
    const GPUElementType key_element_type;
    const size_t key_element_size;
    const DataTypes payload_types;
    const std::vector<GPUElementType> payload_element_types;
    const std::vector<size_t> payload_element_sizes;


    UploadPipe build_key_pipe;
    std::vector<UploadPipe> build_payload_pipes;
    UploadPipe probe_key_pipe;

    GPUHashTablePtr hash_table_on_gpu;

    size_t build_rows = 0;
    size_t build_bytes = 0;
    bool built = false;
};

}

#endif
