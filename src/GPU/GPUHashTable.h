#pragma once

#include "config.h"

#if USE_GPU

#include <GPU/GPUTypes.h>
#include <GPU/GPUUploadPipe.h>
#include <GPU/IHashJoin.h>

#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <DataTypes/IDataType.h>

#include <memory>
#include <vector>

namespace DB::GPU
{

/** A hash table over the right table of an `INNER JOIN`, living on the device.
  *
  * The right table's key and payload columns are uploaded as its blocks arrive and stay there. A
  * probe sends only the left block's key column and brings back, for each matching pair, the index
  * of the left row and the right table's payload gathered to match - so the left table, normally
  * the larger one, never crosses the link.
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

    HashTable(const HashTable &) = delete;
    HashTable & operator=(const HashTable &) = delete;

    void addBuildBlock(const IColumn & key_column, const Columns & payload_columns);

    void finishBuild();

    size_t buildRows() const { return build_rows; }
    size_t buildBytes() const { return build_bytes; }

    Matches probe(const IColumn & key_column);

    /// An empty result, with a column of the right type per payload column.
    Matches noMatches() const;

private:
    const GPUElementType key_element_type;
    const DataTypes payload_types;
    const std::vector<GPUElementType> payload_element_types;
    const size_t row_bytes;

    UploadPipe build_key_pipe;
    std::vector<UploadPipe> build_payload_pipes;
    UploadPipe probe_key_pipe;

    std::unique_ptr<IHashJoin> hash_join;

    size_t build_rows = 0;
    size_t build_bytes = 0;
    bool built = false;
};

}

#endif
