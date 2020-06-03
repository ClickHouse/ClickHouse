#pragma once

#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/IMergeTreeDataPartWriter.h>

#include <vector>

namespace DB
{

namespace
{
    constexpr auto INDEX_FILE_EXTENSION = ".idx";
}

class IMergeTreeDataPartIndexWriter
{
public:
    explicit IMergeTreeDataPartIndexWriter(IMergeTreeDataPartWriter & part_writer);

    virtual void initPrimaryIndex() = 0;
    virtual void calculateAndSerializePrimaryIndex(const Block & primary_index_block, size_t rows) = 0;
    virtual void finishPrimaryIndexSerialization(MergeTreeData::DataPart::Checksums & checksums) = 0;

    virtual ~IMergeTreeDataPartIndexWriter() = default;

    Columns releaseIndexColumns()
    {
        return Columns(std::make_move_iterator(index_columns.begin()), std::make_move_iterator(index_columns.end()));
    }
protected:
    IMergeTreeDataPartWriter & part_writer;
    bool primary_index_initialized = false;
    MutableColumns index_columns;
    /// Index is already serialized up to this mark.
    size_t index_mark = 0;

    DataTypes index_types;
    /// Index columns values from the last row from the last block
    /// It's written to index file in the `writeSuffixAndFinalizePart` method
    Row last_index_row;
};

using MergeTreeIndexWriterPtr = std::unique_ptr<IMergeTreeDataPartIndexWriter>;

}
