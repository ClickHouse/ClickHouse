#pragma once

#include <Columns/IColumn.h>
#include <DataTypes/IDataType.h>
#include <IO/HashingWriteBuffer.h>
#include <IO/WriteBufferFromFileBase.h>
#include <Storages/MergeTree/IMergeTreeDataPartIndexWriter.h>

namespace DB
{

class MergeTreeDataPartIndexWriterSingleDisk: public IMergeTreeDataPartIndexWriter
{
public:
    MergeTreeDataPartIndexWriterSingleDisk(IMergeTreeDataPartWriter & part_writer);

    void initPrimaryIndex() override;
    void calculateAndSerializePrimaryIndex(const Block & primary_index_block, size_t rows) override;
    void finishPrimaryIndexSerialization(MergeTreeData::DataPart::Checksums & checksums) override;

    void initSkipIndices() override;
    void calculateAndSerializeSkipIndices(const Block & skip_indexes_block, size_t rows) override;
    void finishSkipIndicesSerialization(MergeTreeData::DataPart::Checksums & checksums) override;
};

}
