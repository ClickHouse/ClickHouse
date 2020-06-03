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

private:
    std::unique_ptr<WriteBufferFromFileBase> index_file_stream;
    std::unique_ptr<HashingWriteBuffer> index_stream;
};

}
