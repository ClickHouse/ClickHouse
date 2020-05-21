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

    virtual void initSkipIndices() = 0;
    virtual void calculateAndSerializeSkipIndices(const Block & skip_indexes_block, size_t rows) = 0;
    virtual void finishSkipIndicesSerialization(MergeTreeData::DataPart::Checksums & checksums) = 0;

    virtual ~IMergeTreeDataPartIndexWriter() = default;
protected:
    IMergeTreeDataPartWriter & part_writer;
};

using MergeTreeIndexWriterPtr = std::unique_ptr<IMergeTreeDataPartIndexWriter>;

}
