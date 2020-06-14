#pragma once

#include <Storages/MergeTree/MergeTreeIndexGranularity.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <DataStreams/IBlockOutputStream.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/IMergeTreeDataPartWriter.h>

namespace DB
{

class IMergedBlockOutputStream : public IBlockOutputStream
{
public:
    IMergedBlockOutputStream(
        const MergeTreeDataPartPtr & data_part);

    using WrittenOffsetColumns = std::set<std::string>;

    const MergeTreeIndexGranularity & getIndexGranularity()
    {
        return writer->getIndexGranularity();
    }

protected:
    using SerializationState = IDataType::SerializeBinaryBulkStatePtr;

    IDataType::OutputStreamGetter createStreamGetter(const String & name, WrittenOffsetColumns & offset_columns);

    /// Remove all columns marked expired in data_part. Also, clears checksums
    /// and columns array. Return set of removed files names.
    static NameSet removeEmptyColumnsFromPart(
        const MergeTreeDataPartPtr & data_part,
        NamesAndTypesList & columns,
        MergeTreeData::DataPart::Checksums & checksums);

protected:
    const MergeTreeData & storage;

    VolumePtr volume;
    String part_path;

    static Block getBlockAndPermute(const Block & block, const Names & names, const IColumn::Permutation * permutation);

    IMergeTreeDataPart::MergeTreeWriterPtr writer;
};

}
