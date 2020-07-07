#pragma once

#include <Storages/MergeTree/IMergeTreeDataPartWriter.h>

namespace DB
{

/// Writes data part in wide format.
class MergeTreeDataPartWriterWide : public IMergeTreeDataPartWriter
{
public:

    using ColumnToSize = std::map<std::string, UInt64>;

    MergeTreeDataPartWriterWide(
        const MergeTreeData::DataPartPtr & data_part,
        const NamesAndTypesList & columns_list,
        const StorageMetadataPtr & metadata_snapshot,
        const std::vector<MergeTreeIndexPtr> & indices_to_recalc,
        const String & marks_file_extension,
        const CompressionCodecPtr & default_codec,
        const MergeTreeWriterSettings & settings,
        const MergeTreeIndexGranularity & index_granularity);

protected:
    /// Write data of one column.
    /// Return how many marks were written and
    /// how many rows were written for last mark
    virtual void writeColumn(
        const String & name,
        const IDataType & type,
        const IColumn & column,
        WrittenOffsetColumns & offset_columns) = 0;

    /// Write single granule of one column (rows between 2 marks)
    virtual size_t writeSingleGranule(
        const String & name,
        const IDataType & type,
        const IColumn & column,
        WrittenOffsetColumns & offset_columns,
        IDataType::SerializeBinaryBulkStatePtr & serialization_state,
        IDataType::SerializeBinaryBulkSettings & serialize_settings,
        size_t from_row,
        size_t number_of_rows,
        bool write_marks) = 0;

    /// Write mark for column
    virtual void writeSingleMark(
        const String & name,
        const IDataType & type,
        WrittenOffsetColumns & offset_columns,
        size_t number_of_rows,
        DB::IDataType::SubstreamPath & path) = 0;

    virtual void writeFinalMark(
        const std::string & column_name,
        const DataTypePtr column_type,
        WrittenOffsetColumns & offset_columns,
        DB::IDataType::SubstreamPath & path) = 0;
};

}
