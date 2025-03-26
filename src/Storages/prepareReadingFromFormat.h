#pragma once
#include <Core/Block.h>
#include <Storages/StorageSnapshot.h>
#include <DataTypes/Serializations/SerializationInfo.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{
    struct PrewhereInfo;
    using PrewhereInfoPtr = std::shared_ptr<PrewhereInfo>;

    struct ReadFromFormatInfo
    {
        /// Header that will return Source from storage.
        /// Concatenation of requested_columns and requested_virtual_columns.
        Block source_header;
        /// Header that will be passed to IInputFormat to read data from file.
        /// Superset of requested_columns.
        /// It can contain more columns than were requested if format doesn't support
        /// reading subset of columns.
        Block format_header;
        /// Description of columns for format_header. Used for inserting defaults.
        ColumnsDescription columns_description;
        /// Columns to request from IInputFormat.
        /// Includes columns to read from file and maybe prewhere result column (if
        /// !remove_prewhere_column). The prewhere result column is not necessarily at the end.
        /// Doesn't include columns that are only used as prewhere input; IInputFormat should deduce
        /// them from prewhere expression.
        NamesAndTypesList requested_columns;
        /// The list of requested virtual columns.
        NamesAndTypesList requested_virtual_columns;
        /// Hints for the serialization of columns.
        /// For example can be retrieved from the destination table in INSERT SELECT query.
        SerializationInfoByName serialization_hints;
        PrewhereInfoPtr prewhere_info;
    };

    /// Get all needed information for reading from data in some input format.
    ReadFromFormatInfo prepareReadingFromFormat(const Strings & requested_columns, const StorageSnapshotPtr & storage_snapshot, const ContextPtr & context, bool supports_subset_of_columns, bool supports_subset_of_subcolumns = false);

    ReadFromFormatInfo updateFormatPrewhereInfo(const ReadFromFormatInfo & info, const PrewhereInfoPtr & prewhere_info);

    /// Returns the serialization hints from the insertion table (if it's set in the Context).
    SerializationInfoByName getSerializationHintsForFileLikeStorage(const StorageMetadataPtr & metadata_snapshot, const ContextPtr & context);
}
