#pragma once
#include <Core/Block.h>
#include <Storages/StorageSnapshot.h>
#include <DataTypes/Serializations/SerializationInfo.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{
    struct ReadFromFormatInfo
    {
        /// Header that will return Source from storage.
        /// It contains all requested columns including virtual columns;
        Block source_header;
        /// Header that will be passed to IInputFormat to read data from file.
        /// It can contain more columns than were requested if format doesn't support
        /// reading subset of columns.
        Block format_header;
        /// Description of columns for format_header. Used for inserting defaults.
        ColumnsDescription columns_description;
        /// The list of requested columns without virtual columns.
        NamesAndTypesList requested_columns;
        /// The list of requested virtual columns.
        NamesAndTypesList requested_virtual_columns;
        /// Hints for the serialization of columns.
        /// For example can be retrieved from the destination table in INSERT SELECT query.
        SerializationInfoByName serialization_hints;
    };

    /// Get all needed information for reading from data in some input format.
    ReadFromFormatInfo prepareReadingFromFormat(const Strings & requested_columns, const StorageSnapshotPtr & storage_snapshot, const ContextPtr & context, bool supports_subset_of_columns);

    /// Returns the serialization hints from the insertion table (if it's set in the Context).
    SerializationInfoByName getSerializationHintsForFileLikeStorage(const StorageMetadataPtr & metadata_snapshot, const ContextPtr & context);
}
