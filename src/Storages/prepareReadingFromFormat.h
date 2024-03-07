#pragma once
#include <Core/Block.h>
#include <Storages/StorageSnapshot.h>

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
    };

    /// Get all needed information for reading from data in some input format.
    ReadFromFormatInfo prepareReadingFromFormat(const Strings & requested_columns, const StorageSnapshotPtr & storage_snapshot, bool supports_subset_of_columns);
}
