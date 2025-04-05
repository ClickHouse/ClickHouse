#include "IDataLakeMetadata.h"

namespace DB
{

DB::ReadFromFormatInfo IDataLakeMetadata::prepareReadingFromFormat(
    const Strings & requested_columns,
    const DB::StorageSnapshotPtr & storage_snapshot,
    const ContextPtr & context,
    bool supports_subset_of_columns)
{
    return DB::prepareReadingFromFormat(requested_columns, storage_snapshot, context, supports_subset_of_columns);
}

}
