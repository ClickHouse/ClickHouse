#pragma once

#include <DataTypes/IDataType.h>
#include <Columns/ColumnObject.h>

namespace DB
{

/// Object column can store only limited number of paths as subcolumns.
/// If this limit is reached, all other paths are stored together in a
/// single column called shared data.
/// This function collects all the paths stored in Object column into
/// separate columns (except typed paths, they are not returned and
/// should be processed separately).
/// IMPORTANT: returned string_views reference data inside shared_data_column and dynamic paths
/// of the Object column, which must stay alive while the result is used.
std::vector<std::pair<std::string_view, ColumnPtr>> flattenPaths(const ColumnObject & object_column);

/// Flatten paths from shared data for a single bucket only.
/// Only materializes ColumnDynamic columns for paths belonging to target_bucket.
/// IMPORTANT: returned string_views reference data inside shared_data_column,
/// which must stay alive while the result is used.
std::vector<std::pair<std::string_view, ColumnPtr>> flattenSharedDataPathsForBucket(
    const IColumn & shared_data_column, size_t start, size_t end,
    const DataTypePtr & dynamic_type, size_t target_bucket, size_t num_buckets);

/// Scan shared data for path names belonging to a single bucket without materializing ColumnDynamic columns.
/// Returns a sorted list of unique path names. Used by Compact serialization to write StructurePrefix
/// headers before writing Data.
/// IMPORTANT: returned string_views reference data inside shared_data_column,
/// which must stay alive while the result is used.
std::vector<std::string_view> scanPathNamesForBucket(
    const IColumn & shared_data_column, size_t start, size_t end,
    size_t target_bucket, size_t num_buckets);

/// Insert data from flattened representation of an Object column to a usual Object column.
void unflattenAndInsertPaths(const std::vector<String> & flattened_paths, std::vector<ColumnPtr> && flattened_columns, ColumnObject & object_column, size_t num_rows);

/// Get the bucket number for a specific path.
size_t getSharedDataPathBucket(std::string_view path, size_t num_buckets);

/// Split shared data column to num_buckets columns by putting all paths from the original column to the corresponding bucket column.
Columns splitSharedDataPathsToBuckets(const IColumn & shared_data_column, size_t start, size_t end, size_t num_buckets);

/// Collect paths from bucket columns into a single shared data column.
/// If paths_prefix != nullptr collect only paths that matches this prefix
/// and write paths without this prefix in the result column.
void collectSharedDataFromBuckets(const Columns & shared_data_buckets, IColumn & shared_data_column, const String * paths_prefix = nullptr);

/// Create a column that will contain indexes of paths from paths_column column based on provided mapping path_to_index.
std::pair<ColumnPtr, DataTypePtr> createPathsIndexes(const std::unordered_map<std::string_view, size_t> & path_to_index, const IColumn & paths_column, size_t start, size_t end);

/// Deserialize up to limit indexes from the read buffer and collect corresponding paths to the paths_column.
void deserializeIndexesAndCollectPaths(IColumn & paths_column, ReadBuffer & istr, std::vector<String> && paths, size_t rows_offset, size_t limit);

}
