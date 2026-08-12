#pragma once

#include <DataTypes/IDataType.h>
#include <Columns/ColumnObject.h>
#include <Common/PODArray.h>

namespace DB
{

/// Object column can store only limited number of paths as subcolumns.
/// If this limit is reached, all other paths are stored together in a
/// single column called shared data.
/// This function collects all the paths stored in Object column into
/// separate columns (except typed paths, they are not returned and
/// should be processed separately).
/// IMPORTANT: returned string_views reference path data inside the Object column (shared data paths
/// and dynamic paths map keys), which must stay alive while the result is used.
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
void unflattenAndInsertPaths(const std::vector<String> & flattened_paths, Columns && flattened_columns, ColumnObject & object_column, size_t num_rows);

/// Get the bucket number for a specific path.
size_t getSharedDataPathBucket(std::string_view path, size_t num_buckets);

/// Splits the shared data of rows [start, end) into buckets, one bucket at a time, to reduce peak
/// memory: instead of materializing all `num_buckets` buckets simultaneously, the caller builds/
/// serializes/frees one bucket at a time. Two output shapes are supported, both driven by the same
/// bucket assignment:
///   - `extractBucket` returns a shared data column for one bucket (used by MAP_WITH_BUCKETS);
///   - `flattenBucket` returns the flattened per-path columns for one bucket (used by ADVANCED).
/// The bucket assignment for every path is computed once in the constructor (avoiding repeated hashing
/// on each call), and per-bucket sizes are precomputed so each `extractBucket` column is allocated
/// exactly (avoiding power-of-two over-allocation).
class SharedDataBucketsSplitter
{
public:
    SharedDataBucketsSplitter(const IColumn & shared_data_column_, size_t start_, size_t end_, size_t num_buckets_);

    /// Build the shared data column for a single bucket.
    ColumnPtr extractBucket(size_t bucket) const;

    /// Flatten the paths of a single bucket into separate per-path columns, each densified to one value
    /// per row (the stored value where the path is present, a default where it is absent), sorted by path.
    /// IMPORTANT: returned string_views reference path data inside the shared data column, which must
    /// stay alive while the result is used.
    std::vector<std::pair<std::string_view, ColumnPtr>> flattenBucket(size_t bucket, const DataTypePtr & dynamic_type) const;

private:
    const IColumn & shared_data_column;
    size_t start;
    size_t end;
    size_t num_buckets;
    /// Bucket index for each path, in the order paths are traversed (rows [start, end), paths within a row).
    PODArray<UInt8> path_buckets;
    std::vector<size_t> bucket_num_paths;
    std::vector<size_t> bucket_paths_chars_size;
    std::vector<size_t> bucket_values_chars_size;
};
/// Collect paths from bucket columns into a single shared data column.
/// If paths_prefix != nullptr collect only paths that matches this prefix
/// and write paths without this prefix in the result column.
void collectSharedDataFromBuckets(const Columns & shared_data_buckets, IColumn & shared_data_column, const String * paths_prefix = nullptr);

/// Create a column that will contain indexes of paths from paths_column column based on provided mapping path_to_index.
std::pair<ColumnPtr, DataTypePtr> createPathsIndexes(const std::unordered_map<std::string_view, size_t> & path_to_index, const IColumn & paths_column, size_t start, size_t end);

/// Deserialize up to limit indexes from the read buffer and collect corresponding paths to the paths_column.
void deserializeIndexesAndCollectPaths(IColumn & paths_column, ReadBuffer & istr, std::vector<String> && paths, size_t rows_offset, size_t limit);

}
