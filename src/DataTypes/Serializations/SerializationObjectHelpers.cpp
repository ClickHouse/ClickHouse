#include <DataTypes/Serializations/SerializationObjectHelpers.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypeDynamic.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnObject.h>
#include <Common/SipHash.h>
#include <IO/ReadHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

std::vector<std::pair<std::string_view, ColumnPtr>> flattenPaths(const ColumnObject & object_column)
{
    SharedDataBucketsSplitter splitter(*object_column.getSharedDataPtr(), 0, object_column.size(), 1);
    auto all_paths = splitter.flattenBucket(0, object_column.getDynamicType());
    for (const auto & [path, column] : object_column.getDynamicPaths())
        all_paths.emplace_back(path, column);
    std::sort(all_paths.begin(), all_paths.end());
    return all_paths;
}

void unflattenAndInsertPaths(const std::vector<String> & flattened_paths, MutableColumns && flattened_columns, ColumnObject & object_column, size_t num_rows)
{
    /// Iterate over paths and try to add them to dynamic paths until the limit is reached.
    /// All remaining paths will be inserted into shared data.
    std::map<std::string_view, ColumnPtr> paths_for_shared_data;
    for (size_t i = 0; i != flattened_paths.size(); ++i)
    {
        if (object_column.canAddNewDynamicPath())
            object_column.addNewDynamicPath(flattened_paths[i], std::move(flattened_columns[i]));
        else
            paths_for_shared_data.emplace(flattened_paths[i], std::move(flattened_columns[i]));
    }

    auto [shared_data_paths, shared_data_values] = object_column.getSharedDataPathsAndValues();
    auto & shared_data_offsets = object_column.getSharedDataOffsets();
    std::unordered_map<std::string_view, const ColumnDynamic *> dynamic_columns_ptrs;
    dynamic_columns_ptrs.reserve(flattened_paths.size());
    for (const auto & [path, column] : paths_for_shared_data)
        dynamic_columns_ptrs[path] = assert_cast<const ColumnDynamic *>(column.get());

    for (size_t i = 0; i != num_rows; ++i)
    {
        for (const auto & [path, column] : paths_for_shared_data)
            ColumnObject::serializePathAndValueIntoSharedData(shared_data_paths, shared_data_values, path, *dynamic_columns_ptrs[path], i);
        shared_data_offsets.push_back(shared_data_paths->size());
    }
}

size_t getSharedDataPathBucket(std::string_view path, size_t num_buckets)
{
    /// Do not change the hash function here, it will break paths
    /// reading from buckets as we read only corresponding bucket.
    SipHash hash;
    hash.update(path);
    return hash.get64() % num_buckets;
}

SharedDataBucketsSplitter::SharedDataBucketsSplitter(const IColumn & shared_data_column_, size_t start_, size_t end_, size_t num_buckets_)
    : shared_data_column(shared_data_column_)
    , start(start_)
    , end(end_)
    , num_buckets(num_buckets_)
    , bucket_num_paths(num_buckets_, 0)
    , bucket_paths_chars_size(num_buckets_, 0)
    , bucket_values_chars_size(num_buckets_, 0)
{
    const auto [shared_data_paths, shared_data_values, shared_data_offsets] = ColumnObject::getSharedDataPathsValuesAndOffsets(shared_data_column);
    const auto & paths_offsets = shared_data_paths->getOffsets();
    const auto & values_offsets = shared_data_values->getOffsets();

    /// Compute the bucket for every path once and accumulate per-bucket sizes, so `extractBucket` can
    /// pre-allocate each bucket exactly and does not recompute the path hashes.
    path_buckets.reserve((*shared_data_offsets)[ssize_t(end) - 1] - (*shared_data_offsets)[ssize_t(start) - 1]);
    for (size_t i = start; i != end; ++i)
    {
        size_t offset_start = (*shared_data_offsets)[ssize_t(i) - 1];
        size_t offset_end = (*shared_data_offsets)[ssize_t(i)];
        for (size_t j = offset_start; j != offset_end; ++j)
        {
            size_t bucket = getSharedDataPathBucket(shared_data_paths->getDataAt(j), num_buckets);
            path_buckets.push_back(static_cast<UInt8>(bucket));
            ++bucket_num_paths[bucket];
            /// The number of chars occupied by value `j` (exactly what `insertFrom` appends) is the
            /// difference of consecutive string offsets.
            bucket_paths_chars_size[bucket] += paths_offsets[ssize_t(j)] - paths_offsets[ssize_t(j) - 1];
            bucket_values_chars_size[bucket] += values_offsets[ssize_t(j)] - values_offsets[ssize_t(j) - 1];
        }
    }
}

ColumnPtr SharedDataBucketsSplitter::extractBucket(size_t bucket) const
{
    const auto [shared_data_paths, shared_data_values, shared_data_offsets] = ColumnObject::getSharedDataPathsValuesAndOffsets(shared_data_column);

    auto bucket_column = shared_data_column.cloneEmpty();
    auto [bucket_paths, bucket_values, bucket_offsets] = ColumnObject::getSharedDataPathsValuesAndOffsets(*bucket_column);

    /// Pre-allocate exactly to avoid power-of-two over-allocation.
    bucket_paths->getChars().reserve_exact(bucket_paths_chars_size[bucket]);
    bucket_paths->getOffsets().reserve_exact(bucket_num_paths[bucket]);
    bucket_values->getChars().reserve_exact(bucket_values_chars_size[bucket]);
    bucket_values->getOffsets().reserve_exact(bucket_num_paths[bucket]);
    bucket_offsets->reserve_exact(end - start);

    size_t path_index = 0;
    for (size_t i = start; i != end; ++i)
    {
        size_t offset_start = (*shared_data_offsets)[ssize_t(i) - 1];
        size_t offset_end = (*shared_data_offsets)[ssize_t(i)];
        for (size_t j = offset_start; j != offset_end; ++j, ++path_index)
        {
            if (path_buckets[path_index] == bucket)
            {
                bucket_paths->insertFrom(*shared_data_paths, j);
                bucket_values->insertFrom(*shared_data_values, j);
            }
        }

        bucket_offsets->push_back(bucket_paths->size());
    }

    return bucket_column;
}

std::vector<std::pair<std::string_view, ColumnPtr>> SharedDataBucketsSplitter::flattenBucket(size_t bucket, const DataTypePtr & dynamic_type) const
{
    const auto [shared_data_paths, shared_data_values, shared_data_offsets] = ColumnObject::getSharedDataPathsValuesAndOffsets(shared_data_column);

    /// Collect values of the paths belonging to this bucket into separate columns. Each column is
    /// densified to have a value for every row (a default where the path is absent). Gaps are backfilled
    /// with a single bulk insertManyDefaults right before storing a real value (and once at the end),
    /// which removes the per-row scan over every accumulated path column. The number of defaults
    /// materialized is unchanged; only the scan is removed.
    /// Keys are string_views referencing shared_data_paths to avoid copying path strings.
    std::unordered_map<std::string_view, MutableColumnPtr> flattened_shared_data_paths;
    size_t path_index = 0;
    for (size_t i = start; i != end; ++i)
    {
        size_t offset_start = (*shared_data_offsets)[ssize_t(i) - 1];
        size_t offset_end = (*shared_data_offsets)[ssize_t(i)];
        size_t row = i - start;
        for (size_t j = offset_start; j != offset_end; ++j, ++path_index)
        {
            /// Skip paths not belonging to this bucket (bucket assignment precomputed in the constructor).
            if (path_buckets[path_index] != bucket)
                continue;

            std::string_view path = shared_data_paths->getDataAt(j);
            auto it = flattened_shared_data_paths.find(path);
            /// If we see this path for the first time, add it to the list and create a column for it.
            if (it == flattened_shared_data_paths.end())
                it = flattened_shared_data_paths.emplace(path, dynamic_type->createColumn()).first;

            /// Backfill defaults for the rows where this path was absent, up to the current row.
            if (it->second->size() < row)
                it->second->insertManyDefaults(row - it->second->size());

            ColumnObject::deserializeValueFromSharedData(shared_data_values, j, *it->second);
        }
    }

    /// Backfill defaults for the trailing rows where each path was absent.
    size_t num_rows = end - start;
    for (const auto & [_, column] : flattened_shared_data_paths)
    {
        if (column->size() < num_rows)
            column->insertManyDefaults(num_rows - column->size());
    }

    /// Keep paths sorted for consistency.
    std::vector<std::pair<std::string_view, ColumnPtr>> result;
    result.reserve(flattened_shared_data_paths.size());
    for (const auto & [path, column] : flattened_shared_data_paths)
        result.emplace_back(path, column->getPtr());
    std::sort(result.begin(), result.end());
    return result;
}

void collectSharedDataFromBuckets(const Columns & shared_data_buckets, IColumn & shared_data_column, const String * paths_prefix)
{
    const auto [shared_data_paths, shared_data_values, shared_data_offsets] = ColumnObject::getSharedDataPathsValuesAndOffsets(shared_data_column);
    std::vector<const ColumnString *> shared_data_paths_buckets(shared_data_buckets.size());
    std::vector<const ColumnString *> shared_data_values_buckets(shared_data_buckets.size());
    std::vector<const ColumnArray::Offsets *> shared_data_offsets_buckets(shared_data_buckets.size());
    for (size_t i = 0; i != shared_data_buckets.size(); ++i)
        std::tie(shared_data_paths_buckets[i], shared_data_values_buckets[i], shared_data_offsets_buckets[i]) = ColumnObject::getSharedDataPathsValuesAndOffsets(*shared_data_buckets[i]);

    size_t num_rows = shared_data_buckets[0]->size();
    for (size_t i = 0; i != num_rows; ++i)
    {
        /// Shared data contains paths in sorted order in each row.
        /// Collect all paths from all buckets in this row and sort them.
        /// Save each path bucket and index to be able find corresponding value later.
        std::vector<std::tuple<std::string_view, size_t, size_t>> all_paths;
        for (size_t bucket = 0; bucket != shared_data_buckets.size(); ++bucket)
        {
            size_t offset_start = (*shared_data_offsets_buckets[bucket])[ssize_t(i) - 1];
            size_t offset_end = (*shared_data_offsets_buckets[bucket])[ssize_t(i)];

            /// If no paths prefix specified, collect all paths.
            if (!paths_prefix)
            {
                for (size_t j = offset_start; j != offset_end; ++j)
                {
                    auto path = shared_data_paths_buckets[bucket]->getDataAt(j);
                    all_paths.emplace_back(path, bucket, j);
                }
            }
            /// Otherwise collect only paths that match the prefix.
            else
            {
                size_t lower_bound_index = ColumnObject::findPathLowerBoundInSharedData(*paths_prefix, *shared_data_paths_buckets[bucket], offset_start, offset_end);
                for (; lower_bound_index != offset_end; ++lower_bound_index)
                {
                    auto path = shared_data_paths_buckets[bucket]->getDataAt(lower_bound_index);
                    if (!path.starts_with(*paths_prefix))
                        break;
                    auto sub_path = path.substr(paths_prefix->size());
                    all_paths.emplace_back(sub_path, bucket, lower_bound_index);
                }
            }
        }

        std::sort(all_paths.begin(), all_paths.end());
        for (const auto [path, bucket, offset] : all_paths)
        {
            shared_data_paths->insertData(path.data(), path.size());
            shared_data_values->insertFrom(*shared_data_values_buckets[bucket], offset);
        }

        shared_data_offsets->push_back(shared_data_paths->size());
    }
}

namespace
{

template <typename T>
ColumnPtr createPathsIndexesImpl(const std::unordered_map<std::string_view, size_t> & path_to_index, const ColumnString & paths_column, size_t start, size_t end)
{
    auto indexes_column = ColumnVector<T>::create();
    auto & data = indexes_column->getData();
    data.reserve(end - start);
    for (size_t i = start; i < end; ++i)
        data.push_back(static_cast<T>(path_to_index.at(paths_column.getDataAt(i))));
    return indexes_column;
}

template <typename T = UInt8>
void deserializeIndexesAndCollectPathsImpl(ColumnString & paths_column, ReadBuffer & istr, std::vector<String> && paths, size_t limit)
{
    auto & data = paths_column.getChars();
    auto & offsets = paths_column.getOffsets();
    size_t offset = data.size();

    /// Avoiding calling resize in a loop improves the performance.
    data.resize(std::max(data.capacity(), static_cast<size_t>(4096)));

    for (size_t i = 0; i != limit; ++i)
    {
        if (istr.eof())
            break;

        T index;
        readBinaryLittleEndian(index, istr);

        if (index >= paths.size())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Object path index is out of range: {} >= {}", static_cast<UInt64>(index), paths.size());

        const String & path = paths[index];
        offset += path.size();
        offsets.push_back(offset);

        /// Reallocate data if needed.
        if (unlikely(offset > data.size()))
            data.resize_exact(roundUpToPowerOfTwoOrZero(std::max(offset, data.size() * 2)));

        memcpy(&data[offset - path.size()], path.data(), path.size());
    }

    data.resize_exact(offset);
}

}

std::pair<ColumnPtr, DataTypePtr> createPathsIndexes(const std::unordered_map<std::string_view, size_t> & path_to_index, const IColumn & paths_column, size_t start, size_t end)
{
    const auto & paths_string_column = assert_cast<const ColumnString &>(paths_column);
    auto indexes_type = getSmallestIndexesType(path_to_index.size());
    switch (indexes_type->getTypeId())
    {
        case TypeIndex::UInt8:
            return {createPathsIndexesImpl<UInt8>(path_to_index, paths_string_column, start, end), indexes_type};
        case TypeIndex::UInt16:
            return {createPathsIndexesImpl<UInt16>(path_to_index, paths_string_column, start, end), indexes_type};
        case TypeIndex::UInt32:
            return {createPathsIndexesImpl<UInt32>(path_to_index, paths_string_column, start, end), indexes_type};
        case TypeIndex::UInt64:
            return {createPathsIndexesImpl<UInt64>(path_to_index, paths_string_column, start, end), indexes_type};
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected type of paths indexes: {}", indexes_type->getName());
    }
}

void deserializeIndexesAndCollectPaths(IColumn & paths_column, ReadBuffer & istr, std::vector<String> && paths, size_t limit)
{
    auto & paths_string_column = assert_cast<ColumnString &>(paths_column);
    auto indexes_type = getSmallestIndexesType(paths.size());
    switch (indexes_type->getTypeId())
    {
        case TypeIndex::UInt8:
            deserializeIndexesAndCollectPathsImpl<UInt8>(paths_string_column, istr, std::move(paths), limit);
            break;
        case TypeIndex::UInt16:
            deserializeIndexesAndCollectPathsImpl<UInt16>(paths_string_column, istr, std::move(paths), limit);
            break;
        case TypeIndex::UInt32:
            deserializeIndexesAndCollectPathsImpl<UInt32>(paths_string_column, istr, std::move(paths), limit);
            break;
        case TypeIndex::UInt64:
            deserializeIndexesAndCollectPathsImpl<UInt64>(paths_string_column, istr, std::move(paths), limit);
            break;
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected column type of paths indexes: {}", indexes_type->getName());
    }
}

}
