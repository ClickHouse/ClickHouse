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

std::vector<std::vector<std::pair<String, ColumnPtr>>> flattenAndBucketSharedDataPaths(const IColumn & shared_data_column, size_t start, size_t end, const DataTypePtr & dynamic_type, size_t num_buckets)
{
    /// First, iterate over shared data and collect values of
    /// all paths that are stored there into separate columns.
    std::unordered_map<String, MutableColumnPtr> flattened_shared_data_paths;
    const auto [shared_data_paths, shared_data_values, shared_data_offsets] = ColumnObject::getSharedDataPathsValuesAndOffsets(shared_data_column);
    for (size_t i = start; i != end; ++i)
    {
        size_t offset_start = (*shared_data_offsets)[ssize_t(i) - 1];
        size_t offset_end = (*shared_data_offsets)[ssize_t(i)];
        for (size_t j = offset_start; j != offset_end; ++j)
        {
            auto path = shared_data_paths->getDataAt(j).toString();
            auto it = flattened_shared_data_paths.find(path);
            /// If we see this path for the first time, add it to the list and create a column for it.
            if (it == flattened_shared_data_paths.end())
            {
                it = flattened_shared_data_paths.emplace(path, dynamic_type->createColumn()).first;
                it->second->insertManyDefaults(i - start);
            }

            ColumnObject::deserializeValueFromSharedData(shared_data_values, j, *it->second);
        }

        /// Insert default value to all paths that were not present in this row.
        for (const auto & [_, column] : flattened_shared_data_paths)
        {
            if (column->size() != i + 1 - start)
                column->insertDefault();
        }
    }

    /// Iterate over collected paths and put them into corresponding buckets.
    std::vector<std::vector<std::pair<String, ColumnPtr>>> buckets(num_buckets);
    for (const auto & [path, column] : flattened_shared_data_paths)
        buckets[getSharedDataPathBucket(path, num_buckets)].emplace_back(path, column->getPtr());

    /// Keep paths sorted in each bucket for consistency.
    for (auto & bucket : buckets)
        std::sort(bucket.begin(), bucket.end());

    return buckets;
}

std::vector<std::pair<String, ColumnPtr>> flattenPaths(const ColumnObject & object_column)
{
    auto all_paths = std::move(flattenAndBucketSharedDataPaths(*object_column.getSharedDataPtr(), 0, object_column.size(), object_column.getDynamicType(), 1)[0]);
    for (const auto & [path, column] : object_column.getDynamicPaths())
        all_paths.emplace_back(path, column);
    std::sort(all_paths.begin(), all_paths.end());
    return all_paths;
}

void unflattenAndInsertPaths(const std::vector<String> & flattened_paths, std::vector<ColumnPtr> && flattened_columns, ColumnObject & object_column, size_t num_rows)
{
    /// Iterate over paths and try to add them to dynamic paths until the limit is reached.
    /// All remaining paths will be inserted into shared data.
    std::map<std::string_view, ColumnPtr> paths_for_shared_data;
    for (size_t i = 0; i != flattened_paths.size(); ++i)
    {
        if (object_column.canAddNewDynamicPath())
            object_column.addNewDynamicPath(flattened_paths[i], IColumn::mutate(std::move(flattened_columns[i])));
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

std::vector<ColumnPtr> splitSharedDataPathsToBuckets(const IColumn & shared_data_column, size_t start, size_t end, size_t num_buckets)
{
    std::vector<ColumnPtr> shared_data_buckets(num_buckets);
    std::vector<ColumnString *> shared_data_paths_buckets(num_buckets);
    std::vector<ColumnString *> shared_data_values_buckets(num_buckets);
    std::vector<ColumnArray::Offsets *> shared_data_offsets_buckets(num_buckets);
    for (size_t i = 0; i != num_buckets; ++i)
    {
        auto column = shared_data_column.cloneEmpty();
        std::tie(shared_data_paths_buckets[i], shared_data_values_buckets[i], shared_data_offsets_buckets[i]) = ColumnObject::getSharedDataPathsValuesAndOffsets(*column);
        shared_data_buckets[i] = std::move(column);
    }

    const auto [shared_data_paths, shared_data_values, shared_data_offsets] = ColumnObject::getSharedDataPathsValuesAndOffsets(shared_data_column);
    for (size_t i = start; i != end; ++i)
    {
        size_t offset_start = (*shared_data_offsets)[ssize_t(i) - 1];
        size_t offset_end = (*shared_data_offsets)[ssize_t(i)];
        for (size_t j = offset_start; j != offset_end; ++j)
        {
            size_t bucket = getSharedDataPathBucket(shared_data_paths->getDataAt(j).toView(), num_buckets);
            shared_data_paths_buckets[bucket]->insertFrom(*shared_data_paths, j);
            shared_data_values_buckets[bucket]->insertFrom(*shared_data_values, j);
        }

        for (size_t bucket = 0; bucket != num_buckets; ++bucket)
            shared_data_offsets_buckets[bucket]->push_back(shared_data_paths_buckets[bucket]->size());
    }

    return shared_data_buckets;
}

void collectSharedDataFromBuckets(const std::vector<ColumnPtr> & shared_data_buckets, IColumn & shared_data_column, const String * paths_prefix)
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
                    auto path = shared_data_paths_buckets[bucket]->getDataAt(j).toView();
                    all_paths.emplace_back(path, bucket, j);
                }
            }
            /// Otherwise collect only paths that match the prefix.
            else
            {
                size_t lower_bound_index = ColumnObject::findPathLowerBoundInSharedData(*paths_prefix, *shared_data_paths_buckets[bucket], offset_start, offset_end);
                for (; lower_bound_index != offset_end; ++lower_bound_index)
                {
                    auto path = shared_data_paths_buckets[bucket]->getDataAt(lower_bound_index).toView();
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
        data.push_back(static_cast<T>(path_to_index.at(paths_column.getDataAt(i).toView())));
    return indexes_column;
}

template <typename T = UInt8>
void deserializeIndexesAndCollectPathsImpl(ColumnString & paths_column, ReadBuffer & istr, std::vector<String> && paths, size_t rows_offset, size_t limit)
{
    /// Ignore first rows_offset values as we don't need them in the result.
    istr.ignore(sizeof(T) * rows_offset);
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

void deserializeIndexesAndCollectPaths(IColumn & paths_column, ReadBuffer & istr, std::vector<String> && paths, size_t rows_offset, size_t limit)
{
    auto & paths_string_column = assert_cast<ColumnString &>(paths_column);
    auto indexes_type = getSmallestIndexesType(paths.size());
    switch (indexes_type->getTypeId())
    {
        case TypeIndex::UInt8:
            deserializeIndexesAndCollectPathsImpl<UInt8>(paths_string_column, istr, std::move(paths), rows_offset, limit);
            break;
        case TypeIndex::UInt16:
            deserializeIndexesAndCollectPathsImpl<UInt16>(paths_string_column, istr, std::move(paths), rows_offset, limit);
            break;
        case TypeIndex::UInt32:
            deserializeIndexesAndCollectPathsImpl<UInt32>(paths_string_column, istr, std::move(paths), rows_offset, limit);
            break;
        case TypeIndex::UInt64:
            deserializeIndexesAndCollectPathsImpl<UInt64>(paths_string_column, istr, std::move(paths), rows_offset, limit);
            break;
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected column type of paths indexes: {}", indexes_type->getName());
    }

}

}
