#include <DataTypes/Serializations/SerializationObjectHelpers.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypeDynamic.h>
#include <Columns/ColumnObject.h>


namespace DB
{

std::vector<std::pair<String, ColumnPtr>> flattenPaths(const ColumnObject & object_column)
{
    /// First, iterate over shared data and collect values of
    /// all paths that are stored there into separate columns.
    std::unordered_map<String, MutableColumnPtr> flattened_shared_data_paths;
    const auto [shared_data_paths, shared_data_values] = object_column.getSharedDataPathsAndValues();
    const auto & shared_data_offsets = object_column.getSharedDataOffsets();
    auto dynamic_type = std::make_shared<DataTypeDynamic>(object_column.getMaxDynamicTypes());
    for (size_t i = 0; i != shared_data_offsets.size(); ++i)
    {
        size_t start = shared_data_offsets[ssize_t(i) - 1];
        size_t end = shared_data_offsets[ssize_t(i)];
        for (size_t j = start; j != end; ++j)
        {
            auto path = shared_data_paths->getDataAt(j).toString();
            auto it = flattened_shared_data_paths.find(path);
            /// If we see this path for the first time, add it to the list and create a column for it.
            if (it == flattened_shared_data_paths.end())
            {
                it = flattened_shared_data_paths.emplace(path, dynamic_type->createColumn()).first;
                it->second->insertManyDefaults(i);
            }

            ColumnObject::deserializeValueFromSharedData(shared_data_values, j, *it->second);
        }

        /// Insert default value to all paths that were not present in this row.
        for (const auto & [_, column] : flattened_shared_data_paths)
        {
            if (column->size() != i + 1)
                column->insertDefault();
        }
    }

    std::vector<std::pair<String, ColumnPtr>> all_paths;
    all_paths.reserve(flattened_shared_data_paths.size() + object_column.getDynamicPaths().size());
    for (const auto & [path, column] : object_column.getDynamicPaths())
        all_paths.emplace_back(path, column);
    for (const auto & [path, column] : flattened_shared_data_paths)
        all_paths.emplace_back(path, column->getPtr());
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
            paths_for_shared_data.emplace(flattened_paths[i], flattened_columns[i]);
    }

    auto [shared_data_paths, shared_data_values] = object_column.getSharedDataPathsAndValues();
    auto & shared_data_offsets = object_column.getSharedDataOffsets();
    for (size_t i = 0; i != num_rows; ++i)
    {
        for (const auto & [path, column] : paths_for_shared_data)
            ColumnObject::serializePathAndValueIntoSharedData(shared_data_paths, shared_data_values, path, *column, i);
        shared_data_offsets.push_back(shared_data_paths->size());
    }
}

}
