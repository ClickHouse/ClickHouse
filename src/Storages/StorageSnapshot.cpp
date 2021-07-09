#include <Storages/StorageSnapshot.h>
#include <Storages/IStorage.h>
#include <DataTypes/ObjectUtils.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_FOUND_COLUMN_IN_BLOCK;
    extern const int EMPTY_LIST_OF_COLUMNS_QUERIED;
    extern const int NO_SUCH_COLUMN_IN_TABLE;
    extern const int COLUMN_QUERIED_MORE_THAN_ONCE;
}

NamesAndTypesList StorageSnapshot::getColumns(const GetColumnsOptions & options) const
{
    auto all_columns = metadata->getColumns().get(options);

    if (options.with_virtuals)
    {
        /// Virtual columns must be appended after ordinary,
        /// because user can override them.
        auto virtuals = storage.getVirtuals();
        if (!virtuals.empty())
        {
            NameSet column_names;
            for (const auto & column : all_columns)
                column_names.insert(column.name);
            for (auto && column : virtuals)
                if (!column_names.count(column.name))
                    all_columns.push_back(std::move(column));
        }
    }

    if (options.with_extended_objects)
        all_columns = extendObjectColumns(all_columns, object_types, options.with_subcolumns);

    return all_columns;
}

Block StorageSnapshot::getSampleBlockForColumns(const Names & column_names) const
{
    Block res;

    auto all_columns = getColumns(GetColumnsOptions(GetColumnsOptions::All)
        .withSubcolumns().withVirtuals().withExtendedObjects());

    std::unordered_map<String, DataTypePtr> columns_map;
    columns_map.reserve(all_columns.size());

    for (const auto & elem : all_columns)
        columns_map.emplace(elem.name, elem.type);

    for (const auto & name : column_names)
    {
        auto it = columns_map.find(name);
        if (it != columns_map.end())
            res.insert({it->second->createColumn(), it->second, it->first});
        else
            throw Exception(ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK,
                "Column {} not found in table {}", backQuote(name), storage.getStorageID().getNameForLogs());
    }

    return res;
}

void StorageSnapshot::check(const Names & column_names) const
{
    auto available_columns = getColumns(GetColumnsOptions(GetColumnsOptions::AllPhysical)
        .withSubcolumns().withVirtuals().withExtendedObjects()).getNames();

    if (column_names.empty())
        throw Exception(ErrorCodes::EMPTY_LIST_OF_COLUMNS_QUERIED,
            "Empty list of columns queried. There are columns: {}",
            boost::algorithm::join(available_columns, ","));

    std::unordered_set<std::string_view> columns_set(available_columns.begin(), available_columns.end());
    std::unordered_set<std::string_view> unique_names;

    for (const auto & name : column_names)
    {
        if (columns_set.end() == columns_set.find(name))
            throw Exception(ErrorCodes::NO_SUCH_COLUMN_IN_TABLE,
                "There is no column with name {} in table {}. There are columns: ",
                backQuote(name), storage.getStorageID().getNameForLogs(), boost::algorithm::join(available_columns, ","));

        if (unique_names.end() != unique_names.find(name))
            throw Exception(ErrorCodes::COLUMN_QUERIED_MORE_THAN_ONCE, "Column {} queried more than once", name);

        unique_names.insert(name);
    }
}

}
