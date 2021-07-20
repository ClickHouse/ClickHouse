#include <Storages/StorageSnapshot.h>
#include <Storages/IStorage.h>
#include <DataTypes/ObjectUtils.h>
#include <sparsehash/dense_hash_map>
#include <sparsehash/dense_hash_set>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_FOUND_COLUMN_IN_BLOCK;
    extern const int EMPTY_LIST_OF_COLUMNS_QUERIED;
    extern const int NO_SUCH_COLUMN_IN_TABLE;
    extern const int COLUMN_QUERIED_MORE_THAN_ONCE;
}

namespace
{

#if !defined(ARCADIA_BUILD)
    using NamesAndTypesMap = google::dense_hash_map<StringRef, const DataTypePtr *, StringRefHash>;
    using UniqueStrings = google::dense_hash_set<StringRef, StringRefHash>;
#else
    using NamesAndTypesMap = google::sparsehash::dense_hash_map<StringRef, const DataTypePtr *, StringRefHash>;
    using UniqueStrings = google::sparsehash::dense_hash_set<StringRef, StringRefHash>;
#endif

    NamesAndTypesMap getColumnsMap(const NamesAndTypesList & columns)
    {
        NamesAndTypesMap res;
        res.set_empty_key(StringRef());

        for (const auto & column : columns)
            res.insert({column.name, &column.type});

        return res;
    }

    UniqueStrings initUniqueStrings()
    {
        UniqueStrings strings;
        strings.set_empty_key(StringRef());
        return strings;
    }
}

NamesAndTypesList StorageSnapshot::getColumns(const GetColumnsOptions & options) const
{
    auto all_columns = getMetadataForQuery()->getColumns().get(options);
    return addVirtualsAndObjects(options, std::move(all_columns));
}

NamesAndTypesList StorageSnapshot::getColumnsByNames(const GetColumnsOptions & options, const Names & names) const
{
    auto all_columns = getMetadataForQuery()->getColumns().getByNames(options, names);
    return addVirtualsAndObjects(options, std::move(all_columns));
}

NamesAndTypesList StorageSnapshot::addVirtualsAndObjects(const GetColumnsOptions & options, NamesAndTypesList columns_list) const
{
    if (options.with_virtuals)
    {
        /// Virtual columns must be appended after ordinary,
        /// because user can override them.
        auto virtuals = storage.getVirtuals();
        if (!virtuals.empty())
        {
            NameSet column_names;
            for (const auto & column : columns_list)
                column_names.insert(column.name);
            for (auto && column : virtuals)
                if (!column_names.count(column.name))
                    columns_list.push_back(std::move(column));
        }
    }

    if (options.with_extended_objects)
        columns_list = extendObjectColumns(columns_list, object_types, options.with_subcolumns);

    return columns_list;
}

Block StorageSnapshot::getSampleBlockForColumns(const Names & column_names) const
{
    Block res;

    /// Virtual columns must be appended after ordinary, because user can
    /// override them.
    const auto virtuals_map = getColumnsMap(storage.getVirtuals());
    const auto & columns = getMetadataForQuery()->getColumns();

    for (const auto & name : column_names)
    {
        auto column = columns.tryGetColumnOrSubcolumn(GetColumnsOptions::All, name);
        if (column)
        {
            res.insert({column->type->createColumn(), column->type, column->name});
        }
        else if (auto it = object_types.find(name); it != object_types.end())
        {
            const auto & type = it->second;
            res.insert({type->createColumn(), type, name});
        }
        else if (auto jt = virtuals_map.find(name); jt != virtuals_map.end())
        {
            const auto & type = *jt->second;
            res.insert({type->createColumn(), type, name});
        }
        else
            throw Exception(ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK,
                "Column {} not found in table {}", backQuote(name), storage.getStorageID().getNameForLogs());
    }

    return res;
}

void StorageSnapshot::check(const Names & column_names) const
{
    const auto & columns = getMetadataForQuery()->getColumns();

    if (column_names.empty())
    {
        auto list_of_columns = listOfColumns(columns.getAllPhysicalWithSubcolumns());
        throw Exception(ErrorCodes::EMPTY_LIST_OF_COLUMNS_QUERIED,
            "Empty list of columns queried. There are columns: {}", list_of_columns);
    }

    const auto virtuals_map = getColumnsMap(storage.getVirtuals());
    auto unique_names = initUniqueStrings();

    for (const auto & name : column_names)
    {
        bool has_column = columns.hasColumnOrSubcolumn(GetColumnsOptions::AllPhysical, name) || virtuals_map.count(name);

        if (!has_column)
        {
            auto list_of_columns = listOfColumns(columns.getAllPhysicalWithSubcolumns());
            throw Exception(ErrorCodes::NO_SUCH_COLUMN_IN_TABLE,
                "There is no column with name {} in table {}. There are columns: {}",
                backQuote(name), storage.getStorageID().getNameForLogs(), list_of_columns);
        }

        if (unique_names.end() != unique_names.find(name))
            throw Exception(ErrorCodes::COLUMN_QUERIED_MORE_THAN_ONCE, "Column {} queried more than once", name);

        unique_names.insert(name);
    }
}

DataTypePtr StorageSnapshot::getConcreteType(const String & column_name) const
{
    auto it = object_types.find(column_name);
    if (it != object_types.end())
        return it->second;

    return metadata->getColumns().get(column_name).type;
}

}
