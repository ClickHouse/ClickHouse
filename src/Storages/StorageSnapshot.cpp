#include <Storages/StorageSnapshot.h>
#include <Storages/IStorage.h>
#include <DataTypes/ObjectUtils.h>
#include <DataTypes/NestedUtils.h>
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

void StorageSnapshot::init()
{
    for (const auto & [name, type] : storage.getVirtuals())
        virtual_columns[name] = type;
}

NamesAndTypesList StorageSnapshot::getColumns(const GetColumnsOptions & options) const
{
    auto all_columns = getMetadataForQuery()->getColumns().get(options);

    if (options.with_extended_objects)
        extendObjectColumns(all_columns, object_columns, options.with_subcolumns);

    if (options.with_virtuals)
    {
        /// Virtual columns must be appended after ordinary,
        /// because user can override them.
        if (!virtual_columns.empty())
        {
            NameSet column_names;
            for (const auto & column : all_columns)
                column_names.insert(column.name);

            for (const auto & [name, type] : virtual_columns)
                if (!column_names.count(name))
                    all_columns.emplace_back(name, type);
        }
    }

    return all_columns;
}

NamesAndTypesList StorageSnapshot::getColumnsByNames(const GetColumnsOptions & options, const Names & names) const
{
    NamesAndTypesList res;
    const auto & columns = getMetadataForQuery()->getColumns();
    for (const auto & name : names)
    {
        auto column = columns.tryGetColumn(options, name);
        if (column && !isObject(column->type))
        {
            res.emplace_back(std::move(*column));
            continue;
        }

        if (options.with_extended_objects)
        {
            auto object_column = object_columns.tryGetColumn(options, name);
            if (object_column)
            {
                res.emplace_back(std::move(*object_column));
                continue;
            }
        }

        if (options.with_virtuals)
        {
            auto it = virtual_columns.find(name);
            if (it != virtual_columns.end())
            {
                res.emplace_back(name, it->second);
                continue;
            }
        }

        throw Exception(ErrorCodes::NO_SUCH_COLUMN_IN_TABLE, "There is no column {} in table", name);
    }

    return res;
}

Block StorageSnapshot::getSampleBlockForColumns(const Names & column_names) const
{
    Block res;

    const auto & columns = getMetadataForQuery()->getColumns();
    for (const auto & name : column_names)
    {
        auto column = columns.tryGetColumnOrSubcolumn(GetColumnsOptions::All, name);
        auto object_column = object_columns.tryGetColumnOrSubcolumn(GetColumnsOptions::All, name);

        if (column && !object_column)
        {
            res.insert({column->type->createColumn(), column->type, column->name});
        }
        else if (object_column)
        {
            res.insert({object_column->type->createColumn(), object_column->type, object_column->name});
        }
        else if (auto it = virtual_columns.find(name); it != virtual_columns.end())
        {
            /// Virtual columns must be appended after ordinary, because user can
            /// override them.
            const auto & type = it->second;
            res.insert({type->createColumn(), type, name});
        }
        else
        {
            throw Exception(ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK,
                "Column {} not found in table {}", backQuote(name), storage.getStorageID().getNameForLogs());
        }
    }

    return res;
}

namespace
{

#if !defined(ARCADIA_BUILD)
    using DenseHashSet = google::dense_hash_set<StringRef, StringRefHash>;
#else
    using DenseHashSet = google::sparsehash::dense_hash_set<StringRef, StringRefHash>;
#endif

}

void StorageSnapshot::check(const Names & column_names) const
{
    const auto & columns = getMetadataForQuery()->getColumns();
    auto options = GetColumnsOptions(GetColumnsOptions::AllPhysical).withSubcolumns();

    if (column_names.empty())
    {
        auto list_of_columns = listOfColumns(columns.get(options));
        throw Exception(ErrorCodes::EMPTY_LIST_OF_COLUMNS_QUERIED,
            "Empty list of columns queried. There are columns: {}", list_of_columns);
    }

    DenseHashSet unique_names;
    unique_names.set_empty_key(StringRef());

    for (const auto & name : column_names)
    {
        bool has_column = columns.hasColumnOrSubcolumn(GetColumnsOptions::AllPhysical, name)
            || object_columns.hasColumnOrSubcolumn(GetColumnsOptions::AllPhysical, name)
            || virtual_columns.count(name);

        if (!has_column)
        {
            auto list_of_columns = listOfColumns(columns.get(options));
            throw Exception(ErrorCodes::NO_SUCH_COLUMN_IN_TABLE,
                "There is no column with name {} in table {}. There are columns: {}",
                backQuote(name), storage.getStorageID().getNameForLogs(), list_of_columns);
        }

        if (unique_names.count(name))
            throw Exception(ErrorCodes::COLUMN_QUERIED_MORE_THAN_ONCE, "Column {} queried more than once", name);

        unique_names.insert(name);
    }
}

DataTypePtr StorageSnapshot::getConcreteType(const String & column_name) const
{
    auto object_column = object_columns.tryGetColumnOrSubcolumn(GetColumnsOptions::All, column_name);
    if (object_column)
        return object_column->type;

    return metadata->getColumns().get(column_name).type;
}

}
