#include <Storages/System/StorageSystemFileLogFiles.h>
#include <Storages/System/SystemTableSourceRegistry.h>

#if USE_FILELOG

#include <Access/ContextAccess.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Storages/FileLog/StorageFileLog.h>
#include <Storages/StorageTableProxy.h>

#include <sys/stat.h>


namespace DB
{

ColumnsDescription StorageSystemFileLogFiles::getColumnsDescription()
{
    return ColumnsDescription{
        {"database", std::make_shared<DataTypeString>(), "Database of the table with the FileLog engine."},
        {"table", std::make_shared<DataTypeString>(), "Name of the table with the FileLog engine."},
        {"file_name", std::make_shared<DataTypeString>(), "Name of the file in the directory of the table, the same as the `_filename` virtual column."},
        {"path", std::make_shared<DataTypeString>(), "Absolute path of the file."},
        {"inode", std::make_shared<DataTypeUInt64>(), "Inode of the file. The table tracks files by inode, so a renamed file keeps its row."},
        {"current_offset", std::make_shared<DataTypeUInt64>(), "Number of bytes of the file already read. The next read starts at this offset."},
        {"file_size", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()),
         "Current size of the file. `file_size - current_offset` is the number of bytes not read yet. "
         "`NULL` if `path` no longer refers to `inode`, because the file was renamed or removed and the table has not processed it yet."},
        {"num_records_read", std::make_shared<DataTypeUInt64>(), "Number of records read from the file since the table was loaded."},
        {"last_poll_time", std::make_shared<DataTypeDateTime>(),
         "Time when the table last started reading the file, including reads that returned nothing or failed."},
        {"last_exception", std::make_shared<DataTypeString>(),
         "Text of the most recent exception of a background read round (to materialized views) that included the file. "
         "It is kept after the file is consumed again."},
        {"last_exception_time", std::make_shared<DataTypeDateTime>(), "Time of `last_exception`."},
        {"state",
         std::make_shared<DataTypeEnum8>(DataTypeEnum8::Values{
             {"consuming", 0},
             {"stuck", 1},
         }),
         "`stuck` if the last background read round that included the file failed, `consuming` otherwise."},
    };
}

void StorageSystemFileLogFiles::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    const auto access = context->getAccess();
    const bool show_tables_granted = access->isGranted(AccessType::SHOW_TABLES);

    for (const auto & db : DatabaseCatalog::instance().getDatabases(GetDatabasesOptions{.with_datalake_catalogs = false}))
    {
        if (db.first == DatabaseCatalog::TEMPORARY_DATABASE || db.second->isExternal())
            continue;

        for (auto it = db.second->getTablesIterator(context, {}, /* skip_not_loaded */ true); it->isValid(); it->next())
        {
            StoragePtr table = it->table();
            if (const auto * proxy = dynamic_cast<const StorageTableProxy *>(table.get()))
                table = proxy->tryGetNested();
            const auto * file_log = dynamic_cast<const StorageFileLog *>(table.get());
            if (!file_log)
                continue;
            if (!show_tables_granted && !access->isGranted(AccessType::SHOW_TABLES, it->databaseName(), it->name()))
                continue;

            for (const auto & [inode, statistics] : file_log->getFileStatistics())
            {
                const String path = file_log->getFullDataPath(statistics.file_name);

                size_t i = 0;
                res_columns[i++]->insert(it->databaseName());
                res_columns[i++]->insert(it->name());
                res_columns[i++]->insert(statistics.file_name);
                res_columns[i++]->insert(path);
                res_columns[i++]->insert(inode);
                res_columns[i++]->insert(statistics.offset);

                struct stat file_stat{};
                if (::stat(path.c_str(), &file_stat) == 0 && file_stat.st_ino == inode)
                    res_columns[i++]->insert(static_cast<UInt64>(file_stat.st_size));
                else
                    res_columns[i++]->insertDefault();

                res_columns[i++]->insert(statistics.num_records_read);
                res_columns[i++]->insert(statistics.last_poll_time);
                res_columns[i++]->insert(statistics.last_exception);
                res_columns[i++]->insert(statistics.last_exception_time);
                res_columns[i++]->insert(static_cast<Int8>(statistics.stuck ? 1 : 0));
            }
        }
    }
}

}

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemFileLogFiles) }

#endif
