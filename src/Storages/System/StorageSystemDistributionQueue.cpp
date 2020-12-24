#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/System/StorageSystemDistributionQueue.h>
#include <Storages/Distributed/DirectoryMonitor.h>
#include <Storages/StorageDistributed.h>
#include <Storages/VirtualColumnUtils.h>
#include <Access/ContextAccess.h>
#include <Common/typeid_cast.h>
#include <Databases/IDatabase.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

}


namespace
{

using namespace DB;

/// Drop "password" from the path.
///
/// In case of use_compact_format_in_distributed_parts_names=0 the path format is:
///
///     user[:password]@host:port#default_database format
///
/// And password should be masked out.
///
/// See:
/// - Cluster::Address::fromFullString()
/// - Cluster::Address::toFullString()
std::string maskDataPath(const std::string & path)
{
    std::string masked_path = path;

    if (!masked_path.ends_with('/'))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid path format");

    masked_path.pop_back();

    size_t node_pos = masked_path.rfind('/');
    /// Loop through each node, that separated with a comma
    while (node_pos != std::string::npos)
    {
        ++node_pos;

        size_t user_pw_end = masked_path.find('@', node_pos);
        if (user_pw_end == std::string::npos)
        {
            /// Likey new format (use_compact_format_in_distributed_parts_names=1)
            return path;
        }

        size_t pw_start = masked_path.find(':', node_pos);
        if (pw_start > user_pw_end)
        {
            /// No password in path
            return path;
        }
        ++pw_start;

        size_t pw_length = user_pw_end - pw_start;
        /// Replace with a single '*' to hide even the password length.
        masked_path.replace(pw_start, pw_length, 1, '*');

        /// "," cannot be in the node specification since it will be encoded in hex.
        node_pos = masked_path.find(',', node_pos);
    }

    masked_path.push_back('/');

    return masked_path;
}

}

namespace DB
{


NamesAndTypesList StorageSystemDistributionQueue::getNamesAndTypes()
{
    return {
        { "database",              std::make_shared<DataTypeString>() },
        { "table",                 std::make_shared<DataTypeString>() },
        { "data_path",             std::make_shared<DataTypeString>() },
        { "is_blocked",            std::make_shared<DataTypeUInt8>()  },
        { "error_count",           std::make_shared<DataTypeUInt64>() },
        { "data_files",            std::make_shared<DataTypeUInt64>() },
        { "data_compressed_bytes", std::make_shared<DataTypeUInt64>() },
        { "last_exception",        std::make_shared<DataTypeString>() },
    };
}


void StorageSystemDistributionQueue::fillData(MutableColumns & res_columns, const Context & context, const SelectQueryInfo & query_info) const
{
    const auto access = context.getAccess();
    const bool check_access_for_databases = !access->isGranted(AccessType::SHOW_TABLES);

    std::map<String, std::map<String, StoragePtr>> tables;
    for (const auto & db : DatabaseCatalog::instance().getDatabases())
    {
        /// Check if database can contain distributed tables
        if (!db.second->canContainDistributedTables())
            continue;

        const bool check_access_for_tables = check_access_for_databases && !access->isGranted(AccessType::SHOW_TABLES, db.first);

        for (auto iterator = db.second->getTablesIterator(context); iterator->isValid(); iterator->next())
        {
            StoragePtr table = iterator->table();
            if (!table)
                continue;

            if (!dynamic_cast<const StorageDistributed *>(table.get()))
                continue;
            if (check_access_for_tables && !access->isGranted(AccessType::SHOW_TABLES, db.first, iterator->name()))
                continue;
            tables[db.first][iterator->name()] = table;
        }
    }


    MutableColumnPtr col_database_mut = ColumnString::create();
    MutableColumnPtr col_table_mut = ColumnString::create();

    for (auto & db : tables)
    {
        for (auto & table : db.second)
        {
            col_database_mut->insert(db.first);
            col_table_mut->insert(table.first);
        }
    }

    ColumnPtr col_database_to_filter = std::move(col_database_mut);
    ColumnPtr col_table_to_filter = std::move(col_table_mut);

    /// Determine what tables are needed by the conditions in the query.
    {
        Block filtered_block
        {
            { col_database_to_filter, std::make_shared<DataTypeString>(), "database" },
            { col_table_to_filter, std::make_shared<DataTypeString>(), "table" },
        };

        VirtualColumnUtils::filterBlockWithQuery(query_info.query, filtered_block, context);

        if (!filtered_block.rows())
            return;

        col_database_to_filter = filtered_block.getByName("database").column;
        col_table_to_filter = filtered_block.getByName("table").column;
    }

    for (size_t i = 0, tables_size = col_database_to_filter->size(); i < tables_size; ++i)
    {
        String database = (*col_database_to_filter)[i].safeGet<const String &>();
        String table = (*col_table_to_filter)[i].safeGet<const String &>();

        auto & distributed_table = dynamic_cast<StorageDistributed &>(*tables[database][table]);

        for (const auto & status : distributed_table.getDirectoryMonitorsStatuses())
        {
            size_t col_num = 0;
            res_columns[col_num++]->insert(database);
            res_columns[col_num++]->insert(table);
            res_columns[col_num++]->insert(maskDataPath(status.path));
            res_columns[col_num++]->insert(status.is_blocked);
            res_columns[col_num++]->insert(status.error_count);
            res_columns[col_num++]->insert(status.files_count);
            res_columns[col_num++]->insert(status.bytes_count);

            if (status.last_exception)
                res_columns[col_num++]->insert(getExceptionMessage(status.last_exception, false));
            else
                res_columns[col_num++]->insertDefault();
        }
    }
}

}
