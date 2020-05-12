#include <Core/Block.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Databases/MySQL/MasterStatusInfo.h>
#include <Formats/MySQLBlockInputStream.h>
#include <Common/quoteString.h>

namespace DB
{
/*MasterStatusInfo::MasterStatusInfo(
    String binlog_file_, UInt64 binlog_position_, String binlog_do_db_, String binlog_ignore_db_, String executed_gtid_set_)
    : binlog_file(binlog_file_), binlog_position(binlog_position_), binlog_do_db(binlog_do_db_), binlog_ignore_db(binlog_ignore_db_),
    executed_gtid_set(executed_gtid_set_)
{
}*/

static std::vector<String> fetchTablesInDB(const mysqlxx::PoolWithFailover::Entry & connection, const std::string & database)
{
    Block header{{std::make_shared<DataTypeString>(), "table_name"}};
    String query = "SELECT TABLE_NAME AS table_name FROM INFORMATION_SCHEMA.TABLES  WHERE TABLE_SCHEMA = " + quoteString(database);

    std::vector<String> tables_in_db;
    MySQLBlockInputStream input(connection, query, header, DEFAULT_BLOCK_SIZE);

    while (Block block = input.read())
    {
        tables_in_db.reserve(tables_in_db.size() + block.rows());
        for (size_t index = 0; index < block.rows(); ++index)
            tables_in_db.emplace_back((*block.getByPosition(0).column)[index].safeGet<String>());
    }

    return tables_in_db;
}

MasterStatusInfo::MasterStatusInfo(mysqlxx::PoolWithFailover::Entry & connection, const String & database)
{
    bool locked_tables = false;

    try
    {
        connection->query("FLUSH TABLES;").execute();
        connection->query("FLUSH TABLES WITH READ LOCK;").execute();

        locked_tables = true;
        fetchMasterStatus(connection);
        connection->query("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;").execute();
        connection->query("START TRANSACTION /*!40100 WITH CONSISTENT SNAPSHOT */;").execute();

        need_dumping_tables = fetchTablesInDB(connection, database);
        connection->query("UNLOCK TABLES;").execute();
    }
    catch (...)
    {
        if (locked_tables)
            connection->query("UNLOCK TABLES;").execute();

        throw;
    }
}
void MasterStatusInfo::fetchMasterStatus(mysqlxx::PoolWithFailover::Entry & connection)
{
    Block header
        {
            {std::make_shared<DataTypeString>(), "File"},
            {std::make_shared<DataTypeUInt64>(), "Position"},
            {std::make_shared<DataTypeString>(), "Binlog_Do_DB"},
            {std::make_shared<DataTypeString>(), "Binlog_Ignore_DB"},
            {std::make_shared<DataTypeString>(), "Executed_Gtid_Set"},
        };

    MySQLBlockInputStream input(connection, "SHOW MASTER STATUS;", header, DEFAULT_BLOCK_SIZE);
    Block master_status = input.read();

    if (!master_status || master_status.rows() != 1)
        throw Exception("Unable to get master status from MySQL.", ErrorCodes::LOGICAL_ERROR);

    binlog_file = (*master_status.getByPosition(0).column)[0].safeGet<String>();
    binlog_position = (*master_status.getByPosition(1).column)[0].safeGet<UInt64>();
    binlog_do_db = (*master_status.getByPosition(2).column)[0].safeGet<String>();
    binlog_ignore_db = (*master_status.getByPosition(3).column)[0].safeGet<String>();
    executed_gtid_set = (*master_status.getByPosition(4).column)[0].safeGet<String>();
}

}
