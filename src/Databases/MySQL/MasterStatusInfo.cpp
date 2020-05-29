#include <Core/Block.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Databases/MySQL/MasterStatusInfo.h>
#include <Formats/MySQLBlockInputStream.h>
#include <Common/quoteString.h>
#include <Poco/File.h>
#include <IO/Operators.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>

namespace DB
{

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
void MasterStatusInfo::fetchMasterStatus(mysqlxx::PoolWithFailover::Entry & connection)
{
    Block header{
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

bool MasterStatusInfo::checkBinlogFileExists(mysqlxx::PoolWithFailover::Entry & connection)
{
    Block header{
        {std::make_shared<DataTypeString>(), "Log_name"},
        {std::make_shared<DataTypeUInt64>(), "File_size"},
        {std::make_shared<DataTypeString>(), "Encrypted"}
    };

    MySQLBlockInputStream input(connection, "SHOW MASTER LOGS", header, DEFAULT_BLOCK_SIZE);

    while (Block block = input.read())
    {
        for (size_t index = 0; index < block.rows(); ++index)
        {
            const auto & log_name = (*block.getByPosition(0).column)[index].safeGet<String>();
            if (log_name == binlog_file)
                return true;
        }
    }
    return false;
}
void MasterStatusInfo::finishDump()
{
    WriteBufferFromFile out(persistent_path);
    out << "Version:\t1\n"
        << "Binlog File:\t" << binlog_file << "\nBinlog Position:\t" << binlog_position << "\nBinlog Do DB:\t" << binlog_do_db
        << "\nBinlog Ignore DB:\t" << binlog_ignore_db << "\nExecuted GTID SET:\t" << executed_gtid_set;

    out.next();
    out.sync();
}

void MasterStatusInfo::transaction(const MySQLReplication::Position & position, const std::function<void()> & fun)
{
    binlog_file = position.binlog_name;
    binlog_position = position.binlog_pos;

    {
        Poco::File temp_file(persistent_path + ".temp");
        if (temp_file.exists())
            temp_file.remove();
    }

    WriteBufferFromFile out(persistent_path + ".temp");
    out << "Version:\t1\n"
        << "Binlog File:\t" << binlog_file << "\nBinlog Position:\t" << binlog_position << "\nBinlog Do DB:\t" << binlog_do_db
        << "\nBinlog Ignore DB:\t" << binlog_ignore_db << "\nExecuted GTID SET:\t" << executed_gtid_set;
    out.next();
    out.sync();

    fun();
    Poco::File(persistent_path + ".temp").renameTo(persistent_path);
}

MasterStatusInfo::MasterStatusInfo(mysqlxx::PoolWithFailover::Entry & connection, const String & path_, const String & database)
    : persistent_path(path_)
{
    if (Poco::File(persistent_path).exists())
    {
        ReadBufferFromFile in(persistent_path);
        in >> "Version:\t1\n" >> "Binlog File:\t" >> binlog_file >> "\nBinlog Position:\t" >> binlog_position >> "\nBinlog Do DB:\t"
            >> binlog_do_db >> "\nBinlog Ignore DB:\t" >> binlog_ignore_db >> "\nExecuted GTID SET:\t" >> executed_gtid_set;

        if (checkBinlogFileExists(connection))
        {
            std::cout << "Load From File \n";
            return;
        }
    }

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
        /// TODO: 拉取建表语句, 解析并构建出表结构(列列表, 主键, 唯一索引, 分区键)
    }
    catch (...)
    {
        if (locked_tables)
            connection->query("UNLOCK TABLES;").execute();

        throw;
    }
}

}
