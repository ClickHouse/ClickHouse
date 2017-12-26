#pragma once

#include <Storages/IStorage.h>
#include <mysqlxx/Pool.h>


namespace DB
{

/** Implements storage in the MySQL database.
  * Use ENGINE = mysql(host_port, database_name, table_name, user_name, password)
  * Read only.
  */
class StorageMySQL : public IStorage
{
public:
    StorageMySQL(
        const std::string & name,
        const std::string & host,
        UInt16 port,
        const std::string & remote_database_name,
        const std::string & remote_table_name,
        const std::string & user,
        const std::string & password,
        const NamesAndTypesList & columns);

    std::string getName() const override { return "MySQL"; }
    std::string getTableName() const override { return name; }
    const NamesAndTypesList & getColumnsListImpl() const override { return columns; }

    BlockInputStreams read(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

private:
    std::string name;

    std::string host;
    UInt16 port;
    std::string remote_database_name;
    std::string remote_table_name;
    std::string user;
    std::string password;

    NamesAndTypesList columns;
    mysqlxx::Pool pool;
};

}
