#include <mutex>
#include <unordered_map>
#include <utility>
#include <vector>

#include <Core/Block.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/StorageID.h>
#include <Interpreters/Context.h>

namespace DB {

using BufferAndSortingColumns = std::pair<Block, std::vector<size_t>>;
using BufferAndSortingColumnsPtr = std::shared_ptr<BufferAndSortingColumns>;
using MySQLDatabaseBuffer = std::unordered_map<String, BufferAndSortingColumnsPtr>;
using MySQLDatabaseBufferPtr = std::shared_ptr<MySQLDatabaseBuffer>;

class MySQLReplicaBuffer
{
public:
    std::vector<BufferAndSortingColumnsPtr> getTableDataBuffers(
        const String & mysql_table_name,
        const Context & context);

    Block readBlock(const StorageID & table_id);
    MySQLDatabaseBufferPtr readDatabaseBuffer();

    void registerTable(const StorageID & table_id, const String & mysql_table_name);
    void registerDatabase(const String & database_name);

private:
    bool checkThresholds(const StorageID & table_id);
    bool checkDatabaseThresholds();

private:
    std::mutex mutex;

    std::unordered_map<String, BufferAndSortingColumnsPtr> data;
    MySQLDatabaseBufferPtr database_data;

    std::unordered_map<String, std::vector<StorageID>> consumer_tables;
    std::optional<String> ch_database;
};

}
