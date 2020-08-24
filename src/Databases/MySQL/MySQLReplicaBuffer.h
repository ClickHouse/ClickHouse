#include <mutex>
#include <Interpretes/StorageID.h>

namespace DB {

using BufferAndSortingColumns = std::pair<Block, std::vector<size_t>>;
using BufferAndSortingColumnsPtr = std::shared_ptr<BufferAndSortingColumns>;

class MySQLReplicaBuffer {
public:
    BufferAndSortingColumnsPtr getTableDataBuffer(const String & mysql_table_name, const Context & context);
    void finishProcessing();

    BlockPtr readNextBlock(const StorageID & table_id);

    void registerTable(const StorageID & table_id);
    void registerDatabase(const String & database_name);

private:
    void flushData();

    bool checkThresholds(const String & mysql_table_name);

private:
    std::mutex mutex;

    std::unordered_map<String, BufferAndSortingColumnsPtr> data;
    std::unordered_map<String, std::queue<BlockPtr>> flushedData;

    std::unordered_map<String, std::vector<StorageID>> consumerTables;
    std::optional<String> chDatabase;
};

}
