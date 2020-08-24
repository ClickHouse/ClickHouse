#include <Common/Exception.h>

namespace DB {

namespace ErrorCodes {
    extern const int NOT_IMPLEMENTED;
}

void MySQLReplicaBuffer::flushData() {
    const std::lock_guard<std::mutex> lock(mutex);

    for (auto & [mysql_table_name, buffer] : data) {
        if (!(force || checkThresholds(mysql_table_name))) {
            continue;
        }

        const auto & ch_tables = consumerTables[mysql_table_name];
        for (const auto & ch_table_id : ch_tables) {
            flushedData[ch_table_id.getFullTableName()].push(&data[mysql_table_name].first);
        }

        StoragePtr storage = DatabaseCatalog::instance().getTable(StorageID(chDatabase, mysql_table_name), context);
        if (storage) {
            flushedData[storage.getStorageID().getFullTableName()].push(&data[mysql_table_name].first);
        }

        auto oldBuffer = data[mysql_table_name];
        data[mysql_database_name] = std::make_shared<BufferAndSortingColumns>(oldBuffer->first.cloneEmpty(), oldBuffer->second);
    }
}

// These are called on table/database startup
void MySQLReplicaBuffer::registerTable(const StorageID & table_id, const String & mysql_table_name) {
    const std::lock_guard<std::mutex> lock(mutex);

    consumerTables[mysql_table_name].push_back(table_id);
}

void MySQLReplicaBuffer::registerDatabase(const String & database_name) {
    const std::lock_guard<std::mutex> lock(mutex);

    if (chDatabase) {
        throw Exception(
            "Only one CH database can be replica of remote MySQL databse",
            ErrorCodes::NOT_IMPLEMENTED);
    }

    chDatabase = database_name;
}

BufferAndSortingColumnsPtr MySQLReplicaBuffer::getTableDataBuffer(const String & mysql_table_name, const Context & context) {
    if (data.find(mysql_table_name) == data.end()) {
        StoragePtr storage;
        if (!consumerTables[mysql_table_name].empty()) {
            storage = DatabaseCatalog::instance().getTable(consumerTables[mysql_table_name].front(), context);
        }
        if (chDatabase) {
            storage = DatabaseCatalog::instance().getTable(StorageID(*chDatabase, mysql_table_name), context);
        }

        if (!storage) {
            return nullptr;
        }

        const StorageInMemoryMetadata & metadata = storage->getInMemoryMetadata();
        BufferAndSortingColumnsPtr & buffer_and_soring_columns = data.try_emplace(
            mysql_table_name,
            std::make_shared<BufferAndSortingColumns>(metadata.getSampleBlock(), std::vector<size_t>{})).first->second;

        Names required_for_sorting_key = metadata.getColumnsRequiredForSortingKey();

        for (const auto & required_name_for_sorting_key : required_for_sorting_key) {
            buffer_and_soring_columns->second.emplace_back(
                buffer_and_soring_columns->first.getPositionByName(required_name_for_sorting_key));
        }
    }

    if (data.find(mysql_table_name) != data.end()) {
        return data[mysql_table_name];
    }
}

bool MySQLReplicaBuffer::checkThresholds(const String & /*mysql_table_name*/) {
    return true;
}

void MySQLReplicaBuffer::finishProcessing() {
    flushData();
}

BlockPtr MySQLReplicaBuffer::readNextBlock(const StorageID & table_id) {
    const std::lock_guard<std::mutex> lock(mutex);

    auto result = flushedData[table_id.getFullTableName()].front();
    flushedData[table_id.getFullTableName()].pop();
    return result;
}

}
