#include <Databases/MySQL/MySQLReplicaBuffer.h>
#include <Common/Exception.h>

namespace DB {

namespace ErrorCodes {
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
}

// These are called on table/database startup
void MySQLReplicaBuffer::registerTable(const StorageID & table_id, const String & mysql_table_name) {
    const std::lock_guard<std::mutex> lock(mutex);

    consumer_tables[mysql_table_name].push_back(table_id);
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

std::vector<BufferAndSortingColumnsPtr> MySQLReplicaBuffer::getTableDataBuffers(const String & mysql_table_name, const Context & context) {
    // TODO: Is there a way to check that mutex is taken?
    std::vector<BufferAndSortingColumnsPtr> result;
    for (const auto& table_id : consumer_tables[mysql_table_name]) {
        auto ch_table_name = table_id.getFullTableName();
        if (data.find(ch_table_name) == data.end()) {
            StoragePtr storage = DatabaseCatalog::instance().getTable(table_id, context);

            if (!storage) {
                continue;
            }

            const StorageInMemoryMetadata & metadata = storage->getInMemoryMetadata();
            BufferAndSortingColumnsPtr & buffer_and_soring_columns = data.try_emplace(
                ch_table_name,
                std::make_shared<BufferAndSortingColumns>(metadata.getSampleBlock(), std::vector<size_t>{})).first->second;

            Names required_names_for_sorting_key = metadata.getColumnsRequiredForSortingKey();

            for (const auto & required_name_for_sorting_key : required_names_for_sorting_key) {
                buffer_and_soring_columns->second.emplace_back(
                    buffer_and_soring_columns->first.getPositionByName(required_name_for_sorting_key));
            }
        }

        const auto it = data.find(ch_table_name);
        if (it != data.end()) {
            result.push_back(it->second);
        }
    }
    if (ch_database) {
        if (database_data.find(mysql_table_name) == database_data.end()) {
            StoragePtr storage = DatabaseCatalog::instance().getTable(StorageID(*ch_database, mysql_table_name), context);

            if (storage) {
                const StorageInMemoryMetadata & metadata = storage->getInMemoryMetadata();
                BufferAndSortingColumnsPtr & buffer_and_soring_columns = database_data.try_emplace(
                    mysql_table_name,
                    std::make_shared<BufferAndSortingColumns>(metadata.getSampleBlock(), std::vector<size_t>{})).first->second;

                Names required_names_for_sorting_key = metadata.getColumnsRequiredForSortingKey();

                for (const auto & required_name_for_sorting_key : required_names_for_sorting_key) {
                    buffer_and_soring_columns->second.emplace_back(
                        buffer_and_soring_columns->first.getPositionByName(required_name_for_sorting_key));
                }
            }
        }

        const auto it = database_data.find(ch_table_name);
        if (it != database_data.end()) {
            result.push_back(it->second);
        }
    }
}

Block MySQLReplicaBuffer::readBlock(const StorageID & table_id) {
    std::lock_guard<std::mutex> lock(mutex);

    const auto it = data.find(table_id.getFullTableName());
    if (checkThresholds(table_id) && it) {

        auto old_buffer = it->second;
        data[table_id.getFullTableName()] = std::make_shared<BufferAndSortingColumns>(old_buffer->first.cloneEmpty(), old_buffer->second);

        return old_buffer->first;
    }

    return Block();
}

MySQLDatabaseBufferPtr readDatabaseBuffer() {
    std::lock_guard<std::mutex> lock(mutex);

    if (!ch_database) {
        throw Exception(
            "Call registerTable first",
            ErrorCodes::LOGICAL_ERROR);
    }

    if (!checkDatabaseThresholds()) {
        return nullptr;
    }

    MySQLDatabaseBufferPtr oldBuffer = database_data;
    database_data = std::make_shared<MySQLDatabaseBuffer>();

    return oldBuffer;
}

bool MySQLReplicaBuffer::checkThresholds(const StorageID & /*table_id*/) {
    return true;
}

bool MySQLReplicaBuffer::checkDatabaseThresholds() {
    return true;
}
