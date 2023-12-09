#pragma once

#include <Poco/MongoDB/Connection.h>

#include <Storages/IStorage.h>

namespace DB
{
/* Implements storage in the MongoDB database.
 * Use ENGINE = MongoDB(host:port, database, collection, user, password [, options]);
 * Read only.
 */

class StorageMongoDB final : public IStorage
{
public:
    StorageMongoDB(
        const StorageID & table_id_,
        const std::string & host_,
        uint16_t port_,
        const std::string & database_name_,
        const std::string & collection_name_,
        const std::string & username_,
        const std::string & password_,
        const std::string & options_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment,
        const FormatSettings & format_settings);

    std::string getName() const override { return "MongoDB"; }

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    SinkToStoragePtr write(
        const ASTPtr & query,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        ContextPtr context,
        bool async_insert) override;

    struct Configuration
    {
        std::string host;
        UInt16 port;
        std::string username;
        std::string password;
        std::string database;
        std::string table;
        std::string options;
    };

    static ColumnsDescription getTableStructureFromData(
        std::shared_ptr<Poco::MongoDB::Connection> & connection_,
        const std::string & database_name_,
        const std::string & collection_name_,
        const FormatSettings & format_settings);

    static Configuration getConfiguration(ASTs engine_args, ContextPtr context);

    static std::string getMongoURI(
        const std::string & host_,
        uint16_t port_,
        const std::string & database_name_,
        const std::string & options_);

    static std::shared_ptr<Poco::MongoDB::Connection> getConnection(
        const std::string & uri_,
        const std::string & database_name_,
        const std::string & username_,
        const std::string & password_);

private:
    void connectIfNotConnected();

    const std::string database_name;
    const std::string collection_name;
    const std::string username;
    const std::string password;
    const std::string uri;

    std::shared_ptr<Poco::MongoDB::Connection> connection;
    std::mutex connection_mutex; /// Protects the variable `connection`.
};

}
