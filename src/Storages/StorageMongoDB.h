#pragma once

#include <Poco/MongoDB/Connection.h>

#include <base/shared_ptr_helper.h>

#include <Storages/IStorage.h>
#include <Storages/NamedCollections.h>


namespace DB
{
/* Implements storage in the MongoDB database.
 * Use ENGINE = mysql(host_port, database_name, table_name, user_name, password)
 * Read only.
 */

class StorageMongoDB final : public shared_ptr_helper<StorageMongoDB>, public IStorage
{
    friend struct shared_ptr_helper<StorageMongoDB>;
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
        const String & comment);

    std::string getName() const override { return "MongoDB"; }

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    struct Configuration : public StorageConfiguration
    {
        String host;
        UInt16 port;
        String database;
        String table;
        String username;
        String password;
        String options;
    };

    static Configuration getConfiguration(ASTs engine_args, ContextPtr context);

    static NamedConfiguration getConfigKeys()
    {
        static const NamedConfiguration config_keys =
        {
            {"host", ConfigKeyInfo{ .type = Field::Types::String }},
            {"port", ConfigKeyInfo{ .type = Field::Types::UInt64 }},
            {"database", ConfigKeyInfo{ .type = Field::Types::String }},
            {"table", ConfigKeyInfo{ .type = Field::Types::String }},
            {"collection", ConfigKeyInfo{ .type = Field::Types::String }},
            {"user", ConfigKeyInfo{ .type = Field::Types::String }},
            {"password", ConfigKeyInfo{ .type = Field::Types::String }},
            {"options", ConfigKeyInfo{ .type = Field::Types::String }},
            {"format", ConfigKeyInfo{ .type = Field::Types::String, .default_value = "auto" }},
            {"compression_method", ConfigKeyInfo{ .type = Field::Types::String, .default_value = "auto" }},
            {"structure", ConfigKeyInfo{ .type = Field::Types::String, .default_value = "auto" }},
        };
        return config_keys;
    }
private:
    void connectIfNotConnected();

    const std::string host;
    const uint16_t port; /// NOLINT
    const std::string database_name;
    const std::string collection_name;
    const std::string username;
    const std::string password;
    const std::string options;
    const std::string uri;

    std::shared_ptr<Poco::MongoDB::Connection> connection;
    bool authenticated = false;
    std::mutex connection_mutex; /// Protects the variables `connection` and `authenticated`.
};

}
