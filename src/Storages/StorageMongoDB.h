#pragma once

#include <Poco/MongoDB/Connection.h>

#include <Storages/IStorage.h>
#include <Storages/ExternalDataSourceConfiguration.h>

namespace DB
{
/* Implements storage in the MongoDB database.
 * Use ENGINE = mysql(host_port, database_name, table_name, user_name, password)
 * Read only.
 */

class StorageMongoDB final : public IStorage
{
private:
    struct CreatePasskey
    {
    };

public:
    template <typename... TArgs>
    static std::shared_ptr<StorageMongoDB> create(TArgs &&... args)
    {
        return std::make_shared<StorageMongoDB>(CreatePasskey{}, std::forward<TArgs>(args)...);
    }

    template <typename... TArgs>
    explicit StorageMongoDB(CreatePasskey, TArgs &&... args) : StorageMongoDB{std::forward<TArgs>(args)...}
    {
    }

    std::string getName() const override { return "MongoDB"; }

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    static StorageMongoDBConfiguration getConfiguration(ASTs engine_args, ContextPtr context);

private:
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

    void connectIfNotConnected();

    const std::string host;
    [[maybe_unused]] const uint16_t port; /// NOLINT -- forgotten to remove or an error? needs checking
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
