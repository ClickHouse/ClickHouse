#pragma once

#include <common/shared_ptr_helper.h>

#include <Storages/IStorage.h>

#include <Poco/MongoDB/Connection.h>


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
        short unsigned int port_,
        const std::string & database_name_,
        const std::string & collection_name_,
        const std::string & username_,
        const std::string & password_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment);

    std::string getName() const override { return "MongoDB"; }

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

private:
    void connectIfNotConnected();

    const std::string host;
    const short unsigned int port;
    const std::string database_name;
    const std::string collection_name;
    const std::string username;
    const std::string password;

    std::shared_ptr<Poco::MongoDB::Connection> connection;
    bool authenticated = false;
    std::mutex connection_mutex; /// Protects the variables `connection` and `authenticated`.
};

}
