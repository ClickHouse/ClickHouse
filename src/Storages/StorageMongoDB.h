
#pragma once

#include <ext/shared_ptr_helper.h>

#include <Storages/IStorage.h>
#include <Interpreters/Context.h>

#include <Poco/MongoDB/Connection.h>


namespace DB
{
/* Implements storage in the MongoDB database.
 * Use ENGINE = mysql(host_port, database_name, table_name, user_name, password)
 * Read only.
 */

class StorageMongoDB final : public ext::shared_ptr_helper<StorageMongoDB>, public IStorage
{
    friend struct ext::shared_ptr_helper<StorageMongoDB>;
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
        const Context & context_);

    std::string getName() const override { return "MongoDB"; }

    Pipes read(
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;


private:
    std::string host;
    short unsigned int port;
    std::string database_name;
    std::string collection_name;
    std::string username;
    std::string password;

    Context global_context;
    std::shared_ptr<Poco::MongoDB::Connection> connection;
};

}
