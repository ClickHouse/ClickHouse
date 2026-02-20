#pragma once
#include "config.h"

#if USE_AVRO && USE_HIVE
#    include <filesystem>
#    include <Databases/DataLake/ICatalog.h>
#    include <IO/HTTPHeaderEntries.h>
#    include <IO/ReadWriteBufferFromHTTP.h>
#    include <Interpreters/Context_fwd.h>
#    include <Poco/JSON/Object.h>
#    include <Poco/Net/HTTPBasicCredentials.h>

#    include <ThriftHiveMetastore.h>
#    include <thrift/protocol/TBinaryProtocol.h>
#    include <thrift/transport/TSocket.h>
#    include <thrift/transport/TTransportUtils.h>


namespace DB
{
class ReadBuffer;
}

namespace DataLake
{

class HiveCatalog final : public ICatalog, private DB::WithContext
{
public:
    explicit HiveCatalog(
        const std::string & warehouse_, const std::string & base_url_, DB::ContextPtr context_);

    ~HiveCatalog() override = default;

    bool empty() const override;

    DB::Names getTables() const override;

    bool existsTable(const std::string & namespace_name, const std::string & table_name) const override;

    void getTableMetadata(const std::string & namespace_name, const std::string & table_name, TableMetadata & result) const override;

    bool tryGetTableMetadata(const std::string & namespace_name, const std::string & table_name, TableMetadata & result) const override;

    std::optional<StorageType> getStorageType() const override;

    DB::DatabaseDataLakeCatalogType getCatalogType() const override { return DB::DatabaseDataLakeCatalogType::ICEBERG_HIVE; }

private:
    std::shared_ptr<apache::thrift::transport::TTransport> socket;
    std::shared_ptr<apache::thrift::transport::TTransport> transport;
    std::shared_ptr<apache::thrift::protocol::TBinaryProtocol> protocol;
     /// Somehow API of apache::thrift::transport::TBufferBase is not thread-safe.
     /// Database Datalake can call this function from multiple threads, so we need to protect
     /// access to the client.
    mutable std::mutex client_mutex;

    mutable Apache::Hadoop::Hive::ThriftHiveMetastoreClient client TSA_GUARDED_BY(client_mutex);

};

}
#endif
