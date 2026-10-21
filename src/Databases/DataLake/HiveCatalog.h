#pragma once
#include "config.h"

#if USE_AVRO && USE_HIVE
#    include <condition_variable>
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
    HiveCatalog(
        const std::string & warehouse_, const std::string & base_url_, size_t max_connections_, DB::ContextPtr context_);

    ~HiveCatalog() override = default;

    bool empty() const override;

    CatalogTables getTables() const override;

    Namespaces getNamespaces() const override;

    bool existsTable(const std::string & namespace_name, const std::string & table_name) const override;

    void getTableMetadata(const std::string & namespace_name, const std::string & table_name, TableMetadata & result) const override;

    bool tryGetTableMetadata(const std::string & namespace_name, const std::string & table_name, TableMetadata & result) const override;

    std::optional<StorageType> getStorageType() const override;

    DB::DatabaseDataLakeCatalogType getCatalogType() const override { return DB::DatabaseDataLakeCatalogType::ICEBERG_HIVE; }

    DataLakeTableFormat getTableFormat(const TableMetadata &) const override { return DataLakeTableFormat::ICEBERG; }

private:
    /// A Thrift transport is not thread-safe, so each connection is used by one thread at a time.
    struct Connection
    {
        std::shared_ptr<apache::thrift::transport::TTransport> transport;
        std::unique_ptr<Apache::Hadoop::Hive::ThriftHiveMetastoreClient> client;
        UInt64 generation = 0;

        ~Connection();
    };
    using ConnectionPtr = std::unique_ptr<Connection>;

    ConnectionPtr createConnection() const;
    ConnectionPtr acquireConnection() const;
    void releaseConnection(ConnectionPtr connection) const;
    void discardConnection(ConnectionPtr connection) const;
    void invalidateConnections(ConnectionPtr failed_connection) const;

    template <typename Func>
    void executeWithRetry(Func && func) const;

    CatalogTables listTablesInNamespaceDirect(const std::string & namespace_name) const override;

    String base_url;
    const size_t max_connections;

    mutable std::mutex pool_mutex;
    mutable std::condition_variable connection_released;
    mutable std::vector<ConnectionPtr> idle_connections TSA_GUARDED_BY(pool_mutex);
    mutable size_t num_connections TSA_GUARDED_BY(pool_mutex) = 0;
    mutable UInt64 generation TSA_GUARDED_BY(pool_mutex) = 0;
};

}
#endif
