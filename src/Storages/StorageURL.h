#pragma once

#include <Poco/URI.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Formats/FormatSettings.h>
#include <IO/CompressionMethod.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <IO/HTTPHeaderEntries.h>
#include <Storages/IStorage.h>
#include <Storages/StorageFactory.h>
#include <Storages/Cache/SchemaCache.h>
#include <Storages/StorageConfiguration.h>


namespace DB
{

class IOutputFormat;
using OutputFormatPtr = std::shared_ptr<IOutputFormat>;

struct ConnectionTimeouts;
class NamedCollection;

/**
 * This class represents table engine for external urls.
 * It sends HTTP GET to server when select is called and
 * HTTP POST when insert is called. In POST request the data is send
 * using Chunked transfer encoding, so server have to support it.
 */
class IStorageURLBase : public IStorage
{
public:
    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr context) override;

    bool supportsPartitionBy() const override { return true; }

    static ColumnsDescription getTableStructureFromData(
        const String & format,
        const String & uri,
        CompressionMethod compression_method,
        const HTTPHeaderEntries & headers,
        const std::optional<FormatSettings> & format_settings,
        ContextPtr context);

    static SchemaCache & getSchemaCache(const ContextPtr & context);

protected:
    IStorageURLBase(
        const String & uri_,
        ContextPtr context_,
        const StorageID & id_,
        const String & format_name_,
        const std::optional<FormatSettings> & format_settings_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment,
        const String & compression_method_,
        const HTTPHeaderEntries & headers_ = {},
        const String & method_ = "",
        ASTPtr partition_by = nullptr);

    String uri;
    CompressionMethod compression_method;
    String format_name;
    // For URL engine, we use format settings from server context + `SETTINGS`
    // clause of the `CREATE` query. In this case, format_settings is set.
    // For `url` table function, we use settings from current query context.
    // In this case, format_settings is not set.
    std::optional<FormatSettings> format_settings;
    HTTPHeaderEntries headers;
    String http_method; /// For insert can choose Put instead of default Post.
    ASTPtr partition_by;

    virtual std::string getReadMethod() const;

    virtual std::vector<std::pair<std::string, std::string>> getReadURIParams(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        const SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size) const;

    virtual std::function<void(std::ostream &)> getReadPOSTDataCallback(
        const Names & column_names,
        const ColumnsDescription & columns_description,
        const SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size) const;

    bool supportsSubsetOfColumns() const override;

    bool parallelizeOutputAfterReading(ContextPtr context) const override;

private:
    virtual Block getHeaderBlock(const Names & column_names, const StorageSnapshotPtr & storage_snapshot) const = 0;

    static std::optional<ColumnsDescription> tryGetColumnsFromCache(
        const Strings & urls,
        const HTTPHeaderEntries & headers,
        const Poco::Net::HTTPBasicCredentials & credentials,
        const String & format_name,
        const std::optional<FormatSettings> & format_settings,
        const ContextPtr & context);

    static void addColumnsToCache(
        const Strings & urls,
        const ColumnsDescription & columns,
        const String & format_name,
        const std::optional<FormatSettings> & format_settings,
        const ContextPtr & context);

    static std::optional<time_t> getLastModificationTime(
        const String & url,
        const HTTPHeaderEntries & headers,
        const Poco::Net::HTTPBasicCredentials & credentials,
        const ContextPtr & context);
};

class StorageURLSink : public SinkToStorage
{
public:
    StorageURLSink(
        const String & uri,
        const String & format,
        const std::optional<FormatSettings> & format_settings,
        const Block & sample_block,
        ContextPtr context,
        const ConnectionTimeouts & timeouts,
        CompressionMethod compression_method,
        const HTTPHeaderEntries & headers = {},
        const String & method = Poco::Net::HTTPRequest::HTTP_POST);

    std::string getName() const override { return "StorageURLSink"; }
    void consume(Chunk chunk) override;
    void onCancel() override;
    void onException() override;
    void onFinish() override;

private:
    void finalize();
    std::unique_ptr<WriteBuffer> write_buf;
    OutputFormatPtr writer;
    std::mutex cancel_mutex;
    bool cancelled = false;
};

class StorageURL : public IStorageURLBase
{
public:
    StorageURL(
        const String & uri_,
        const StorageID & table_id_,
        const String & format_name_,
        const std::optional<FormatSettings> & format_settings_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment,
        ContextPtr context_,
        const String & compression_method_,
        const HTTPHeaderEntries & headers_ = {},
        const String & method_ = "",
        ASTPtr partition_by_ = nullptr);

    String getName() const override
    {
        return "URL";
    }

    Block getHeaderBlock(const Names & /*column_names*/, const StorageSnapshotPtr & storage_snapshot) const override
    {
        return storage_snapshot->metadata->getSampleBlock();
    }

    static FormatSettings getFormatSettingsFromArgs(const StorageFactory::Arguments & args);

    struct Configuration : public StatelessTableEngineConfiguration
    {
        std::string url;
        std::string http_method;
        HTTPHeaderEntries headers;
        std::string addresses_expr;
    };

    static Configuration getConfiguration(ASTs & args, ContextPtr context);

    static ASTs::iterator collectHeaders(ASTs & url_function_args, HTTPHeaderEntries & header_entries, ContextPtr context);

    static void processNamedCollectionResult(Configuration & configuration, const NamedCollection & collection);
};


/// StorageURLWithFailover is allowed only for URL table function, not as a separate storage.
class StorageURLWithFailover final : public StorageURL
{
public:
    StorageURLWithFailover(
            const std::vector<String> & uri_options_,
            const StorageID & table_id_,
            const String & format_name_,
            const std::optional<FormatSettings> & format_settings_,
            const ColumnsDescription & columns_,
            const ConstraintsDescription & constraints_,
            ContextPtr context_,
            const String & compression_method_);

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

private:
    std::vector<String> uri_options;
};
}
