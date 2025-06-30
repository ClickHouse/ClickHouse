#pragma once

#include <Formats/FormatSettings.h>
#include <IO/CompressionMethod.h>
#include <IO/HTTPHeaderEntries.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/SourceWithKeyCondition.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Storages/Cache/SchemaCache.h>
#include <Storages/IStorage.h>
#include <Storages/StorageConfiguration.h>
#include <Storages/StorageFactory.h>
#include <Storages/prepareReadingFromFormat.h>
#include <Poco/URI.h>


namespace DB
{

class IOutputFormat;
using OutputFormatPtr = std::shared_ptr<IOutputFormat>;

class IInputFormat;
struct ConnectionTimeouts;
class NamedCollection;
class PullingPipelineExecutor;

/**
 * This class represents table engine for external urls.
 * It sends HTTP GET to server when select is called and
 * HTTP POST when insert is called. In POST request the data is send
 * using Chunked transfer encoding, so server have to support it.
 */
class IStorageURLBase : public IStorage
{
public:
    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr context, bool async_insert) override;

    bool supportsPartitionBy() const override { return true; }

    static ColumnsDescription getTableStructureFromData(
        const String & format,
        const String & uri,
        CompressionMethod compression_method,
        const HTTPHeaderEntries & headers,
        const std::optional<FormatSettings> & format_settings,
        const ContextPtr & context);

    static std::pair<ColumnsDescription, String> getTableStructureAndFormatFromData(
        const String & uri,
        CompressionMethod compression_method,
        const HTTPHeaderEntries & headers,
        const std::optional<FormatSettings> & format_settings,
        const ContextPtr & context);


    static SchemaCache & getSchemaCache(const ContextPtr & context);

    static std::optional<time_t> tryGetLastModificationTime(
        const String & url,
        const HTTPHeaderEntries & headers,
        const Poco::Net::HTTPBasicCredentials & credentials,
        const ContextPtr & context);

protected:
    friend class ReadFromURL;

    IStorageURLBase(
        const String & uri_,
        const ContextPtr & context_,
        const StorageID & id_,
        const String & format_name_,
        const std::optional<FormatSettings> & format_settings_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment,
        const String & compression_method_,
        const HTTPHeaderEntries & headers_ = {},
        const String & method_ = "",
        ASTPtr partition_by = nullptr,
        bool distributed_processing_ = false);

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
    bool distributed_processing;

    virtual std::string getReadMethod() const;

    virtual std::vector<std::pair<std::string, std::string>> getReadURIParams(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        const SelectQueryInfo & query_info,
        const ContextPtr & context,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size) const;

    virtual std::function<void(std::ostream &)> getReadPOSTDataCallback(
        const Names & column_names,
        const ColumnsDescription & columns_description,
        const SelectQueryInfo & query_info,
        const ContextPtr & context,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size) const;

    virtual bool supportsSubsetOfColumns(const ContextPtr & context) const;

    bool prefersLargeBlocks() const override;

    bool parallelizeOutputAfterReading(ContextPtr context) const override;

    bool supportsTrivialCountOptimization(const StorageSnapshotPtr &, ContextPtr) const override { return true; }

private:
    static std::pair<ColumnsDescription, String> getTableStructureAndFormatFromDataImpl(
        std::optional<String> format,
        const String & uri,
        CompressionMethod compression_method,
        const HTTPHeaderEntries & headers,
        const std::optional<FormatSettings> & format_settings,
        const ContextPtr & context);

    virtual Block getHeaderBlock(const Names & column_names, const StorageSnapshotPtr & storage_snapshot) const = 0;
};

bool urlWithGlobs(const String & uri);

String getSampleURI(String uri, ContextPtr context);

class StorageURLSource : public SourceWithKeyCondition, WithContext
{
    using URIParams = std::vector<std::pair<String, String>>;

public:
    class DisclosedGlobIterator
    {
    public:
        DisclosedGlobIterator(const String & uri_, size_t max_addresses, const ActionsDAG::Node * predicate, const NamesAndTypesList & virtual_columns, const ContextPtr & context);

        String next();
        size_t size();
    private:
        class Impl;
        /// shared_ptr to have copy constructor
        std::shared_ptr<Impl> pimpl;
    };

    using FailoverOptions = std::vector<String>;
    using IteratorWrapper = std::function<FailoverOptions()>;

    StorageURLSource(
        const ReadFromFormatInfo & info,
        std::shared_ptr<IteratorWrapper> uri_iterator_,
        const std::string & http_method,
        std::function<void(std::ostream &)> callback,
        const String & format,
        const std::optional<FormatSettings> & format_settings,
        String name_,
        const ContextPtr & context,
        UInt64 max_block_size,
        const ConnectionTimeouts & timeouts,
        CompressionMethod compression_method,
        size_t max_parsing_threads,
        const HTTPHeaderEntries & headers_ = {},
        const URIParams & params = {},
        bool glob_url = false,
        bool need_only_count_ = false);

    ~StorageURLSource() override;

    String getName() const override { return name; }

    void setKeyCondition(const std::optional<ActionsDAG> & filter_actions_dag, ContextPtr context_) override
    {
        setKeyConditionImpl(filter_actions_dag, context_, block_for_format);
    }

    Chunk generate() override;

    static void setCredentials(Poco::Net::HTTPBasicCredentials & credentials, const Poco::URI & request_uri);

    static std::pair<Poco::URI, std::unique_ptr<ReadWriteBufferFromHTTP>> getFirstAvailableURIAndReadBuffer(
        std::vector<String>::const_iterator & option,
        const std::vector<String>::const_iterator & end,
        ContextPtr context,
        const URIParams & params,
        const String & http_method,
        std::function<void(std::ostream &)> callback,
        const ConnectionTimeouts & timeouts,
        Poco::Net::HTTPBasicCredentials & credentials,
        const HTTPHeaderEntries & headers,
        bool glob_url,
        bool delay_initialization);

private:
    void addNumRowsToCache(const String & uri, size_t num_rows);
    std::optional<size_t> tryGetNumRowsFromCache(const String & uri, std::optional<time_t> last_mod_time);

    using InitializeFunc = std::function<bool()>;
    InitializeFunc initialize;

    String name;
    ColumnsDescription columns_description;
    NamesAndTypesList requested_columns;
    bool need_headers_virtual_column;
    NamesAndTypesList requested_virtual_columns;
    Block block_for_format;
    std::shared_ptr<IteratorWrapper> uri_iterator;
    Poco::URI curr_uri;
    std::optional<size_t> current_file_size;
    String format;
    const std::optional<FormatSettings> & format_settings;
    HTTPHeaderEntries headers;
    bool need_only_count;
    size_t total_rows_in_file = 0;

    Poco::Net::HTTPBasicCredentials credentials;

    Map http_response_headers;
    bool http_response_headers_initialized = false;

    std::unique_ptr<ReadBuffer> read_buf;
    std::shared_ptr<IInputFormat> input_format;
    std::unique_ptr<QueryPipeline> pipeline;
    std::unique_ptr<PullingPipelineExecutor> reader;
};

class StorageURLSink : public SinkToStorage
{
public:
    StorageURLSink(
        String uri_,
        String format_,
        const std::optional<FormatSettings> & format_settings,
        const Block & sample_block,
        const ContextPtr & context,
        const ConnectionTimeouts & timeouts,
        const CompressionMethod & compression_method,
        HTTPHeaderEntries headers = {},
        String method = Poco::Net::HTTPRequest::HTTP_POST);

    ~StorageURLSink() override;

    std::string getName() const override { return "StorageURLSink"; }
    void consume(Chunk & chunk) override;
    void onFinish() override;

private:
    void finalizeBuffers();
    void releaseBuffers();
    void cancelBuffers();
    void initBuffers();

    String uri;
    String format;
    std::optional<FormatSettings> format_settings;
    ContextPtr context;
    ConnectionTimeouts timeouts;
    CompressionMethod compression_method;
    HTTPHeaderEntries headers;
    String http_method;

    std::unique_ptr<WriteBuffer> write_buf;
    OutputFormatPtr writer;
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
        const ContextPtr & context_,
        const String & compression_method_,
        const HTTPHeaderEntries & headers_ = {},
        const String & method_ = "",
        ASTPtr partition_by_ = nullptr,
        bool distributed_processing_ = false);

    String getName() const override
    {
        return "URL";
    }

    Block getHeaderBlock(const Names & /*column_names*/, const StorageSnapshotPtr & storage_snapshot) const override
    {
        return storage_snapshot->metadata->getSampleBlock();
    }

    bool supportsSubcolumns() const override { return true; }
    bool supportsOptimizationToSubcolumns() const override { return false; }

    bool supportsDynamicSubcolumns() const override { return true; }

    void addInferredEngineArgsToCreateQuery(ASTs & args, const ContextPtr & context) const override;

    static FormatSettings getFormatSettingsFromArgs(const StorageFactory::Arguments & args);

    struct Configuration : public StatelessTableEngineConfiguration
    {
        std::string url;
        std::string http_method;
        HTTPHeaderEntries headers;
        std::string addresses_expr;
    };

    static Configuration getConfiguration(ASTs & args, const ContextPtr & context);

    /// Does evaluateConstantExpressionOrIdentifierAsLiteral() on all arguments.
    /// If `headers(...)` argument is present, parses it and moves it to the end of the array.
    /// Returns number of arguments excluding `headers(...)`.
    static size_t evalArgsAndCollectHeaders(ASTs & url_function_args, HTTPHeaderEntries & header_entries, const ContextPtr & context, bool evaluate_arguments = true);

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
        const ContextPtr & context_,
        const String & compression_method_);

    void read(
        QueryPlan & query_plan,
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
