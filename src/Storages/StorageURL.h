#pragma once

#include <Storages/IStorage.h>
#include <Poco/URI.h>
#include <common/shared_ptr_helper.h>
#include <DataStreams/IBlockOutputStream.h>
#include <Formats/FormatSettings.h>
#include <IO/CompressionMethod.h>
#include <Storages/StorageFactory.h>


namespace DB
{

struct ConnectionTimeouts;

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
        const StorageMetadataPtr & /*metadata_snapshot*/,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    BlockOutputStreamPtr write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr context) override;

protected:
    IStorageURLBase(
        const Poco::URI & uri_,
        ContextPtr context_,
        const StorageID & id_,
        const String & format_name_,
        const std::optional<FormatSettings> & format_settings_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment,
        const String & compression_method_);

    Poco::URI uri;
    String compression_method;
    String format_name;
    // For URL engine, we use format settings from server context + `SETTINGS`
    // clause of the `CREATE` query. In this case, format_settings is set.
    // For `url` table function, we use settings from current query context.
    // In this case, format_settings is not set.
    std::optional<FormatSettings> format_settings;

    virtual std::string getReadMethod() const;

    virtual std::vector<std::pair<std::string, std::string>> getReadURIParams(
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        const SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size) const;

    virtual std::function<void(std::ostream &)> getReadPOSTDataCallback(
        const Names & column_names,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        const SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size) const;

private:
    virtual Block getHeaderBlock(const Names & column_names, const StorageMetadataPtr & metadata_snapshot) const = 0;
};

class StorageURLBlockOutputStream : public IBlockOutputStream
{
public:
    StorageURLBlockOutputStream(
        const Poco::URI & uri,
        const String & format,
        const std::optional<FormatSettings> & format_settings,
        const Block & sample_block_,
        ContextPtr context,
        const ConnectionTimeouts & timeouts,
        CompressionMethod compression_method);

    Block getHeader() const override
    {
        return sample_block;
    }

    void write(const Block & block) override;
    void writePrefix() override;
    void writeSuffix() override;

private:
    Block sample_block;
    std::unique_ptr<WriteBuffer> write_buf;
    BlockOutputStreamPtr writer;
};

class StorageURL : public shared_ptr_helper<StorageURL>, public IStorageURLBase
{
    friend struct shared_ptr_helper<StorageURL>;
public:
    StorageURL(
        const Poco::URI & uri_,
        const StorageID & table_id_,
        const String & format_name_,
        const std::optional<FormatSettings> & format_settings_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment,
        ContextPtr context_,
        const String & compression_method_);

    String getName() const override
    {
        return "URL";
    }

    Block getHeaderBlock(const Names & /*column_names*/, const StorageMetadataPtr & metadata_snapshot) const override
    {
        return metadata_snapshot->getSampleBlock();
    }

    static FormatSettings getFormatSettingsFromArgs(const StorageFactory::Arguments & args);
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
        const StorageMetadataPtr & /*metadata_snapshot*/,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

private:
    std::vector<Poco::URI> uri_options;
};
}
