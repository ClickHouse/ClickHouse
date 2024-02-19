#pragma once
#include <Interpreters/Context_fwd.h>
#include <Storages/ObjectStorage/StorageObjectStorage_fwd_internal.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Storages/ObjectStorage/StorageObjectStorageQuerySettings.h>
#include <Formats/ReadSchemaUtils.h>


namespace DB
{

class ReadBufferIterator : public IReadBufferIterator, WithContext
{
public:
    using FileIterator = std::shared_ptr<StorageObjectStorageSource::IIterator>;

    ReadBufferIterator(
        ObjectStoragePtr object_storage_,
        ConfigurationPtr configuration_,
        const FileIterator & file_iterator_,
        const std::optional<FormatSettings> & format_settings_,
        const StorageObjectStorageSettings & query_settings_,
        SchemaCache & schema_cache_,
        ObjectInfos & read_keys_,
        const ContextPtr & context_);

    Data next() override;

    void setNumRowsToLastFile(size_t num_rows) override;

    void setSchemaToLastFile(const ColumnsDescription & columns) override;

    void setResultingSchema(const ColumnsDescription & columns) override;

    String getLastFileName() const override;

    void setFormatName(const String & format_name) override;

    bool supportsLastReadBufferRecreation() const override { return true; }

    std::unique_ptr<ReadBuffer> recreateLastReadBuffer() override;

private:
    SchemaCache::Key getKeyForSchemaCache(const String & path, const String & format_name) const;
    SchemaCache::Keys getPathsForSchemaCache() const;
    std::optional<ColumnsDescription> tryGetColumnsFromCache(
        const ObjectInfos::iterator & begin, const ObjectInfos::iterator & end);

    ObjectStoragePtr object_storage;
    const ConfigurationPtr configuration;
    const FileIterator file_iterator;
    const std::optional<FormatSettings> & format_settings;
    const StorageObjectStorageSettings query_settings;
    SchemaCache & schema_cache;
    ObjectInfos & read_keys;
    std::optional<String> format;

    size_t prev_read_keys_size;
    ObjectInfoPtr current_object_info;
    bool first = true;
};
}
