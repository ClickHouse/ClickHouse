#pragma once
#include <Interpreters/Context_fwd.h>
#include <Storages/ObjectStorage/StorageObjectStorage_fwd_internal.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
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

    std::pair<std::unique_ptr<ReadBuffer>, std::optional<ColumnsDescription>> next() override;

    void setNumRowsToLastFile(size_t num_rows) override;

    void setSchemaToLastFile(const ColumnsDescription & columns) override;

    void setResultingSchema(const ColumnsDescription & columns) override;

    String getLastFileName() const override;

private:
    SchemaCache::Key getKeyForSchemaCache(const String & path) const;
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

    size_t prev_read_keys_size;
    ObjectInfoPtr current_object_info;
    bool first = true;
};
}
