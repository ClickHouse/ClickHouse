#pragma once
#include <Interpreters/Context_fwd.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Storages/ObjectStorage/Settings.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Formats/ReadSchemaUtils.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_EXTRACT_TABLE_STRUCTURE;

}

template <typename StorageSettings>
class ReadBufferIterator : public IReadBufferIterator, WithContext
{
public:
    using Storage = StorageObjectStorage<StorageSettings>;
    using Source = StorageObjectStorageSource<StorageSettings>;
    using FileIterator = std::shared_ptr<typename Source::IIterator>;
    using ObjectInfos = typename Storage::ObjectInfos;

    ReadBufferIterator(
        ObjectStoragePtr object_storage_,
        Storage::ConfigurationPtr configuration_,
        const FileIterator & file_iterator_,
        const std::optional<FormatSettings> & format_settings_,
        ObjectInfos & read_keys_,
        const ContextPtr & context_)
        : WithContext(context_)
        , object_storage(object_storage_)
        , configuration(configuration_)
        , file_iterator(file_iterator_)
        , format_settings(format_settings_)
        , storage_settings(StorageSettings::create(context_->getSettingsRef()))
        , read_keys(read_keys_)
        , prev_read_keys_size(read_keys_.size())
    {
    }

    std::pair<std::unique_ptr<ReadBuffer>, std::optional<ColumnsDescription>> next() override
    {
        /// For default mode check cached columns for currently read keys on first iteration.
        if (first && storage_settings.schema_inference_mode == SchemaInferenceMode::DEFAULT)
        {
            if (auto cached_columns = tryGetColumnsFromCache(read_keys.begin(), read_keys.end()))
                return {nullptr, cached_columns};
        }

        current_object_info = file_iterator->next(0);
        if (current_object_info->relative_path.empty())
        {
            if (first)
            {
                throw Exception(
                    ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE,
                    "Cannot extract table structure from {} format file, "
                    "because there are no files with provided path. "
                    "You must specify table structure manually",
                    configuration->format);
            }
            return {nullptr, std::nullopt};
        }

        first = false;

        /// File iterator could get new keys after new iteration,
        /// check them in schema cache if schema inference mode is default.
        if (getContext()->getSettingsRef().schema_inference_mode == SchemaInferenceMode::DEFAULT
            && read_keys.size() > prev_read_keys_size)
        {
            auto columns_from_cache = tryGetColumnsFromCache(read_keys.begin() + prev_read_keys_size, read_keys.end());
            prev_read_keys_size = read_keys.size();
            if (columns_from_cache)
                return {nullptr, columns_from_cache};
        }
        else if (getContext()->getSettingsRef().schema_inference_mode == SchemaInferenceMode::UNION)
        {
            ObjectInfos paths = {current_object_info};
            if (auto columns_from_cache = tryGetColumnsFromCache(paths.begin(), paths.end()))
                return {nullptr, columns_from_cache};
        }

        first = false;

        std::unique_ptr<ReadBuffer> read_buffer = object_storage->readObject(
            StoredObject(current_object_info->relative_path),
            getContext()->getReadSettings(),
            {},
            current_object_info->metadata.size_bytes);

        read_buffer = wrapReadBufferWithCompressionMethod(
            std::move(read_buffer),
            chooseCompressionMethod(current_object_info->relative_path, configuration->compression_method),
            static_cast<int>(getContext()->getSettingsRef().zstd_window_log_max));

        return {std::move(read_buffer), std::nullopt};
    }

    void setNumRowsToLastFile(size_t num_rows) override
    {
        if (storage_settings.schema_inference_use_cache)
        {
            Storage::getSchemaCache(getContext()).addNumRows(
                getKeyForSchemaCache(current_object_info->relative_path), num_rows);
        }
    }

    void setSchemaToLastFile(const ColumnsDescription & columns) override
    {
        if (storage_settings.schema_inference_use_cache
            && storage_settings.schema_inference_mode == SchemaInferenceMode::UNION)
        {
            Storage::getSchemaCache(getContext()).addColumns(
                getKeyForSchemaCache(current_object_info->relative_path), columns);
        }
    }

    void setResultingSchema(const ColumnsDescription & columns) override
    {
        if (storage_settings.schema_inference_use_cache
            && storage_settings.schema_inference_mode == SchemaInferenceMode::DEFAULT)
        {
            Storage::getSchemaCache(getContext()).addManyColumns(getPathsForSchemaCache(), columns);
        }
    }

    String getLastFileName() const override { return current_object_info->relative_path; }

private:
    SchemaCache::Key getKeyForSchemaCache(const String & path) const
    {
        auto source = fs::path(configuration->getDataSourceDescription()) / path;
        return DB::getKeyForSchemaCache(source, configuration->format, format_settings, getContext());
    }

    SchemaCache::Keys getPathsForSchemaCache() const
    {
        Strings sources;
        sources.reserve(read_keys.size());
        std::transform(
            read_keys.begin(), read_keys.end(),
            std::back_inserter(sources),
            [&](const auto & elem)
            {
                return fs::path(configuration->getDataSourceDescription()) / elem->relative_path;
            });
        return DB::getKeysForSchemaCache(sources, configuration->format, format_settings, getContext());
    }

    std::optional<ColumnsDescription> tryGetColumnsFromCache(
        const ObjectInfos::iterator & begin,
        const ObjectInfos::iterator & end)
    {
        if (!storage_settings.schema_inference_use_cache)
            return std::nullopt;

        auto & schema_cache = Storage::getSchemaCache(getContext());
        for (auto it = begin; it < end; ++it)
        {
            const auto & object_info = (*it);
            auto get_last_mod_time = [&] -> std::optional<time_t>
            {
                if (object_info->metadata.last_modified)
                    return object_info->metadata.last_modified->epochMicroseconds();
                else
                {
                    object_info->metadata = object_storage->getObjectMetadata(object_info->relative_path);
                    return object_info->metadata.last_modified->epochMicroseconds();
                }
            };

            auto cache_key = getKeyForSchemaCache(object_info->relative_path);
            auto columns = schema_cache.tryGetColumns(cache_key, get_last_mod_time);
            if (columns)
                return columns;
        }

        return std::nullopt;
    }

    ObjectStoragePtr object_storage;
    const Storage::ConfigurationPtr configuration;
    const FileIterator file_iterator;
    const std::optional<FormatSettings> & format_settings;
    const StorageObjectStorageSettings storage_settings;
    ObjectInfos & read_keys;

    size_t prev_read_keys_size;
    Storage::ObjectInfoPtr current_object_info;
    bool first = true;
};
}
