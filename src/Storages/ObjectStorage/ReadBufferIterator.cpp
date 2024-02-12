#include <Storages/ObjectStorage/ReadBufferIterator.h>
#include <Storages/ObjectStorage/StorageObjectStorageQuerySettings.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <IO/ReadBufferFromFileBase.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_EXTRACT_TABLE_STRUCTURE;

}

ReadBufferIterator::ReadBufferIterator(
    ObjectStoragePtr object_storage_,
    ConfigurationPtr configuration_,
    const FileIterator & file_iterator_,
    const std::optional<FormatSettings> & format_settings_,
    const StorageObjectStorageSettings & query_settings_,
    SchemaCache & schema_cache_,
    ObjectInfos & read_keys_,
    const ContextPtr & context_)
    : WithContext(context_)
    , object_storage(object_storage_)
    , configuration(configuration_)
    , file_iterator(file_iterator_)
    , format_settings(format_settings_)
    , query_settings(query_settings_)
    , schema_cache(schema_cache_)
    , read_keys(read_keys_)
    , prev_read_keys_size(read_keys_.size())
{
}

SchemaCache::Key ReadBufferIterator::getKeyForSchemaCache(const String & path) const
{
    auto source = fs::path(configuration->getDataSourceDescription()) / path;
    return DB::getKeyForSchemaCache(source, configuration->format, format_settings, getContext());
}

SchemaCache::Keys ReadBufferIterator::getPathsForSchemaCache() const
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

std::optional<ColumnsDescription> ReadBufferIterator::tryGetColumnsFromCache(
    const ObjectInfos::iterator & begin,
    const ObjectInfos::iterator & end)
{
    if (!query_settings.schema_inference_use_cache)
        return std::nullopt;

    for (auto it = begin; it < end; ++it)
    {
        const auto & object_info = (*it);
        auto get_last_mod_time = [&] -> std::optional<time_t>
        {
            if (object_info->metadata)
                return object_info->metadata->last_modified->epochMicroseconds();
            else
            {
                object_info->metadata = object_storage->getObjectMetadata(object_info->relative_path);
                return object_info->metadata->last_modified->epochMicroseconds();
            }
        };

        auto cache_key = getKeyForSchemaCache(object_info->relative_path);
        auto columns = schema_cache.tryGetColumns(cache_key, get_last_mod_time);
        if (columns)
            return columns;
    }

    return std::nullopt;
}

void ReadBufferIterator::setNumRowsToLastFile(size_t num_rows)
{
    if (query_settings.schema_inference_use_cache)
        schema_cache.addNumRows(getKeyForSchemaCache(current_object_info->relative_path), num_rows);
}

void ReadBufferIterator::setSchemaToLastFile(const ColumnsDescription & columns)
{
    if (query_settings.schema_inference_use_cache
        && query_settings.schema_inference_mode == SchemaInferenceMode::UNION)
    {
        schema_cache.addColumns(getKeyForSchemaCache(current_object_info->relative_path), columns);
    }
}

void ReadBufferIterator::setResultingSchema(const ColumnsDescription & columns)
{
    if (query_settings.schema_inference_use_cache
        && query_settings.schema_inference_mode == SchemaInferenceMode::DEFAULT)
    {
        schema_cache.addManyColumns(getPathsForSchemaCache(), columns);
    }
}

String ReadBufferIterator::getLastFileName() const
{
    if (current_object_info)
        return current_object_info->relative_path;
    else
        return "";
}

std::pair<std::unique_ptr<ReadBuffer>, std::optional<ColumnsDescription>> ReadBufferIterator::next()
{
    /// For default mode check cached columns for currently read keys on first iteration.
    if (first && query_settings.schema_inference_mode == SchemaInferenceMode::DEFAULT)
    {
        if (auto cached_columns = tryGetColumnsFromCache(read_keys.begin(), read_keys.end()))
            return {nullptr, cached_columns};
    }

    current_object_info = file_iterator->next(0);
    if (!current_object_info || current_object_info->relative_path.empty())
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

    chassert(current_object_info->metadata);
    std::unique_ptr<ReadBuffer> read_buffer = object_storage->readObject(
        StoredObject(current_object_info->relative_path),
        getContext()->getReadSettings(),
        {},
        current_object_info->metadata->size_bytes);

    read_buffer = wrapReadBufferWithCompressionMethod(
        std::move(read_buffer),
        chooseCompressionMethod(current_object_info->relative_path, configuration->compression_method),
        static_cast<int>(getContext()->getSettingsRef().zstd_window_log_max));

    return {std::move(read_buffer), std::nullopt};
}

}
