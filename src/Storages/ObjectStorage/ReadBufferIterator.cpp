#include <Storages/ObjectStorage/ReadBufferIterator.h>
#include <Storages/ObjectStorage/StorageObjectStorageQuerySettings.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <IO/ReadBufferFromFileBase.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_EXTRACT_TABLE_STRUCTURE;
    extern const int CANNOT_DETECT_FORMAT;

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
    , format(configuration->format.empty() || configuration->format == "auto" ? std::nullopt : std::optional<String>(configuration->format))
    , prev_read_keys_size(read_keys_.size())
{
}

SchemaCache::Key ReadBufferIterator::getKeyForSchemaCache(const String & path, const String & format_name) const
{
    auto source = fs::path(configuration->getDataSourceDescription()) / path;
    return DB::getKeyForSchemaCache(source, format_name, format_settings, getContext());
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
    return DB::getKeysForSchemaCache(sources, *format, format_settings, getContext());
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
                return object_info->metadata->last_modified.epochMicroseconds();
            else
            {
                object_info->metadata = object_storage->getObjectMetadata(object_info->relative_path);
                return object_info->metadata->last_modified.epochMicroseconds();
            }
        };

        chassert(object_info);
        if (format)
        {
            auto cache_key = getKeyForSchemaCache(object_info->relative_path, *format);
            if (auto columns = schema_cache.tryGetColumns(cache_key, get_last_mod_time))
                return columns;
        }
        else
        {
            /// If format is unknown, we can iterate through all possible input formats
            /// and check if we have an entry with this format and this file in schema cache.
            /// If we have such entry for some format, we can use this format to read the file.
            for (const auto & format_name : FormatFactory::instance().getAllInputFormats())
            {
                auto cache_key = getKeyForSchemaCache(object_info->relative_path, format_name);
                if (auto columns = schema_cache.tryGetColumns(cache_key, get_last_mod_time))
                {
                    /// Now format is known. It should be the same for all files.
                    format = format_name;
                    return columns;
                }
            }
        }
    }

    return std::nullopt;
}

void ReadBufferIterator::setNumRowsToLastFile(size_t num_rows)
{
    chassert(current_object_info);
    if (query_settings.schema_inference_use_cache)
        schema_cache.addNumRows(getKeyForSchemaCache(current_object_info->relative_path, *format), num_rows);
}

void ReadBufferIterator::setSchemaToLastFile(const ColumnsDescription & columns)
{
    chassert(current_object_info);
    if (query_settings.schema_inference_use_cache
        && query_settings.schema_inference_mode == SchemaInferenceMode::UNION)
    {
        schema_cache.addColumns(getKeyForSchemaCache(current_object_info->relative_path, *format), columns);
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

void ReadBufferIterator::setFormatName(const String & format_name)
{
    format = format_name;
}

String ReadBufferIterator::getLastFileName() const
{
    if (current_object_info)
        return current_object_info->relative_path;
    else
        return "";
}

std::unique_ptr<ReadBuffer> ReadBufferIterator::recreateLastReadBuffer()
{
    chassert(current_object_info);

    auto impl = object_storage->readObject(
        StoredObject(current_object_info->relative_path), getContext()->getReadSettings());

    int zstd_window_log_max = static_cast<int>(getContext()->getSettingsRef().zstd_window_log_max);
    return wrapReadBufferWithCompressionMethod(
        std::move(impl), chooseCompressionMethod(current_object_info->relative_path, configuration->compression_method),
        zstd_window_log_max);
}

ReadBufferIterator::Data ReadBufferIterator::next()
{
    if (first)
    {
        /// If format is unknown we iterate through all currently read keys on first iteration and
        /// try to determine format by file name.
        if (!format)
        {
            for (const auto & object_info : read_keys)
            {
                if (auto format_from_file_name = FormatFactory::instance().tryGetFormatFromFileName(object_info->relative_path))
                {
                    format = format_from_file_name;
                    break;
                }
            }
        }

        /// For default mode check cached columns for currently read keys on first iteration.
        if (first && getContext()->getSettingsRef().schema_inference_mode == SchemaInferenceMode::DEFAULT)
        {
            if (auto cached_columns = tryGetColumnsFromCache(read_keys.begin(), read_keys.end()))
                return {nullptr, cached_columns, format};
        }
    }

    while (true)
    {
        current_object_info = file_iterator->next(0);

        if (!current_object_info || current_object_info->relative_path.empty())
        {
            if (first)
            {
                if (format)
                    throw Exception(
                        ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE,
                        "The table structure cannot be extracted from a {} format file, because there are no files with provided path "
                        "in {} or all files are empty. You can specify table structure manually",
                        *format, object_storage->getName());

                throw Exception(
                    ErrorCodes::CANNOT_DETECT_FORMAT,
                    "The data format cannot be detected by the contents of the files, because there are no files with provided path "
                    "in {} or all files are empty. You can specify the format manually", object_storage->getName());
            }

            return {nullptr, std::nullopt, format};
        }

        /// file iterator could get new keys after new iteration
        if (read_keys.size() > prev_read_keys_size)
        {
            /// If format is unknown we can try to determine it by new file names.
            if (!format)
            {
                for (auto it = read_keys.begin() + prev_read_keys_size; it != read_keys.end(); ++it)
                {
                    if (auto format_from_file_name = FormatFactory::instance().tryGetFormatFromFileName((*it)->relative_path))
                    {
                        format = format_from_file_name;
                        break;
                    }
                }
            }

            /// Check new files in schema cache if schema inference mode is default.
            if (getContext()->getSettingsRef().schema_inference_mode == SchemaInferenceMode::DEFAULT)
            {
                auto columns_from_cache = tryGetColumnsFromCache(read_keys.begin() + prev_read_keys_size, read_keys.end());
                if (columns_from_cache)
                    return {nullptr, columns_from_cache, format};
            }

            prev_read_keys_size = read_keys.size();
        }

        if (query_settings.skip_empty_files
            && current_object_info->metadata && current_object_info->metadata->size_bytes == 0)
            continue;

        /// In union mode, check cached columns only for current key.
        if (getContext()->getSettingsRef().schema_inference_mode == SchemaInferenceMode::UNION)
        {
            ObjectInfos objects{current_object_info};
            if (auto columns_from_cache = tryGetColumnsFromCache(objects.begin(), objects.end()))
            {
                first = false;
                return {nullptr, columns_from_cache, format};
            }
        }

        std::unique_ptr<ReadBuffer> read_buffer = object_storage->readObject(
            StoredObject(current_object_info->relative_path),
            getContext()->getReadSettings(),
            {},
            current_object_info->metadata->size_bytes);

        if (!query_settings.skip_empty_files || !read_buffer->eof())
        {
            first = false;

            read_buffer = wrapReadBufferWithCompressionMethod(
                std::move(read_buffer),
                chooseCompressionMethod(current_object_info->relative_path, configuration->compression_method),
                static_cast<int>(getContext()->getSettingsRef().zstd_window_log_max));

            return {std::move(read_buffer), std::nullopt, format};
        }
    }
}
}
