#include <Storages/ObjectStorage/ReadBufferIterator.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Core/Settings.h>
#include <Formats/FormatFactory.h>
#include <IO/ReadBufferFromFileBase.h>


namespace DB
{
namespace Setting
{
    extern const SettingsSchemaInferenceMode schema_inference_mode;
    extern const SettingsInt64 zstd_window_log_max;
}

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
    SchemaCache & schema_cache_,
    ObjectInfos & read_keys_,
    const ContextPtr & context_)
    : WithContext(context_)
    , object_storage(object_storage_)
    , configuration(configuration_)
    , file_iterator(file_iterator_)
    , format_settings(format_settings_)
    , query_settings(configuration->getQuerySettings(context_))
    , schema_cache(schema_cache_)
    , read_keys(read_keys_)
    , prev_read_keys_size(read_keys_.size())
{
    if (configuration->format != "auto")
        format = configuration->format;
}

SchemaCache::Key ReadBufferIterator::getKeyForSchemaCache(const ObjectInfo & object_info, const String & format_name) const
{
    auto source = StorageObjectStorageSource::getUniqueStoragePathIdentifier(*configuration, object_info);
    return DB::getKeyForSchemaCache(source, format_name, format_settings, getContext());
}

SchemaCache::Keys ReadBufferIterator::getKeysForSchemaCache() const
{
    Strings sources;
    sources.reserve(read_keys.size());
    std::transform(
        read_keys.begin(), read_keys.end(),
        std::back_inserter(sources),
        [&](const auto & elem)
        {
            return StorageObjectStorageSource::getUniqueStoragePathIdentifier(*configuration, *elem);
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
            const auto & path = object_info->isArchive() ? object_info->getPathToArchive() : object_info->getPath();
            if (!object_info->metadata)
                object_info->metadata = object_storage->tryGetObjectMetadata(path);

            return object_info->metadata
                ? std::optional<time_t>(object_info->metadata->last_modified.epochTime())
                : std::nullopt;
        };

        if (format)
        {
            const auto cache_key = getKeyForSchemaCache(*object_info, *format);
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
                const auto cache_key = getKeyForSchemaCache(*object_info, format_name);
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
    if (query_settings.schema_inference_use_cache)
        schema_cache.addNumRows(getKeyForSchemaCache(*current_object_info, *format), num_rows);
}

void ReadBufferIterator::setSchemaToLastFile(const ColumnsDescription & columns)
{
    if (query_settings.schema_inference_use_cache
        && query_settings.schema_inference_mode == SchemaInferenceMode::UNION)
    {
        schema_cache.addColumns(getKeyForSchemaCache(*current_object_info, *format), columns);
    }
}

void ReadBufferIterator::setResultingSchema(const ColumnsDescription & columns)
{
    if (query_settings.schema_inference_use_cache
        && query_settings.schema_inference_mode == SchemaInferenceMode::DEFAULT)
    {
        schema_cache.addManyColumns(getKeysForSchemaCache(), columns);
    }
}

void ReadBufferIterator::setFormatName(const String & format_name)
{
    format = format_name;
}

String ReadBufferIterator::getLastFilePath() const
{
    if (current_object_info)
        return current_object_info->getPath();
    else
        return "";
}

std::unique_ptr<ReadBuffer> ReadBufferIterator::recreateLastReadBuffer()
{
    auto context = getContext();

    const auto & path = current_object_info->isArchive() ? current_object_info->getPathToArchive() : current_object_info->getPath();
    auto impl = object_storage->readObject(StoredObject(path), context->getReadSettings());

    const auto compression_method = chooseCompressionMethod(current_object_info->getFileName(), configuration->compression_method);
    const auto zstd_window = static_cast<int>(context->getSettingsRef()[Setting::zstd_window_log_max]);

    return wrapReadBufferWithCompressionMethod(std::move(impl), compression_method, zstd_window);
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
                auto format_from_file_name = FormatFactory::instance().tryGetFormatFromFileName(object_info->getFileName());
                /// Use this format only if we have a schema reader for it.
                if (format_from_file_name && FormatFactory::instance().checkIfFormatHasAnySchemaReader(*format_from_file_name))
                {
                    format = format_from_file_name;
                    break;
                }
            }
        }

        /// For default mode check cached columns for currently read keys on first iteration.
        if (first && getContext()->getSettingsRef()[Setting::schema_inference_mode] == SchemaInferenceMode::DEFAULT)
        {
            if (auto cached_columns = tryGetColumnsFromCache(read_keys.begin(), read_keys.end()))
            {
                return {nullptr, cached_columns, format};
            }
        }
    }

    while (true)
    {
        current_object_info = file_iterator->next(0);

        if (!current_object_info)
        {
            if (first)
            {
                if (format.has_value())
                {
                    throw Exception(
                        ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE,
                        "The table structure cannot be extracted from a {} format file, "
                        "because there are no files with provided path "
                        "in {} or all files are empty. You can specify table structure manually",
                        *format, object_storage->getName());
                }

                throw Exception(
                    ErrorCodes::CANNOT_DETECT_FORMAT,
                    "The data format cannot be detected by the contents of the files, "
                    "because there are no files with provided path "
                    "in {} or all files are empty. You can specify the format manually",
                    object_storage->getName());
            }

            return {nullptr, std::nullopt, format};
        }

        const auto filename = current_object_info->getFileName();
        chassert(!filename.empty());

        /// file iterator could get new keys after new iteration
        if (read_keys.size() > prev_read_keys_size)
        {
            /// If format is unknown we can try to determine it by new file names.
            if (!format)
            {
                for (auto it = read_keys.begin() + prev_read_keys_size; it != read_keys.end(); ++it)
                {
                    auto format_from_file_name = FormatFactory::instance().tryGetFormatFromFileName((*it)->getFileName());
                    /// Use this format only if we have a schema reader for it.
                    if (format_from_file_name && FormatFactory::instance().checkIfFormatHasAnySchemaReader(*format_from_file_name))
                    {
                        format = format_from_file_name;
                        break;
                    }
                }
            }

            /// Check new files in schema cache if schema inference mode is default.
            if (getContext()->getSettingsRef()[Setting::schema_inference_mode] == SchemaInferenceMode::DEFAULT)
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
        if (getContext()->getSettingsRef()[Setting::schema_inference_mode] == SchemaInferenceMode::UNION)
        {
            ObjectInfos objects{current_object_info};
            if (auto columns_from_cache = tryGetColumnsFromCache(objects.begin(), objects.end()))
            {
                first = false;
                return {nullptr, columns_from_cache, format};
            }
        }

        std::unique_ptr<ReadBuffer> read_buf;
        CompressionMethod compression_method;
        using ObjectInfoInArchive = StorageObjectStorageSource::ArchiveIterator::ObjectInfoInArchive;
        if (const auto * object_info_in_archive = dynamic_cast<const ObjectInfoInArchive *>(current_object_info.get()))
        {
            compression_method = chooseCompressionMethod(filename, configuration->compression_method);
            const auto & archive_reader = object_info_in_archive->archive_reader;
            read_buf = archive_reader->readFile(object_info_in_archive->path_in_archive, /*throw_on_not_found=*/true);
        }
        else
        {
            compression_method = chooseCompressionMethod(filename, configuration->compression_method);
            read_buf = object_storage->readObject(
                StoredObject(current_object_info->getPath()),
                getContext()->getReadSettings(),
                {},
                current_object_info->metadata->size_bytes);
        }

        if (!query_settings.skip_empty_files || !read_buf->eof())
        {
            first = false;

            read_buf = wrapReadBufferWithCompressionMethod(
                std::move(read_buf), compression_method, static_cast<int>(getContext()->getSettingsRef()[Setting::zstd_window_log_max]));

            return {std::move(read_buf), std::nullopt, format};
        }
    }
}
}
