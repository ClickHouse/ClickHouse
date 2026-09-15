#include <Storages/ObjectStorage/MultiFileStorageObjectStorageSink.h>
#include <Interpreters/Context.h>
#include <Common/logger_useful.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadHelpers.h>
#include <filesystem>

namespace DB
{

namespace ErrorCodes
{
    extern const int FILE_ALREADY_EXISTS;
}

namespace
{
    /// The commit file lives in the same directory as the data files.
    std::string commitFilePath(const std::string & base_path, const String & transaction_id)
    {
        return (std::filesystem::path(base_path).parent_path() / ("commit_" + transaction_id)).string();
    }
}

MultiFileStorageObjectStorageSink::MultiFileStorageObjectStorageSink(
    const std::string & base_path_,
    const String & transaction_id_,
    ObjectStoragePtr object_storage_,
    StorageObjectStorageConfigurationPtr configuration_,
    std::size_t max_bytes_per_file_,
    std::size_t max_rows_per_file_,
    bool overwrite_existing_data_files_,
    const std::function<void(const std::string &)> & new_file_path_callback_,
    const std::optional<FormatSettings> & format_settings_,
    SharedHeader sample_block_,
    ContextPtr context_)
    : SinkToStorage(sample_block_),
    base_path(base_path_),
    commit_file_path(commitFilePath(base_path_, transaction_id_)),
    object_storage(object_storage_),
    configuration(configuration_),
    max_bytes_per_file(max_bytes_per_file_),
    max_rows_per_file(max_rows_per_file_),
    overwrite_existing_data_files(overwrite_existing_data_files_),
    new_file_path_callback(new_file_path_callback_),
    format_settings(format_settings_),
    sample_block(sample_block_),
    context(context_)
{
}

MultiFileStorageObjectStorageSink::~MultiFileStorageObjectStorageSink()
{
    if (isCancelled() && current_sink)
        current_sink->cancel();
}

/// Adds a counter that represents file index to the file path.
/// Example:
/// Input is  `table_root/year=2025/month=12/day=12/file.parquet`
/// Output is `table_root/year=2025/month=12/day=12/file.1.parquet`
std::string MultiFileStorageObjectStorageSink::generateNewFilePath()
{
    const auto file_format = Poco::toLower(configuration->getFormat());
    const auto index_string = std::to_string(file_paths.size() + 1);
    std::size_t pos = base_path.rfind(file_format);

    /// normal case - path ends with the file format
    if (pos != std::string::npos)
    {
        const auto path_without_extension = base_path.substr(0, pos);
        const auto file_format_extension = "." + file_format;

        return path_without_extension + index_string + file_format_extension;
    }

    /// if no extension is found, just append the index - I am not even sure this is possible
    return base_path + "." + index_string;
}

std::shared_ptr<StorageObjectStorageSink> MultiFileStorageObjectStorageSink::createNewSink()
{
    auto new_path = generateNewFilePath();

    /// The callback runs before the conflict check on purpose: the caller discards the reported
    /// path along with the failure, and when rewriting is allowed this check is off anyway.
    new_file_path_callback(new_path);

    file_paths.emplace_back(std::move(new_path));

    if (!overwrite_existing_data_files && object_storage->exists(StoredObject(file_paths.back())))
    {
        throw Exception(ErrorCodes::FILE_ALREADY_EXISTS, "File {} already exists", file_paths.back());
    }

    return std::make_shared<StorageObjectStorageSink>(
        file_paths.back(),
        object_storage,
        format_settings,
        sample_block,
        context,
        configuration->getFormat(),
        configuration->getCompressionMethod());
}

void MultiFileStorageObjectStorageSink::consume(Chunk & chunk)
{
    if (isCancelled())
    {
        if (current_sink)
            current_sink->cancel();
        return;
    }

    if (!current_sink)
    {
        current_sink = createNewSink();
    }
    else
    {
        const auto written_bytes = current_sink->getWrittenBytes();

        const bool exceeded_bytes_limit = max_bytes_per_file && written_bytes >= max_bytes_per_file;
        const bool exceeded_rows_limit = max_rows_per_file && current_sink_written_rows >= max_rows_per_file;

        if (exceeded_bytes_limit || exceeded_rows_limit)
        {
            current_sink->onFinish();
            current_sink = createNewSink();
            current_sink_written_rows = 0;
        }
    }

    current_sink->consume(chunk);
    current_sink_written_rows += chunk.getNumRows();
}

void MultiFileStorageObjectStorageSink::onFinish()
{
    if (!current_sink)
        return;

    current_sink->onFinish();
    commit();
}

std::optional<std::vector<std::string>> MultiFileStorageObjectStorageSink::tryReadCommittedPaths(
    const std::string & base_path_,
    const String & transaction_id_,
    const ObjectStoragePtr & object_storage_,
    const ContextPtr & context_)
{
    const auto path = commitFilePath(base_path_, transaction_id_);

    if (!object_storage_->exists(StoredObject(path)))
        return {};

    auto in = object_storage_->readObject(StoredObject(path), context_->getReadSettings());

    std::vector<std::string> committed_paths;
    while (!in->eof())
    {
        String committed_path;
        readStringUntilNewlineInto(committed_path, *in);
        in->tryIgnore(1);
        if (!committed_path.empty())
            committed_paths.emplace_back(std::move(committed_path));
    }

    return committed_paths;
}

void MultiFileStorageObjectStorageSink::commit()
{
    auto out = object_storage->writeObject(
        StoredObject(commit_file_path), 
        WriteMode::Rewrite, /* attributes= */
        {}, DBMS_DEFAULT_BUFFER_SIZE,
        context->getWriteSettings());

    for (const auto & p : file_paths)
    {
        out->write(p.data(), p.size());
        out->write("\n", 1);
    }

    out->finalize();
}

}
