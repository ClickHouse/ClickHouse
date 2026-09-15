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
    extern const int CORRUPTED_DATA;
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
    FileAlreadyExistsPolicy file_already_exists_policy_,
    const std::function<void(const std::string &)> & new_file_path_callback_,
    const std::optional<FormatSettings> & format_settings_,
    SharedHeader sample_block_,
    ContextPtr context_)
    : SinkToStorage(sample_block_),
    base_path(base_path_),
    transaction_id(transaction_id_),
    commit_file_path(commitFilePath(base_path_, transaction_id_)),
    object_storage(object_storage_),
    configuration(configuration_),
    max_bytes_per_file(max_bytes_per_file_),
    max_rows_per_file(max_rows_per_file_),
    file_already_exists_policy(file_already_exists_policy_),
    new_file_path_callback(new_file_path_callback_),
    format_settings(format_settings_),
    sample_block(sample_block_),
    context(context_)
{
    if (file_already_exists_policy != FileAlreadyExistsPolicy::overwrite)
    {
        if (auto committed_paths = tryReadCommittedPaths())
        {
            if (committed_paths->empty())
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "Commit file {} lists no data files", commit_file_path);

            /// Report the whole committed set before throwing: a caller applying `skip` takes these
            /// paths as the part's export result, so it needs every file and not just the first.
            for (const auto & committed_path : *committed_paths)
                new_file_path_callback(committed_path);

            throw Exception(ErrorCodes::FILE_ALREADY_EXISTS,
                "Part was already exported as {} file(s), see commit file {}",
                committed_paths->size(), commit_file_path);
        }
    }

    /// No commit file: either a fresh export, or an attempt that died before finalizing every
    /// file. `error` still reports the leftovers as a conflict, but `skip` has to rewrite them --
    /// the files that attempt never reached carry rows no later attempt produces.
    overwrite_data_files = file_already_exists_policy != FileAlreadyExistsPolicy::error;

    current_sink = createNewSink();
}

MultiFileStorageObjectStorageSink::~MultiFileStorageObjectStorageSink()
{
    if (isCancelled())
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

    /// The callback runs before the conflict check on purpose: under `error` the caller discards
    /// the reported path along with the failure, and under the other policies this check is off.
    new_file_path_callback(new_path);

    file_paths.emplace_back(std::move(new_path));

    if (!overwrite_data_files && object_storage->exists(StoredObject(file_paths.back())))
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
        current_sink->cancel();
        return;
    }

    const auto written_bytes = current_sink->getWrittenBytes();
    
    const bool exceeded_bytes_limit = max_bytes_per_file && written_bytes >= max_bytes_per_file;
    const bool exceeded_rows_limit = max_rows_per_file && current_sink_written_rows >= max_rows_per_file;

    if (exceeded_bytes_limit || exceeded_rows_limit)
    {
        current_sink->onFinish();
        current_sink = createNewSink();
        current_sink_written_rows = 0;
    }

    current_sink->consume(chunk);
    current_sink_written_rows += chunk.getNumRows();
}

void MultiFileStorageObjectStorageSink::onFinish()
{
    current_sink->onFinish();
    commit();
}

std::optional<std::vector<std::string>> MultiFileStorageObjectStorageSink::tryReadCommittedPaths() const
{
    if (!object_storage->exists(StoredObject(commit_file_path)))
        return {};

    auto in = object_storage->readObject(StoredObject(commit_file_path), context->getReadSettings());

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
    /// The constructor already ruled out a pre-existing commit file for every policy but
    /// `overwrite`, so seeing one here means another exporter committed this part while we wrote.
    if (file_already_exists_policy != FileAlreadyExistsPolicy::overwrite
        && object_storage->exists(StoredObject(commit_file_path)))
    {
        throw Exception(ErrorCodes::FILE_ALREADY_EXISTS, "Commit file {} already exists, aborting {} export", commit_file_path, transaction_id);
    }

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
