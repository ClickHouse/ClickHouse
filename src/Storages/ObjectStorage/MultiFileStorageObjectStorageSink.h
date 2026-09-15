#pragma once

#include <Core/SettingsEnums.h>
#include <Storages/ObjectStorage/StorageObjectStorageSink.h>

namespace DB
{

/// This is useful when the data is too large to fit into a single file.
/// It will create a new file when the current file exceeds the max bytes or max rows limit.
/// Ships a commit file including the list of data files to make it transactional
class MultiFileStorageObjectStorageSink : public SinkToStorage
{
public:
    using FileAlreadyExistsPolicy = MergeTreePartExportFileAlreadyExistsPolicy;

    MultiFileStorageObjectStorageSink(
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
        ContextPtr context_);

    ~MultiFileStorageObjectStorageSink() override;

    void consume(Chunk & chunk) override;

    void onFinish() override;

    String getName() const override { return "MultiFileStorageObjectStorageSink"; }

private:
    const std::string base_path;
    const String transaction_id;
    /// Written by `commit` only after every data file has been finalized, so its presence --
    /// unlike that of any individual data file -- proves a previous export of this part
    /// produced the whole set.
    const std::string commit_file_path;
    ObjectStoragePtr object_storage;
    StorageObjectStorageConfigurationPtr configuration;
    std::size_t max_bytes_per_file;
    std::size_t max_rows_per_file;
    FileAlreadyExistsPolicy file_already_exists_policy;
    /// Data files left behind by an attempt that never reached `commit` have to be rewritten.
    bool overwrite_data_files = false;
    std::function<void(const std::string &)> new_file_path_callback;
    const std::optional<FormatSettings> format_settings;
    SharedHeader sample_block;
    ContextPtr context;
    
    std::vector<std::string> file_paths;
    std::shared_ptr<StorageObjectStorageSink> current_sink;
    std::size_t current_sink_written_rows = 0;

    std::string generateNewFilePath();
    std::shared_ptr<StorageObjectStorageSink> createNewSink();
    /// The data files a previous export of this part committed, or nothing when it never committed.
    std::optional<std::vector<std::string>> tryReadCommittedPaths() const;
    void commit();
};

}
