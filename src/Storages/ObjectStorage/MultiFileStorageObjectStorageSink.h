#pragma once

#include <Storages/ObjectStorage/StorageObjectStorageSink.h>

namespace DB
{

/// This is useful when the data is too large to fit into a single file.
/// It will create a new file when the current file exceeds the max bytes or max rows limit.
/// Ships a commit file including the list of data files to make it transactional
class MultiFileStorageObjectStorageSink : public SinkToStorage
{
public:
    MultiFileStorageObjectStorageSink(
        const std::string & base_path_,
        const String & transaction_id_,
        ObjectStoragePtr object_storage_,
        StorageObjectStorageConfigurationPtr configuration_,
        std::size_t max_bytes_per_file_,
        std::size_t max_rows_per_file_,
        bool overwrite_if_exists_,
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
    ObjectStoragePtr object_storage;
    StorageObjectStorageConfigurationPtr configuration;
    std::size_t max_bytes_per_file;
    std::size_t max_rows_per_file;
    bool overwrite_if_exists;
    std::function<void(const std::string &)> new_file_path_callback;
    const std::optional<FormatSettings> format_settings;
    SharedHeader sample_block;
    ContextPtr context;
    
    std::vector<std::string> file_paths;
    std::shared_ptr<StorageObjectStorageSink> current_sink;
    std::size_t current_sink_written_rows = 0;

    std::string generateNewFilePath();
    std::shared_ptr<StorageObjectStorageSink> createNewSink();
    void commit();
};

}
