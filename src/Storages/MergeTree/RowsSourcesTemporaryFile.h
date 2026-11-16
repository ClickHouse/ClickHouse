#pragma once

#include <memory>
#include <IO/ReadSettings.h>
#include <Interpreters/TemporaryDataOnDisk.h>
#include <Processors/QueryPlan/TemporaryFiles.h>
#include <Storages/MergeTree/IDataPartStorage.h>

namespace DB
{

class CompressedWriteBuffer;
class WriteBufferFromFileBase;

/// Manages the "rows_sources" temporary file that is used during vertical merge.
class RowsSourcesTemporaryFile : public ITemporaryFileLookup
{
public:
    /// A logical name of the temporary file under which it will be known to the plan steps that use it.
    static constexpr auto FILE_ID = "rows_sources";

    /// Returns written data size in bytes
    virtual size_t finalizeWriting() = 0;
};

class RowsSourcesOnTemporaryDisk final : public RowsSourcesTemporaryFile
{
public:
    explicit RowsSourcesOnTemporaryDisk(TemporaryDataOnDiskScopePtr temporary_data_on_disk_);

    WriteBuffer & getTemporaryFileForWriting(const String & name) override;
    std::unique_ptr<ReadBuffer> getTemporaryFileForReading(const String & name) override;
    size_t finalizeWriting() override;

private:
    std::unique_ptr<TemporaryDataBuffer> tmp_data_buffer;
    TemporaryDataOnDiskScopePtr temporary_data_on_disk;
    bool finalized = false;
    size_t final_size = 0;
};

class RowsSourcesInPart final : public RowsSourcesTemporaryFile
{
public:
    RowsSourcesInPart(MutableDataPartStoragePtr part_storage_, String file_name_, ReadSettings read_settings_);
    ~RowsSourcesInPart() override;

    WriteBuffer & getTemporaryFileForWriting(const String & name) override;
    std::unique_ptr<ReadBuffer> getTemporaryFileForReading(const String & name) override;
    size_t finalizeWriting() override;

    /// The file was written by an earlier attempt.
    void restore(size_t file_size_);
    [[nodiscard]] size_t getFileSize() const;

private:
    MutableDataPartStoragePtr part_storage;
    String file_name;
    ReadSettings read_settings;
    std::unique_ptr<WriteBufferFromFileBase> out;
    std::unique_ptr<CompressedWriteBuffer> compressed_out;
    size_t file_size = 0;
    bool finalized = false;
};

}
