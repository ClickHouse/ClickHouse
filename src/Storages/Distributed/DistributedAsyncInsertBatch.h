#pragma once

#include <string>
#include <vector>

namespace DB
{

class DistributedAsyncInsertDirectoryQueue;
class WriteBuffer;
class ReadBuffer;
class SettingsChanges;

class DistributedAsyncInsertBatch
{
public:
    explicit DistributedAsyncInsertBatch(DistributedAsyncInsertDirectoryQueue & parent_);

    bool isEnoughSize() const;
    void send(const SettingsChanges & settings_changes);

    /// Write batch to current_batch.txt
    void serialize();

    /// Read batch from current_batch.txt
    void deserialize();

    /// Does all required files exists?
    /// (The only way variant when it is valid is during restoring batch from disk).
    bool valid();

    size_t total_rows = 0;
    size_t total_bytes = 0;
    std::vector<std::string> files;

private:
    void writeText(WriteBuffer & out);
    void readText(ReadBuffer & in);
    void sendBatch(const SettingsChanges & settings_changes);
    void sendSeparateFiles(const SettingsChanges & settings_changes);

    DistributedAsyncInsertDirectoryQueue & parent;

    /// Does the batch had been created from the files in current_batch.txt?
    bool recovered = false;

    bool split_batch_on_failure = true;
    bool fsync = false;
    bool dir_fsync = false;
};

}
