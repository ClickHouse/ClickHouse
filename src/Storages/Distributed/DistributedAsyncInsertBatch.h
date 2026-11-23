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

    /// Try to recover batch (from current_batch.txt)
    /// @return false if the recovering failed and the batch should be started from scratch
    bool recoverBatch();

    bool isEnoughSize() const;
    void send(const SettingsChanges & settings_changes, bool update_current_batch);

    size_t total_rows = 0;
    size_t total_bytes = 0;
    std::vector<std::string> files;

private:
    void sendBatch(const SettingsChanges & settings_changes);
    void sendSeparateFiles(const SettingsChanges & settings_changes);

    DistributedAsyncInsertDirectoryQueue & parent;

    bool split_batch_on_failure = true;
    bool fsync = false;

    /// Save currently processed files to current_batch.txt
    void serialize();
};

}
