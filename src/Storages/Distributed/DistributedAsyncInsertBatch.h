#pragma once

#include <string>
#include <vector>

namespace DB
{

class DistributedAsyncInsertDirectoryQueue;
class WriteBuffer;
class ReadBuffer;

class DistributedAsyncInsertBatch
{
public:
    explicit DistributedAsyncInsertBatch(DistributedAsyncInsertDirectoryQueue & parent_);

    bool isEnoughSize() const;
    void send();

    void serialize();
    void deserialize();

    size_t total_rows = 0;
    size_t total_bytes = 0;
    std::vector<std::string> files;

private:
    void writeText(WriteBuffer & out);
    void readText(ReadBuffer & in);
    void sendBatch();
    void sendSeparateFiles();

    DistributedAsyncInsertDirectoryQueue & parent;

    /// Does the batch had been created from the files in current_batch.txt?
    bool recovered = false;

    bool split_batch_on_failure = true;
    bool fsync = false;
    bool dir_fsync = false;
};

}
