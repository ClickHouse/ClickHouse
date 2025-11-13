#pragma once

#include <functional>
#include <QueryPipeline/QueryPipeline.h>


namespace DB
{

class ProcessListEntry;

struct BlockIO
{
    BlockIO() = default;
    BlockIO(BlockIO &&) = default;

    BlockIO & operator= (BlockIO && rhs) noexcept;
    ~BlockIO();

    BlockIO(const BlockIO &) = delete;
    BlockIO & operator= (const BlockIO & rhs) = delete;

    std::shared_ptr<ProcessListEntry> process_list_entry;

    QueryPipeline pipeline;

    /// Callbacks for query logging could be set here.
    std::function<void(QueryPipeline &&, std::chrono::system_clock::time_point)> finish_callback;
    std::function<void(bool)> exception_callback;

    /// When it is true, don't bother sending any non-empty blocks to the out stream
    bool null_format = false;

    void onFinish(std::chrono::system_clock::time_point finish_time = std::chrono::system_clock::now());
    void onException(bool log_as_error=true);
    void onCancelOrConnectionLoss();

    /// Set is_all_data_sent in system.processes for this query.
    void setAllDataSent() const;

    /// Release query slot early to allow client to reuse it for his next query.
    void releaseQuerySlot() const;

private:
    void reset();
};

}
