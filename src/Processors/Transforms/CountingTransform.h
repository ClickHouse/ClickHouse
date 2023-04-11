#pragma once

#include <IO/Progress.h>
#include <Processors/Transforms/ExceptionKeepingTransform.h>
#include <Access/EnabledQuota.h>


namespace DB
{

class QueryStatus;
class ThreadStatus;

/// Proxy class which counts number of written block, rows, bytes
class CountingTransform final : public ExceptionKeepingTransform
{
public:
    explicit CountingTransform(
        const Block & header,
        ThreadStatus * thread_status_ = nullptr,
        std::shared_ptr<const EnabledQuota> quota_ = nullptr)
        : ExceptionKeepingTransform(header, header)
        , thread_status(thread_status_), quota(std::move(quota_)) {}

    String getName() const override { return "CountingTransform"; }

    void setProgressCallback(const ProgressCallback & callback)
    {
        progress_callback = callback;
    }

    void setProcessListElement(QueryStatus * elem)
    {
        process_elem = elem;
    }

    const Progress & getProgress() const
    {
        return progress;
    }

    void onConsume(Chunk chunk) override;
    GenerateResult onGenerate() override
    {
        GenerateResult res;
        res.chunk = std::move(cur_chunk);
        return res;
    }

protected:
    Progress progress;
    ProgressCallback progress_callback;
    QueryStatus * process_elem = nullptr;
    ThreadStatus * thread_status = nullptr;

    /// Quota is used to limit amount of written bytes.
    std::shared_ptr<const EnabledQuota> quota;
    Chunk cur_chunk;
};

}
