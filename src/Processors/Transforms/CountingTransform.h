#pragma once

#include <IO/Progress.h>
#include <Processors/Transforms/ExceptionKeepingTransform.h>
#include <Access/EnabledQuota.h>


namespace DB
{

class QueryStatus;
using QueryStatusPtr = std::shared_ptr<QueryStatus>;
class ThreadStatus;

/// Proxy class which counts number of written block, rows, bytes
class CountingTransform final : public ExceptionKeepingTransform
{
public:
    explicit CountingTransform(
        const Block & header,
        std::shared_ptr<const EnabledQuota> quota_ = nullptr)
        : ExceptionKeepingTransform(header, header)
        , quota(std::move(quota_)) {}

    String getName() const override { return "CountingTransform"; }

    void setProgressCallback(const ProgressCallback & callback)
    {
        progress_callback = callback;
    }

    void setProcessListElement(QueryStatusPtr elem)
    {
        process_elem = elem;
    }

    void onConsume(Chunk chunk) override;
    GenerateResult onGenerate() override
    {
        GenerateResult res;
        res.chunk = std::move(cur_chunk);
        return res;
    }

protected:
    ProgressCallback progress_callback;
    QueryStatusPtr process_elem;

    /// Quota is used to limit amount of written bytes.
    std::shared_ptr<const EnabledQuota> quota;
    Chunk cur_chunk;
};

}
