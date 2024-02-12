#pragma once

#include <IO/Progress.h>
#include <Processors/Transforms/ExceptionKeepingTransform.h>
#include <Processors/ISimpleTransform.h>
#include <Access/EnabledQuota.h>


namespace DB
{

class QueryStatus;
using QueryStatusPtr = std::shared_ptr<QueryStatus>;
class ThreadStatus;

/// Proxy class which counts number of written block, rows, bytes
class CountingBase
{
public:
    explicit CountingBase(
        ThreadStatus * thread_status_ = nullptr,
        std::shared_ptr<const EnabledQuota> quota_ = nullptr)
        : thread_status(thread_status_), quota(std::move(quota_)) {}

    void setProgressCallback(const ProgressCallback & callback)
    {
        progress_callback = callback;
    }

    void setProcessListElement(QueryStatusPtr elem)
    {
        process_elem = elem;
    }

    const Progress & getProgress() const
    {
        return progress;
    }

    void count(const Chunk & chunk);

protected:
    Progress progress;
    ProgressCallback progress_callback;
    QueryStatusPtr process_elem;
    ThreadStatus * thread_status = nullptr;

    /// Quota is used to limit amount of written bytes.
    std::shared_ptr<const EnabledQuota> quota;
};

class CountingTransform2 final : public ExceptionKeepingTransform
{
public:
    explicit CountingTransform2(
        const Block & header,
        ThreadStatus * thread_status_ = nullptr,
        std::shared_ptr<const EnabledQuota> quota_ = nullptr)
        : ExceptionKeepingTransform(header, header), counting(thread_status_, std::move(quota_)) {}

    String getName() const override { return "CountingTransform"; }
    void onConsume(Chunk chunk) override
    {
        counting.count(chunk);
        cur_chunk = std::move(chunk);
    }

    GenerateResult onGenerate() override
    {
        GenerateResult res;
        res.chunk = std::move(cur_chunk);
        return res;
    }

    void setProgressCallback(const ProgressCallback & callback)
    {
        counting.setProgressCallback(callback);
    }

    void setProcessListElement(QueryStatusPtr elem)
    {
        counting.setProcessListElement(std::move(elem));
    }

protected:
    CountingBase counting;
    Chunk cur_chunk;
};

class SimpleCountingTransform final : public ISimpleTransform
{
public:
    explicit SimpleCountingTransform(
        const Block & header,
        std::shared_ptr<const EnabledQuota> quota_ = nullptr)
        : ISimpleTransform(header, header, false)
        , counting(nullptr, std::move(quota_))
    {}

    String getName() const override { return "SimpleCountingTransform"; }
    void transform(Chunk & chunk) override
    {
        counting.count(chunk);
    }

    void setProgressCallback(const ProgressCallback & callback)
    {
        counting.setProgressCallback(callback);
    }

    void setProcessListElement(QueryStatusPtr elem)
    {
        counting.setProcessListElement(std::move(elem));
    }

protected:
    CountingBase counting;
};

}
