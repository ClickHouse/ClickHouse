#pragma once

#include <Processors/Transforms/ExceptionKeepingTransform.h>
#include <Interpreters/ProcessList.h>


namespace DB
{


/// Proxy class which counts number of written block, rows, bytes
class CountingTransform final : public ExceptionKeepingTransform
{
public:
    explicit CountingTransform(const Block & header, ThreadStatus * thread_status_ = nullptr)
        : ExceptionKeepingTransform(header, header), thread_status(thread_status_) {}

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

    void transform(Chunk & chunk) override;

protected:
    Progress progress;
    ProgressCallback progress_callback;
    QueryStatus * process_elem = nullptr;
    ThreadStatus * thread_status = nullptr;
};

}
