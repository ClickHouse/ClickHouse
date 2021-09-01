#pragma once

#include <Processors/ISimpleTransform.h>
#include <Interpreters/ProcessList.h>


namespace DB
{


/// Proxy class which counts number of written block, rows, bytes
class CountingTransform final : public ISimpleTransform
{
public:
    explicit CountingTransform(const Block & header) : ISimpleTransform(header, header, false) {}

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
};

}
