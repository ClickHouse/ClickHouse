#pragma once
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <Interpreters/ProcessList.h>


namespace DB
{


/// Proxy class which counts number of written block, rows, bytes
class CountingBlockOutputStream : public IBlockOutputStream
{
public:

    CountingBlockOutputStream(const BlockOutputStreamPtr & stream_)
        : stream(stream_) {}

    void setProgressCallback(const ProgressCallback & callback)
    {
        progress_callback = callback;
    }

    void setProcessListElement(ProcessListElement * elem)
    {
        process_elem = elem;
    }

    const Progress & getProgress() const
    {
        return progress;
    }

    void write(const Block & block) override;

    void writePrefix() override                         { stream->writePrefix(); }
    void writeSuffix() override                         { stream->writeSuffix(); }
    void flush() override                               { stream->flush(); }
    void onProgress(const Progress & progress) override { stream->onProgress(progress); }
    String getContentType() const override              { return stream->getContentType(); }

protected:

    BlockOutputStreamPtr stream;
    Progress progress;
    ProgressCallback progress_callback;
    ProcessListElement * process_elem = nullptr;
};

}
