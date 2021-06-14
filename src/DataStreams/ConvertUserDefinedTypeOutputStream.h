#pragma once

#include <DataStreams/IBlockOutputStream.h>

namespace DB
{


class ConvertUserDefinedTypeOutputStream : public IBlockOutputStream
{
public:
    ConvertUserDefinedTypeOutputStream(const BlockOutputStreamPtr & stream_)
        : stream(stream_) {}

    Block getHeader() const override;
    void write(const Block & block) override;

    void writePrefix() override                         { stream->writePrefix(); }
    void writeSuffix() override                         { stream->writeSuffix(); }
    void flush() override                               { stream->flush(); }
    void onProgress(const Progress & current_progress) override { stream->onProgress(current_progress); }
    String getContentType() const override              { return stream->getContentType(); }

private:
    Block convertUserDefinedTypeToNested(const Block & block) const;

protected:
    BlockOutputStreamPtr stream;
};

}
