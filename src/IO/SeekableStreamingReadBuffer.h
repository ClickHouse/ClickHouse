#pragma once

#include <IO/ReadBufferFromFileBase.h>

namespace DB
{

class SeekableStreamingReadBuffer : public ReadBufferFromFileBase
{
    std::unique_ptr<ReadBufferFromFileBase> nested;
public:
    SeekableStreamingReadBuffer(std::unique_ptr<ReadBufferFromFileBase> nested_): nested(std::move(nested_))
    {
        swap(*nested);
    }

    std::string getFileName() const override { return nested->getFileName(); }

    off_t getPosition() override
    {
        swap(*nested);
        off_t position = nested->getPosition();
        swap(*nested);
        return position;
    }

    off_t seek(off_t off, int whence) override
    {
        swap(*nested);
        off_t position = nested->seek(off, whence);
        swap(*nested);
        return position;
    }

    bool nextImpl() override
    {
        swap(*nested);
        bool nested_result = nested->next();
        swap(*nested);
        return nested_result;
    }
};

}
