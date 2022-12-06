#pragma once

#include <Interpreters/Cache/FileSegment.h>

#include <IO/WriteBufferFromFileDecorator.h>

namespace DB
{

class WriteBufferToFileSegment : public WriteBufferFromFileDecorator
{
public:
    explicit WriteBufferToFileSegment(std::unique_ptr<WriteBuffer> impl_, FileSegment * file_segment_);

    void nextImpl() override;

    void finalizeImpl() override;

    ~WriteBufferToFileSegment() override;

private:
    FileSegment * file_segment;
};


}
