#pragma once

#include <IO/WriteBufferFromFileDecorator.h>

namespace DB
{

class FileSegment;

class WriteBufferToFileSegment : public WriteBufferFromFileDecorator
{
public:
    explicit WriteBufferToFileSegment(FileSegment * file_segment_);

    void nextImpl() override;

    ~WriteBufferToFileSegment() override;

private:
    FileSegment * file_segment;
};


}
