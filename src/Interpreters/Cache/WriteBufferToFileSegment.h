#pragma once

#include <IO/WriteBufferFromFileDecorator.h>
#include <Interpreters/Cache/FileSegment.h>
#include <IO/IReadableWriteBuffer.h>

namespace DB
{

class FileSegment;

class WriteBufferToFileSegment : public WriteBufferFromFileDecorator, public IReadableWriteBuffer
{
public:
    explicit WriteBufferToFileSegment(FileSegment * file_segment_);
    explicit WriteBufferToFileSegment(FileSegmentsHolderPtr segment_holder);

    void nextImpl() override;
    ~WriteBufferToFileSegment() override;

private:

    std::shared_ptr<ReadBuffer> getReadBufferImpl() override;

    /// Reference to the file segment in segment_holder if owned by this WriteBufferToFileSegment
    /// or to the external file segment passed to the constructor
    FileSegment * file_segment;

    /// Empty if file_segment is not owned by this WriteBufferToFileSegment
    FileSegmentsHolderPtr segment_holder;
};


}
