#pragma once

#include <IO/ReadBufferFromFileBase.h>
#include <Common/Priority.h>

namespace DB
{

/// Read buffer that allows to read from file view.
/// File view is part of the file (archive) that repsents the other file.
/// For the user this read buffer looks exactly the same as if
/// it was ReadBufferFrom{File/S3/...} from requested file.
/// It is used to read files from archive in PackedFilesIO.
class ReadBufferFromFileView : public ReadBufferFromFileBase
{
public:
    ReadBufferFromFileView(std::unique_ptr<ReadBufferFromFileBase> impl_, const String & file_name_, off_t left_bound_, off_t right_bound_);

    bool supportsRightBoundedReads() const override { return true; }

    size_t getFileOffsetOfBufferEnd() const override { return file_offset_of_buffer_end - left_bound; }
    std::optional<size_t> tryGetFileSize() override { return right_bound - left_bound; }
    String getFileName() const override { return file_name; }

    void prefetch(Priority priority) override;
    void setReadUntilPosition(size_t position) override;
    void setReadUntilEnd() override;

    bool nextImpl() override;
    off_t seek(off_t off, int whence) override;

    off_t getPosition() override;

private:
    size_t getRightBound() const;

    /// Resizes working buffer if it exceeds the right bound.
    void resizeWorkingBuffer();

    const std::unique_ptr<ReadBufferFromFileBase> impl;
    const String file_name;
    const size_t left_bound;
    const size_t right_bound;

    std::optional<size_t> read_until_position;
    size_t file_offset_of_buffer_end = 0;

    /// All operations with @impl should be executed with
    /// original read buffer because @impl may store its own
    /// file offsets and there may be logic that requires these
    /// offsets and the size of buffer to be consistent
    /// (e.g. optimizations of seeks).
    Buffer original_working_buffer;

    template <typename Op>
    void executeWithOriginalBuffer(Op && op);
};

}
