#pragma once

#include <IO/ReadBufferFromFileBase.h>


namespace DB
{

/// Delegates all reads to underlying buffer. Doesn't have own memory.
class ReadBufferFromFileDecorator : public ReadBufferFromFileBase
{
public:
    explicit ReadBufferFromFileDecorator(std::unique_ptr<SeekableReadBuffer> impl_);
    ReadBufferFromFileDecorator(std::unique_ptr<SeekableReadBuffer> impl_, const String & file_name_);

    std::string getFileName() const override;

    off_t getPosition() override;

    off_t seek(off_t off, int whence) override;

    bool nextImpl() override;

    /// External-buffer mode (`set(dest, n); next()`) bypasses `impl` — only
    /// the outer's `working_buffer` is reset by the non-virtual
    /// `ReadBuffer::set`. The swap-based `nextImpl` then calls `impl->next()`
    /// while `impl` may still have its own pending data, tripping
    /// `ReadBuffer::next`'s `chassert(!hasPendingData)`. Force the
    /// `buf.read(dest, n)` fallback path which drains via `eof()→next()` at
    /// the outer level. Inherited by `BoundedReadBuffer`.
    bool supportsExternalBufferMode() const override { return false; }

    bool isWithFileSize() const { return dynamic_cast<const WithFileSize *>(impl.get()) != nullptr; }

    const ReadBuffer & getWrappedReadBuffer() const { return *impl; }

    ReadBuffer & getWrappedReadBuffer() { return *impl; }

    std::optional<size_t> tryGetFileSize() override;

protected:
    std::unique_ptr<SeekableReadBuffer> impl;
    String file_name;
};

}
