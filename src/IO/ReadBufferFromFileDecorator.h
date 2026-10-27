#pragma once

#include <IO/ReadBufferFromFileBase.h>
#include <IO/IReadBufferMetadataProvider.h>


namespace DB
{

/// Delegates all reads to underlying buffer. Doesn't have own memory.
class ReadBufferFromFileDecorator : public ReadBufferFromFileBase, public IReadBufferMetadataProvider
{
public:
    explicit ReadBufferFromFileDecorator(std::unique_ptr<SeekableReadBuffer> impl_);
    ReadBufferFromFileDecorator(std::unique_ptr<SeekableReadBuffer> impl_, const String & file_name_);

    std::string getFileName() const override;

    off_t getPosition() override;

    off_t seek(off_t off, int whence) override;

    bool nextImpl() override;

    bool isWithFileSize() const { return dynamic_cast<const WithFileSize *>(impl.get()) != nullptr; }

    const ReadBuffer & getWrappedReadBuffer() const { return *impl; }

    ReadBuffer & getWrappedReadBuffer() { return *impl; }

    std::optional<size_t> tryGetFileSize() override;
    std::optional<Field> getMetadata(const String & name) const override;

    /// The swap-based `nextImpl` calls `impl->next()` while `impl` may still have pending data,
    /// tripping `ReadBuffer::next`'s `chassert(!hasPendingData)` under set()+next(). Force the
    /// `read(dest, n)` fallback, which drains via `eof() -> next()` at the outer level. Inherited
    /// by `BoundedReadBuffer`.
    bool supportsExternalBufferMode() const override { return false; }

protected:
    std::unique_ptr<SeekableReadBuffer> impl;
    String file_name;
};

}
