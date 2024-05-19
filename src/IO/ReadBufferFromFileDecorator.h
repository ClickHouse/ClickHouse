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

    bool isWithFileSize() const { return dynamic_cast<const WithFileSize *>(impl.get()) != nullptr; }

    const ReadBuffer & getWrappedReadBuffer() const { return *impl; }

    ReadBuffer & getWrappedReadBuffer() { return *impl; }

    size_t getFileSize() override;

protected:
    std::unique_ptr<SeekableReadBuffer> impl;
    String file_name;
};

}
