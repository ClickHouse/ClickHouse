#pragma once

#include <IO/WriteBufferFromFileBase.h>

namespace DB
{

/// Delegates all writes to underlying buffer. Doesn't have own memory.
class WriteBufferFromFileDecorator : public WriteBufferFromFileBase
{
public:
    explicit WriteBufferFromFileDecorator(std::unique_ptr<WriteBuffer> impl_);

    ~WriteBufferFromFileDecorator() override;

    void sync() override;

    std::string getFileName() const override;

    void preFinalize() override;

    /// Forward the wrapped buffer's write-time ETag (if it is a file buffer that produced one), so a
    /// decorated S3 buffer still lets content-addressed callers skip the post-write HEAD.
    std::optional<std::string> getResultObjectETag() const override
    {
        if (const auto * file_buf = dynamic_cast<const WriteBufferFromFileBase *>(impl.get()))
            return file_buf->getResultObjectETag();
        return {};
    }

    const WriteBuffer & getImpl() const { return *impl; }

protected:
    void finalizeImpl() override;

    void cancelImpl() noexcept override;

    std::unique_ptr<WriteBuffer> impl;

private:
    void nextImpl() override;

    bool is_prefinalized = false;
};

}
