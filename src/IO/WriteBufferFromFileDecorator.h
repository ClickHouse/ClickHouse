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

    void finalize() override;

    void sync() override;

    std::string getFileName() const override;

protected:
    std::unique_ptr<WriteBuffer> impl;
    bool finalized = false;

private:
    void nextImpl() override;
};

}
