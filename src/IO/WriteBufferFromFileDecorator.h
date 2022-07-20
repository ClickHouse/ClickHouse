#pragma once

#include <IO/WriteBufferFromFileBase.h>

namespace DB
{

/// Delegates all writes to underlying buffer. Doesn't have own memory.
class WriteBufferFromFileDecorator : public WriteBufferFromFileBase
{
public:
    explicit WriteBufferFromFileDecorator(std::unique_ptr<WriteBuffer> impl_);

    void sync() override;

    std::string getFileName() const override;

    void preFinalize() override
    {
        next();
        impl->preFinalize();
        is_prefinalized = true;
    }

    const WriteBuffer & getImpl() const { return *impl; }

protected:
    void finalizeImpl() override;

    std::unique_ptr<WriteBuffer> impl;

private:
    void nextImpl() override;

    bool is_prefinalized = false;
};

}
