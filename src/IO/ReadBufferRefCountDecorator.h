#pragma once

#include <memory>
#include <IO/ReadBuffer.h>
#include "Common/Macros.h"


namespace DB
{

class ReadBufferRefCountDecorator;
using ReadBufferRefCountDecoratorPtr = std::shared_ptr<ReadBufferRefCountDecorator>;

/// Delegates all reads to underlying buffer. Doesn't have own memory.
class ReadBufferRefCountDecorator : public ReadBuffer, public std::enable_shared_from_this<ReadBufferRefCountDecorator>
{
public:
    using Ptr = ReadBufferRefCountDecoratorPtr;

    template <class... Args>
    static Ptr create(Args &&... args)
    {
        struct make_shared_enabler : public ReadBufferRefCountDecorator
        {
            explicit make_shared_enabler(Args &&... args) : ReadBufferRefCountDecorator(std::forward<Args>(args)...) { }
        };
        return std::make_shared<make_shared_enabler>(std::forward<Args>(args)...);
    }

    ~ReadBufferRefCountDecorator() override;

    bool nextImpl() override;
    size_t getRefCount();

private:
    explicit ReadBufferRefCountDecorator(ReadBuffer & impl_);
    explicit ReadBufferRefCountDecorator(std::unique_ptr<ReadBuffer> impl_);

    ReadBuffer & impl;
    std::unique_ptr<ReadBuffer> holder;
    LoggerPtr logger;
};

}
