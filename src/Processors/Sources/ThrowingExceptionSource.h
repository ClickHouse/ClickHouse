#pragma once
#include <Processors/ISource.h>


namespace DB
{

class ThrowingExceptionSource : public ISource
{
public:

    using CallBack = std::function<Exception()>;

    explicit ThrowingExceptionSource(Block header, CallBack callback_)
        : ISource(std::move(header))
        , callback(std::move(callback_))
    {}

    String getName() const override { return "ThrowingExceptionSource"; }

protected:
    Chunk generate() override
    {
        throw callback();
    }

    CallBack callback;
};

}
