#pragma once
#include <Processors/ISource.h>


namespace DB
{

/// This source is throwing exception at the first attempt to read from it.
/// Can be used as a additional check that pipeline (or its part) is never executed.
class ThrowingExceptionSource : public ISource
{
public:

    using CallBack = std::function<Exception()>;

    explicit ThrowingExceptionSource(SharedHeader header, CallBack callback_)
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
