#pragma once
#include <Processors/IProcessor.h>

namespace DB
{

/// For now, this sink throws only last exception if any.
class ExceptionHandlingSink final : public IProcessor
{
public:
    explicit ExceptionHandlingSink(Block header)
        : IProcessor({std::move(header)}, {})
        , input(inputs.front())
    {
    }

    String getName() const override { return "ExceptionHandlingSink"; }

    Status prepare() override
    {
        while (!input.isFinished())
        {
            input.setNeeded();
            if (!input.hasData())
                return Status::NeedData;

            auto data = input.pullData();
            if (data.exception)
                last_exception = std::move(data.exception);
        }

        if (last_exception)
            return Status::Ready;

        return Status::Finished;
    }

    void work() override
    {
        if (last_exception)
            std::rethrow_exception(std::move(last_exception));
    }

    InputPort & getInputPort() { return input; }

private:
    InputPort & input;
    std::exception_ptr last_exception;
};

}
