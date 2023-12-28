#pragma once

#include <Processors/IProcessor.h>

namespace DB
{

class StreamingAdapter final : public IProcessor
{
public:
    explicit StreamingAdapter(const Block & header_);

    String getName() const override { return "StreamingAdapter"; }

    Status prepare() override;

private:
    enum class StreamingState
    {
        ReadingFromStorage,
        ReadingFromSubscription,
        Finished,
    };

    static std::string StateToString(StreamingState state);

    Status preparePair(InputPort * input, OutputPort * output);

    StreamingState state = StreamingState::ReadingFromStorage;

    InputPort * input_storage_port = nullptr;
    InputPort * input_subscription_port = nullptr;
    OutputPort * output_port = nullptr;
};

}
