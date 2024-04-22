#pragma once

#include <Poco/Logger.h>

#include <Core/MultiEnum.h>

#include <Processors/IProcessor.h>
#include <Processors/Streaming/ISequencer.h>
#include <Processors/Streaming/ReadingSourceOption.h>

namespace DB
{

class StreamingAdapter final : public IProcessor
{
    Status pullData(InputPort * input);

    void readFromStorage();
    void readFromSubscription();
    Status writeToOutputs();

    bool isFinished() const;
    void closeAllPorts();

public:
    explicit StreamingAdapter(const Block & header_, SequencerPtr sequencer_, ReadingSourceOptions reading_sources_);

    String getName() const override { return "StreamingAdapter"; }

    Status prepare() override;
    void work() override;

private:
    SequencerPtr sequencer;
    ReadingSourceOptions reading_sources;

    std::vector<InputPort *> storage_ports;
    std::vector<InputPort *> subscription_ports;

    Poco::Logger * log = &Poco::Logger::get("StreamingAdapter");
};

}
