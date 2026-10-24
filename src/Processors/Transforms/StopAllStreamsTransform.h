#pragma once

#include <Processors/IProcessor.h>

namespace DB
{

class QueryPipelineBuilder;

/// Passes the chunks of one stream as is. If the consumer stops reading before the stream ends, tells all
/// the other streams to stop through `StopAllStreamsSignal`, and stops when told so by another stream.
/// A consumer waiting for data that never comes is woken this way.
class StopAllStreamsTransform final : public IProcessor
{
public:
    explicit StopAllStreamsTransform(SharedHeader header);

    String getName() const override { return "StopAllStreams"; }

    Status prepare() override;

private:
    bool signal_pending = false;
};

/// Connects the `StopAllStreamsTransform`s of all the streams: a chunk on any input finishes all the outputs.
class StopAllStreamsSignal final : public IProcessor
{
public:
    explicit StopAllStreamsSignal(size_t num_streams);

    String getName() const override { return "StopAllStreamsSignal"; }

    Status prepare() override;
};

/// Inserts a `StopAllStreamsTransform` into every stream of the pipelines, all connected to one
/// `StopAllStreamsSignal`, if there are several streams in total.
void addStopAllStreamsTransforms(std::initializer_list<QueryPipelineBuilder *> pipelines);

}
