#pragma once
#include <DataStreams/IBlockOutputStream.h>
#include <Processors/Sinks/SinkToStorage.h>

namespace DB
{

class PushingToSinkBlockOutputStream : public IBlockOutputStream
{
public:
    explicit PushingToSinkBlockOutputStream(SinkToStoragePtr sink_)
        : sink(std::move(sink_)), port(sink->getPort().getHeader(), sink.get()) {}

    Block getHeader() const override { return sink->getPort().getHeader(); }

    void write(const Block & block) override
    {
        /// In case writePrefix was not called.
        if (!port.isConnected())
            writePrefix();

        size_t num_rows = block.rows();
        Chunk chunk(block.getColumns(), num_rows);
        port.push(std::move(chunk));

        while (true)
        {
            switch (auto status = sink->prepare())
            {
                case IProcessor::Status::Ready:
                    sink->work();
                    break;
                case IProcessor::Status::NeedData:
                    return;
                case IProcessor::Status::Async: [[fallthrough]];
                case IProcessor::Status::ExpandPipeline: [[fallthrough]];
                case IProcessor::Status::Finished: [[fallthrough]];
                case IProcessor::Status::PortFull:
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Status {} in not expected in PushingToSinkBlockOutputStream::writePrefix",
                        IProcessor::statusToName(status));
            }
        }
    }

    void writePrefix() override
    {
        connect(port, sink->getPort());

        while (true)
        {
            switch (auto status = sink->prepare())
            {
                case IProcessor::Status::Ready:
                    sink->work();
                    break;
                case IProcessor::Status::NeedData:
                    return;
                case IProcessor::Status::Async: [[fallthrough]];
                case IProcessor::Status::ExpandPipeline: [[fallthrough]];
                case IProcessor::Status::Finished: [[fallthrough]];
                case IProcessor::Status::PortFull:
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Status {} in not expected in PushingToSinkBlockOutputStream::writePrefix",
                        IProcessor::statusToName(status));
            }
        }
    }

    void writeSuffix() override
    {
        sink->getPort().close();
        while (true)
        {
            switch (auto status = sink->prepare())
            {
                case IProcessor::Status::Ready:
                    sink->work();
                    break;
                case IProcessor::Status::Finished:
                    return;
                case IProcessor::Status::NeedData:
                case IProcessor::Status::Async:
                case IProcessor::Status::ExpandPipeline:
                case IProcessor::Status::PortFull:
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Status {} in not expected in PushingToSinkBlockOutputStream::writeSuffix",
                        IProcessor::statusToName(status));
            }
        }
    }

private:
    SinkToStoragePtr sink;
    OutputPort port;
};

}
