#pragma once

#include <Interpreters/Aggregator.h>
#include <IO/ReadBufferFromFile.h>
#include <Compression/CompressedReadBuffer.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/ParallelInputsProcessor.h>
#include <DataStreams/TemporaryFileStream.h>


namespace DB
{


class EarlyWindowBlockInputStream : public IBlockInputStream
{
public:
    EarlyWindowBlockInputStream(const BlockInputStreamPtr & input, const std::vector<Aggregator::Params> & params_vec_);
    virtual ~EarlyWindowBlockInputStream();

    String getName() const override { return "EarlyWindow"; }

    Block getHeader() const override;

protected:
    Block readImpl() override;

private:
    std::vector<Aggregator::Params> params_vec;
    std::vector<Aggregator*> aggregators;
    bool executed;
};


}
