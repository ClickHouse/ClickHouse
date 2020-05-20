#pragma once

#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/copyData.h>


namespace DB
{

class IBlockOutputStream;
using BlockOutputStreamPtr = std::shared_ptr<IBlockOutputStream>;


/** An empty stream of blocks.
  * But at the first read attempt, copies the data from the passed `input` to the `output`.
  * This is necessary to execute the query INSERT SELECT - the query copies data, but returns nothing.
  * The query could be executed without wrapping it in an empty BlockInputStream,
  *  but the progress of query execution and the ability to cancel the query would not work.
  */
class NullAndDoCopyBlockInputStream : public IBlockInputStream
{
public:
    NullAndDoCopyBlockInputStream(const BlockInputStreamPtr & input_, BlockOutputStreamPtr output_)
    {
        input_streams.push_back(input_);
        output_streams.push_back(output_);

        for (auto & input_stream : input_streams)
            children.push_back(input_stream);
    }

    NullAndDoCopyBlockInputStream(const BlockInputStreams & input_, BlockOutputStreams & output_)
        : input_streams(input_), output_streams(output_)
    {
        for (auto & input_stream : input_)
            children.push_back(input_stream);
    }

    /// Suppress readPrefix and readSuffix, because they are called by copyData.
    void readPrefix() override {}
    void readSuffix() override {}

    String getName() const override { return "NullAndDoCopy"; }

    Block getHeader() const override { return {}; }
    Block getTotals() override { return {}; }
    Block getExtremes() override { return {}; }

protected:
    Block readImpl() override
    {
        /// We do not use cancel flag here.
        /// If query was cancelled, it will be processed by child streams.
        /// Part of the data will be processed.

        if (input_streams.size() == 1 && output_streams.size() == 1)
            copyData(*input_streams.at(0), *output_streams.at(0));
        else
            copyData(input_streams, output_streams);
        return Block();
    }

private:
    BlockInputStreams input_streams;
    BlockOutputStreams output_streams;
};

}
