#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>
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
class NullAndDoCopyBlockInputStream : public IProfilingBlockInputStream
{
public:
    NullAndDoCopyBlockInputStream(const BlockInputStreamPtr & input_, BlockOutputStreamPtr output_)
        : input(input_), output(output_)
    {
        children.push_back(input_);
    }

    String getName() const override { return "NullAndDoCopy"; }

    Block getHeader() const override { return {}; }

protected:
    Block readImpl() override
    {
        copyData(*input, *output);
        return Block();
    }

private:
    BlockInputStreamPtr input;
    BlockOutputStreamPtr output;
};

}
