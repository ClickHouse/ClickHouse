#pragma once

#include <Core/Defines.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/IRowInputStream.h>


namespace DB
{

/** Makes block-oriented stream on top of row-oriented stream.
  * It is used to read data from text formats.
  *
  * Also controls over parsing errors and prints diagnostic information about them.
  */
class BlockInputStreamFromRowInputStream : public IProfilingBlockInputStream
{
public:
    /** sample_ - block with zero rows, that structure describes how to interpret values */
    BlockInputStreamFromRowInputStream(
        RowInputStreamPtr row_input_,
        const Block & sample_,
        size_t max_block_size_,
        UInt64 allow_errors_num_,
        Float64 allow_errors_ratio_);

    void readPrefix() override { row_input->readPrefix(); }
    void readSuffix() override { row_input->readSuffix(); }

    String getName() const override { return "BlockInputStreamFromRowInputStream"; }

    String getID() const override
    {
        std::stringstream res;
        res << this;
        return res.str();
    }

    RowInputStreamPtr & getRowInput() { return row_input; }

protected:
    Block readImpl() override;

private:
    RowInputStreamPtr row_input;
    const Block sample;
    size_t max_block_size;

    UInt64 allow_errors_num;
    Float64 allow_errors_ratio;

    size_t total_rows = 0;
    size_t num_errors = 0;
};

}
