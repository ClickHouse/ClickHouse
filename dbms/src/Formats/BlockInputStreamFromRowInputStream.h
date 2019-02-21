#pragma once

#include <Core/Defines.h>
#include <DataStreams/IBlockInputStream.h>
#include <Formats/FormatSettings.h>
#include <Formats/IRowInputStream.h>


namespace DB
{

/** Makes block-oriented stream on top of row-oriented stream.
  * It is used to read data from text formats.
  *
  * Also controls over parsing errors and prints diagnostic information about them.
  */
class BlockInputStreamFromRowInputStream : public IBlockInputStream
{
public:
    /** sample_ - block with zero rows, that structure describes how to interpret values */
    BlockInputStreamFromRowInputStream(
        const RowInputStreamPtr & row_input_,
        const Block & sample_,
        UInt64 max_block_size_,
        const FormatSettings & settings);

    void readPrefix() override { row_input->readPrefix(); }
    void readSuffix() override;

    String getName() const override { return "BlockInputStreamFromRowInputStream"; }

    RowInputStreamPtr & getRowInput() { return row_input; }

    Block getHeader() const override { return sample; }

    const BlockMissingValues & getMissingValues() const override { return block_missing_values; }

protected:
    Block readImpl() override;

private:
    RowInputStreamPtr row_input;
    Block sample;
    UInt64 max_block_size;
    BlockMissingValues block_missing_values;

    UInt64 allow_errors_num;
    Float32 allow_errors_ratio;

    size_t total_rows = 0;
    size_t num_errors = 0;
};

}
