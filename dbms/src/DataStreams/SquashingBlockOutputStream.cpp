#include <DataStreams/SquashingBlockOutputStream.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

SquashingBlockOutputStream::SquashingBlockOutputStream(BlockOutputStreamPtr & dst, size_t min_block_size_rows, size_t min_block_size_bytes)
    : output(dst), transform(min_block_size_rows, min_block_size_bytes)
{
}


void SquashingBlockOutputStream::write(const Block & block)
{
    if (!header)
        header = block.cloneEmpty();

    SquashingTransform::Result result = transform.add(Block(block).mutateColumns());
    if (result.ready)
        output->write(header.cloneWithColumns(std::move(result.columns)));
}


void SquashingBlockOutputStream::finalize()
{
    if (all_written)
        return;

    all_written = true;

    if (!header)
        throw Exception("writeSuffix called without writing data.", ErrorCodes::LOGICAL_ERROR);

    SquashingTransform::Result result = transform.add({});
    if (result.ready && !result.columns.empty())
        output->write(header.cloneWithColumns(std::move(result.columns)));
}


void SquashingBlockOutputStream::flush()
{
    if (!disable_flush)
        finalize();
    output->flush();
}


void SquashingBlockOutputStream::writePrefix()
{
    output->writePrefix();
}


void SquashingBlockOutputStream::writeSuffix()
{
    finalize();
    output->writeSuffix();
}

}
