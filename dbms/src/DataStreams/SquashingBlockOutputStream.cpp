#include <DataStreams/SquashingBlockOutputStream.h>


namespace DB
{

SquashingBlockOutputStream::SquashingBlockOutputStream(BlockOutputStreamPtr & dst, size_t min_block_size_rows, size_t min_block_size_bytes)
    : output(dst), transform(min_block_size_rows, min_block_size_bytes)
{
}


void SquashingBlockOutputStream::write(const Block & block)
{
    SquashingTransform::Result result = transform.add(Block(block));
    if (result.ready)
        output->write(result.block);
}


void SquashingBlockOutputStream::finalize()
{
    if (all_written)
        return;

    all_written = true;

    SquashingTransform::Result result = transform.add({});
    if (result.ready && result.block)
        output->write(result.block);
}


void SquashingBlockOutputStream::flush()
{
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
