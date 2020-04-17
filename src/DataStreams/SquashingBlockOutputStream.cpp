#include <DataStreams/SquashingBlockOutputStream.h>


namespace DB
{

SquashingBlockOutputStream::SquashingBlockOutputStream(BlockOutputStreamPtr dst, Block header_, size_t min_block_size_rows, size_t min_block_size_bytes)
    : output(std::move(dst)), header(std::move(header_)), transform(min_block_size_rows, min_block_size_bytes)
{
}


void SquashingBlockOutputStream::write(const Block & block)
{
    auto squashed_columns = transform.add(block);
    if (!squashed_columns.empty())
        output->write(header.cloneWithColumns(std::move(squashed_columns)));
}


void SquashingBlockOutputStream::finalize()
{
    if (all_written)
        return;

    all_written = true;

    auto squashed_columns = transform.add({});
    if (!squashed_columns.empty())
        output->write(header.cloneWithColumns(std::move(squashed_columns)));
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
