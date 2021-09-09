#include <Core/Block.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/formatBlock.h>

namespace DB
{

void formatBlock(BlockOutputStreamPtr & out, const Block & block)
{
    out->writePrefix();
    out->write(block);
    out->writeSuffix();
    out->flush();
}

}
