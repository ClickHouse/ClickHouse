#include <Processors/Sources/SourceFromTotals.h>
#include <DataStreams/IBlockInputStream.h>

namespace DB
{

SourceFromTotals::SourceFromTotals(InputStreamHolders holders_)
    : ISource(holders_.at(0)->getStream().getHeader()), holders(std::move(holders_))
{
}

Chunk SourceFromTotals::generate()
{
    if (generated)
        return {};

    generated = true;

    for (auto & holder : holders)
    {
        holder->readSuffix();

        if (auto block = holder->getStream().getTotals())
            return Chunk(block.getColumns(), 1);
    }

    return {};
}

}
