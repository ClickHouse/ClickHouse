#include <Columns/ColumnsNumber.h>

#include <DataStreams/MergingAggregatedBlockInputStream.h>


namespace DB
{


Block MergingAggregatedBlockInputStream::readImpl()
{
    if (!executed)
    {
        executed = true;
        AggregatedDataVariants data_variants;

        Aggregator::CancellationHook hook = [&]() { return this->isCancelled(); };
        aggregator.setCancellationHook(hook);

        aggregator.mergeStream(children.back(), data_variants, max_threads);
        blocks = aggregator.convertToBlocks(data_variants, final, max_threads);
        it = blocks.begin();
    }

    Block res;
    if (isCancelled() || it == blocks.end())
        return res;

    res = std::move(*it);
    ++it;

    return res;
}


}
