#include <Common/ClickHouseRevision.h>

#include <DataStreams/BlocksListBlockInputStream.h>
#include <DataStreams/MergingAggregatedMemoryEfficientBlockInputStream.h>
#include <DataStreams/Cuda/CudaAggregatingBlockInputStream.h>
#include <DataStreams/NativeBlockInputStream.h>


namespace ProfileEvents
{
    extern const Event ExternalAggregationMerge;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

Block CudaAggregatingBlockInputStream::getHeader() const
{
    return aggregator.getHeader(final);
}


Block CudaAggregatingBlockInputStream::readImpl()
{
    if (!executed)
    {
        executed = true;
        CudaAggregatedDataVariantsPtr data_variants = std::make_shared<CudaAggregatedDataVariants>();

        //Aggregator::CancellationHook hook = [&]() { return this->isCancelled(); };
        //aggregator.setCancellationHook(hook);

        aggregator.execute(children.back(), *data_variants);

        blocks = aggregator.convertToBlocks(*data_variants, final, 1);
    }

    //if (isCancelledOrThrowIfKilled() || !impl)
    //    return {};

    if (blocks.empty())
        return Block();

    if (blocks.size() == 1)
    {
        Block res = blocks.back();
        blocks.clear();
        return res;
    }

    throw Exception("CudaAggregatingBlockInputStream::readImpl: blocks.size() is greater then 1", 
        ErrorCodes::LOGICAL_ERROR);
}


}
