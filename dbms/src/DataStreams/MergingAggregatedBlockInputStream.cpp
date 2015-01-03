#include <DB/Columns/ColumnsNumber.h>

#include <DB/DataStreams/MergingAggregatedBlockInputStream.h>


namespace DB
{


Block MergingAggregatedBlockInputStream::readImpl()
{
	if (!executed)
	{
		executed = true;
		AggregatedDataVariants data_variants;
		aggregator->mergeStream(children.back(), data_variants, max_threads);
		blocks = aggregator->convertToBlocks(data_variants, final, max_threads);
		it = blocks.begin();
	}

	Block res;
	if (isCancelled() || it == blocks.end())
		return res;

	res = *it;
	++it;

	return res;
}


}
