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
		aggregator->merge(children.back(), data_variants);
		blocks = aggregator->convertToBlocks(data_variants, final, 1);
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
