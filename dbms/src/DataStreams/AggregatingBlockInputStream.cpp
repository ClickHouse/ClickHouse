#include <DB/Columns/ColumnsNumber.h>

#include <DB/DataStreams/AggregatingBlockInputStream.h>


namespace DB
{


Block AggregatingBlockInputStream::readImpl()
{
	if (!executed)
	{
		executed = true;
		AggregatedDataVariants data_variants;

		Aggregator::CancellationHook hook = [&]() { return this->isCancelled(); };
		aggregator.setCancellationHook(hook);

		aggregator.execute(children.back(), data_variants);
		blocks = aggregator.convertToBlocks(data_variants, final, 1);
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
