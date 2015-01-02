#include <DB/Columns/ColumnsNumber.h>

#include <DB/DataStreams/AggregatingBlockInputStream.h>


namespace DB
{


AggregatingBlockInputStream::AggregatingBlockInputStream(BlockInputStreamPtr input_,
	const Names & key_names, const AggregateDescriptions & aggregates,
	bool overflow_row_, bool final_, size_t max_rows_to_group_by_, OverflowMode group_by_overflow_mode_)
	: final(final_)
{
	children.push_back(input_);

	aggregator = new Aggregator(key_names, aggregates, overflow_row_, max_rows_to_group_by_, group_by_overflow_mode_);
}



Block AggregatingBlockInputStream::readImpl()
{
	if (!executed)
	{
		executed = true;
		AggregatedDataVariants data_variants;
		aggregator->execute(children.back(), data_variants);
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
