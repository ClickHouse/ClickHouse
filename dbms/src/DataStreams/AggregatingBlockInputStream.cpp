#include <DB/Columns/ColumnsNumber.h>

#include <DB/DataStreams/AggregatingBlockInputStream.h>


namespace DB
{


	AggregatingBlockInputStream::AggregatingBlockInputStream(BlockInputStreamPtr input_,
		const Names & key_names, const AggregateDescriptions & aggregates,
		bool with_totals_, size_t max_rows_to_group_by_, Limits::OverflowMode group_by_overflow_mode_)
	: has_been_read(false)
{
	children.push_back(input_);

	aggregator = new Aggregator(key_names, aggregates, with_totals_, max_rows_to_group_by_, group_by_overflow_mode_);
}



Block AggregatingBlockInputStream::readImpl()
{
	if (has_been_read)
		return Block();

	has_been_read = true;

	AggregatedDataVariants data_variants;
	aggregator->execute(children.back(), data_variants);

	if (isCancelled())
		return Block();
		
	return aggregator->convertToBlock(data_variants);
}


}
