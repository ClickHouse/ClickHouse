#include <DB/Columns/ColumnsNumber.h>

#include <DB/DataStreams/AggregatingBlockInputStream.h>


namespace DB
{


AggregatingBlockInputStream::AggregatingBlockInputStream(BlockInputStreamPtr input_, ExpressionPtr expression,
	size_t max_rows_to_group_by_, Limits::OverflowMode group_by_overflow_mode_)
	: has_been_read(false)
{
	children.push_back(input_);
	input = &*children.back();

	Names key_names;
	AggregateDescriptions aggregates;
	expression->getAggregateInfo(key_names, aggregates);
	aggregator = new Aggregator(key_names, aggregates, max_rows_to_group_by_, group_by_overflow_mode_);
}



Block AggregatingBlockInputStream::readImpl()
{
	if (has_been_read)
		return Block();

	has_been_read = true;
	
	AggregatedDataVariants data_variants;
	aggregator->execute(input, data_variants);

	if (isCancelled())
		return Block();
		
	return aggregator->convertToBlock(data_variants);
}


}
