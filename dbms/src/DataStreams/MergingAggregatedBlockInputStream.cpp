#include <DB/Columns/ColumnsNumber.h>

#include <DB/DataStreams/MergingAggregatedBlockInputStream.h>


namespace DB
{


MergingAggregatedBlockInputStream::MergingAggregatedBlockInputStream(BlockInputStreamPtr input_, ExpressionPtr expression)
	: input(input_), has_been_read(false)
{
	children.push_back(input);

	Names key_names;
	AggregateDescriptions aggregates;
	expression->getAggregateInfo(key_names, aggregates);
	aggregator = new Aggregator(key_names, aggregates);
}



Block MergingAggregatedBlockInputStream::readImpl()
{
	if (has_been_read)
		return Block();

	has_been_read = true;
	
	AggregatedDataVariants data_variants;
	aggregator->merge(input, data_variants);
	return aggregator->convertToBlock(data_variants);
}


}
