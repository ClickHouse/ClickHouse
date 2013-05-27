#include <DB/Columns/ColumnsNumber.h>

#include <DB/DataStreams/MergingAggregatedBlockInputStream.h>


namespace DB
{


Block MergingAggregatedBlockInputStream::readImpl()
{
	if (has_been_read)
		return Block();

	has_been_read = true;
	
	AggregatedDataVariants data_variants;
	aggregator->merge(children.back(), data_variants);
	return aggregator->convertToBlock(data_variants);
}


}
