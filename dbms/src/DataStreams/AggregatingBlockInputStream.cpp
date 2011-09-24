#include <DB/DataStreams/AggregatingBlockInputStream.h>


namespace DB
{


/*AggregatingBlockInputStream::AggregatingBlockInputStream(BlockInputStreamPtr input_, SharedPtr<Expression> expression)
	: input(input_), has_been_read(false)
{
	children.push_back(input);

	
}*/



Block AggregatingBlockInputStream::readImpl()
{
	if (has_been_read)
		return Block();

	has_been_read = true;
	
	AggregatedData data = aggregator.execute(input);
	Block res = aggregator.getSampleBlock();

	for (AggregatedData::const_iterator it = data.begin(); it != data.end(); ++it)
	{
		size_t i = 0;
		for (Row::const_iterator jt = it->first.begin(); jt != it->first.end(); ++jt, ++i)
			res.getByPosition(i).column->insert(*jt);

		for (AggregateFunctions::const_iterator jt = it->second.begin(); jt != it->second.end(); ++jt, ++i)
			res.getByPosition(i).column->insert(*jt);
	}

	return res;
}


}
