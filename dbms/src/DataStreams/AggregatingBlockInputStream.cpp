#include <DB/DataStreams/AggregatingBlockInputStream.h>


namespace DB
{


AggregatingBlockInputStream::AggregatingBlockInputStream(BlockInputStreamPtr input_, SharedPtr<Expression> expression)
	: input(input_), has_been_read(false)
{
	children.push_back(input);

	Names key_names;
	AggregateDescriptions aggregates;
	expression->getAggregateInfo(key_names, aggregates);
	aggregator = new Aggregator(key_names, aggregates);
}



Block AggregatingBlockInputStream::readImpl()
{
	if (has_been_read)
		return Block();

	has_been_read = true;
	
	AggregatedDataVariants data_variants;
	aggregator->execute(input, data_variants);
	Block res = aggregator->getSampleBlock();
	size_t rows = 0;

	/// В какой структуре данных, агрегированы данные?
	if (!data_variants.without_key.empty())
	{
		AggregatedDataWithoutKey & data = data_variants.without_key;
		rows = 1;

		size_t i = 0;
		for (AggregateFunctions::const_iterator jt = data.begin(); jt != data.end(); ++jt, ++i)
			res.getByPosition(i).column->insert(*jt);
	}
	else if (!data_variants.key64.empty())
	{
		AggregatedDataWithUInt64Key & data = data_variants.key64;
		rows = data.size();
		IColumn & first_column = *res.getByPosition(0).column;
		for (AggregatedDataWithUInt64Key::const_iterator it = data.begin(); it != data.end(); ++it)
		{
			first_column.insert(it->first);

			size_t i = 1;
			for (AggregateFunctions::const_iterator jt = it->second.begin(); jt != it->second.end(); ++jt, ++i)
				res.getByPosition(i).column->insert(*jt);
		}
	}
	else if (!data_variants.hashed.empty())
	{
		AggregatedDataHashed & data = data_variants.hashed;
		rows = data.size();
		for (AggregatedDataHashed::const_iterator it = data.begin(); it != data.end(); ++it)
		{
			size_t i = 0;
			for (Row::const_iterator jt = it->second.first.begin(); jt != it->second.first.end(); ++jt, ++i)
				res.getByPosition(i).column->insert(*jt);

			for (AggregateFunctions::const_iterator jt = it->second.second.begin(); jt != it->second.second.end(); ++jt, ++i)
				res.getByPosition(i).column->insert(*jt);
		}
	}
	else
	{
		AggregatedData & data = data_variants.generic;
		rows = data.size();
		for (AggregatedData::const_iterator it = data.begin(); it != data.end(); ++it)
		{
			size_t i = 0;
			for (Row::const_iterator jt = it->first.begin(); jt != it->first.end(); ++jt, ++i)
				res.getByPosition(i).column->insert(*jt);

			for (AggregateFunctions::const_iterator jt = it->second.begin(); jt != it->second.end(); ++jt, ++i)
				res.getByPosition(i).column->insert(*jt);
		}
	}

	/// Изменяем размер столбцов-констант в блоке.
	size_t columns = res.columns();
	for (size_t i = 0; i < columns; ++i)
		if (res.getByPosition(i).column->isConst())
			res.getByPosition(i).column->cut(0, rows);

	return res;
}


}
