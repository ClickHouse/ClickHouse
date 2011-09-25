#include <DB/DataTypes/DataTypeAggregateFunction.h>
#include <DB/Columns/ColumnAggregateFunction.h>

#include <DB/Interpreters/Aggregator.h>


namespace DB
{


/** Простой алгоритм (агрегация с помощью std::map).
  * Без оптимизации для агрегатных функций, принимающих не более одного значения.
  * Без оптимизации по количеству ключей.
  * Результат хранится в оперативке и должен полностью помещаться в оперативку.
  */
AggregatedData Aggregator::execute(BlockInputStreamPtr stream)
{
	AggregatedData res;

	size_t keys_size = keys.empty() ? key_names.size() : keys.size();
	size_t aggregates_size = aggregates.size();
	Row key(keys_size);
	Columns key_columns(keys_size);

	typedef std::vector<Columns> AggregateColumns;
	AggregateColumns aggregate_columns(aggregates_size);

	typedef std::vector<Row> Rows;
	Rows aggregate_arguments(aggregates_size);

	/// Читаем все данные
	while (Block block = stream->read())
	{
		/// Преобразуем имена столбцов в номера, если номера не заданы
		if (keys.empty() && !key_names.empty())
			for (Names::const_iterator it = key_names.begin(); it != key_names.end(); ++it)
				keys.push_back(block.getPositionByName(*it));

		for (AggregateDescriptions::iterator it = aggregates.begin(); it != aggregates.end(); ++it)
			if (it->arguments.empty() && !it->argument_names.empty())
				for (Names::const_iterator jt = it->argument_names.begin(); jt != it->argument_names.end(); ++jt)
					it->arguments.push_back(block.getPositionByName(*jt));

		for (size_t i = 0; i < aggregates_size; ++i)
		{
			aggregate_arguments[i].resize(aggregates[i].arguments.size());
			aggregate_columns[i].resize(aggregates[i].arguments.size());
		}
		
		/// Запоминаем столбцы, с которыми будем работать
		for (size_t i = 0, size = keys_size; i < size; ++i)
			key_columns[i] = block.getByPosition(keys[i]).column;

		for (size_t i = 0; i < aggregates_size; ++i)
			for (size_t j = 0; j < aggregate_columns[i].size(); ++j)
				aggregate_columns[i][j] = block.getByPosition(aggregates[i].arguments[j]).column;

		/// Создадим пример блока, описывающего результат
		if (!sample)
		{
			for (size_t i = 0, size = keys_size; i < size; ++i)
				sample.insert(block.getByPosition(keys[i]).cloneEmpty());

			for (size_t i = 0; i < aggregates_size; ++i)
			{
				ColumnWithNameAndType col;
				col.name = aggregates[i].column_name;
				col.type = new DataTypeAggregateFunction;
				col.column = new ColumnAggregateFunction;

				sample.insert(col);
			}
		}

		size_t rows = block.rows();

		/// Для всех строчек
		for (size_t i = 0; i < rows; ++i)
		{
			/// Строим ключ
			for (size_t j = 0; j < keys_size; ++j)
				key[j] = (*key_columns[j])[i];

			AggregatedData::iterator it = res.find(key);
			if (it == res.end())
			{
				it = res.insert(std::make_pair(key, AggregateFunctions(aggregates_size))).first;

				for (size_t j = 0; j < aggregates_size; ++j)
					it->second[j] = aggregates[j].function->cloneEmpty();
			}

			/// Добавляем значения
			for (size_t j = 0; j < aggregates_size; ++j)
			{
				for (size_t k = 0, size = aggregate_arguments[j].size(); k < size; ++k)
					aggregate_arguments[j][k] = (*aggregate_columns[j][k])[i];

				it->second[j]->add(aggregate_arguments[j]);
			}
		}
	}

	return res;
}


}
