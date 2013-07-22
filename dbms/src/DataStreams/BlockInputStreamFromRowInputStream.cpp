#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>

#include <DB/DataStreams/BlockInputStreamFromRowInputStream.h>
#include <DB/DataTypes/DataTypeNested.h>
#include <DB/Columns/ColumnArray.h>


namespace DB
{

using Poco::SharedPtr;


BlockInputStreamFromRowInputStream::BlockInputStreamFromRowInputStream(
	RowInputStreamPtr row_input_,
	const Block & sample_,
	size_t max_block_size_)
	: row_input(row_input_), sample(sample_), max_block_size(max_block_size_), total_rows(0)
{
}


Block BlockInputStreamFromRowInputStream::readImpl()
{
	Block res;

	try
	{
		for (size_t rows = 0; rows < max_block_size; ++rows, ++total_rows)
		{
			if (total_rows == 0)
				row_input->readRowBetweenDelimiter();

			Row row;
			bool has_row = row_input->read(row);

			if (!has_row)
				break;

			if (!res)
				res = sample.cloneEmpty();

			if (row.size() != sample.columns())
				throw Exception("Number of columns doesn't match", ErrorCodes::NUMBER_OF_COLUMNS_DOESNT_MATCH);

			for (size_t i = 0; i < row.size(); ++i)
				res.getByPosition(i).column->insert(row[i]);
		}
	}
	catch (const DB::Exception & e)
	{
		throw DB::Exception(e.message() + " (at row " + toString(total_rows + 1) + ")", e, e.code());
	}
	
	/// Указатели на столбцы-массивы, для проверки равенства столбцов смещений во вложенных структурах данных
	typedef std::map<String, ColumnArray *> ArrayColumns;
	ArrayColumns array_columns;
	
	for (size_t i = 0; i < res.columns(); ++i)
	{
		ColumnWithNameAndType & column = res.getByPosition(i);
		
		if (ColumnArray * column_array = dynamic_cast<ColumnArray *>(&*column.column))
		{
			String name = DataTypeNested::extractNestedTableName(column.name);
			
			ArrayColumns::const_iterator it = array_columns.find(name);
			if (array_columns.end() == it)
				array_columns[name] = column_array;
			else
			{
				if (!it->second->hasEqualOffsets(*column_array))
					throw Exception("Sizes of nested arrays do not match", ErrorCodes::SIZES_OF_ARRAYS_DOESNT_MATCH);
				
				/// делаем так, чтобы столбцы смещений массивов внутри одной вложенной таблицы указывали в одно место
				column_array->getOffsetsColumn() = it->second->getOffsetsColumn();
			}
		}
	}

	return res;
}

}
