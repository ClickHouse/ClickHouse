#pragma once

#include <DB/Columns/ColumnAggregateFunction.h>

#include <DB/DataStreams/IProfilingBlockInputStream.h>


namespace DB
{

using Poco::SharedPtr;


/** Преобразует агрегатные функции (с промежуточным состоянием) в потоке блоков в конечные значения.
  */
class FinalizingAggregatedBlockInputStream : public IProfilingBlockInputStream
{
public:
	FinalizingAggregatedBlockInputStream(BlockInputStreamPtr input_)
		: input(input_)
	{
		children.push_back(input);
	}

	String getName() const { return "FinalizingAggregatedBlockInputStream"; }

	BlockInputStreamPtr clone() { return new FinalizingAggregatedBlockInputStream(input); }

protected:
	Block readImpl()
	{
		Block res = input->read();

		if (!res)
			return res;

		size_t rows = res.rows();
		size_t columns = res.columns();
		for (size_t i = 0; i < columns; ++i)
		{
			ColumnWithNameAndType & column = res.getByPosition(i);
			if (ColumnAggregateFunction * col = dynamic_cast<ColumnAggregateFunction *>(&*column.column))
			{
				ColumnAggregateFunction::Container_t & data = col->getData();
				column.type = data[0]->getReturnType();
				ColumnPtr finalized_column = column.type->createColumn();

				finalized_column->reserve(rows, rows * 32);
				for (size_t j = 0; j < rows; ++j)
					finalized_column->insert(data[j]->getResult());

				column.column = finalized_column;
			}
		}

		return res;
	}

private:
	BlockInputStreamPtr input;
};

}
