#pragma once

#include <DB/Columns/ColumnConst.h>

#include <DB/DataStreams/IProfilingBlockInputStream.h>


namespace DB
{

/** Преобразует столбцы-константы в полноценные столбцы ("материализует" их).
  */
class MaterializingBlockInputStream : public IProfilingBlockInputStream
{
public:
	MaterializingBlockInputStream(BlockInputStreamPtr input_)
		: input(input_)
	{
		children.push_back(input);
	}

	Block readImpl()
	{
		Block res = input->read();

		if (!res)
			return res;

		size_t columns = res.columns();
		for (size_t i = 0; i < columns; ++i)
		{
			ColumnPtr col = res.getByPosition(i).column;
			if (col->isConst())
				res.getByPosition(i).column = dynamic_cast<IColumnConst &>(*col).convertToFullColumn();
		}

		return res;
	}

	String getName() const { return "MaterializingBlockInputStream"; }

	BlockInputStreamPtr clone() { return new MaterializingBlockInputStream(input); }

private:
	BlockInputStreamPtr input;
};

}
