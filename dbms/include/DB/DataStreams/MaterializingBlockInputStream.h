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
	{
		children.push_back(input_);
		input = &*children.back();
	}

	String getName() const { return "MaterializingBlockInputStream"; }

	String getID() const
	{
		std::stringstream res;
		res << "Materializing(" << input->getID() << ")";
		return res.str();
	}

protected:
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

private:
	IBlockInputStream * input;
};

}
