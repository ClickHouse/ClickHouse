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
	}

	String getName() const override { return "Materializing"; }

	String getID() const override
	{
		std::stringstream res;
		res << "Materializing(" << children.back()->getID() << ")";
		return res.str();
	}

protected:
	Block readImpl() override
	{
		Block res = children.back()->read();

		if (!res)
			return res;

		size_t columns = res.columns();
		for (size_t i = 0; i < columns; ++i)
		{
			auto & src = res.getByPosition(i).column;
			ColumnPtr converted = src->convertToFullColumnIfConst();
			if (converted)
				src = converted;
		}

		return res;
	}
};

}
