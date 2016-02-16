#pragma once

#include <DB/Columns/ColumnConst.h>
#include <DB/DataStreams/IBlockOutputStream.h>
#include <ext/range.hpp>


namespace DB
{

/** Преобразует столбцы-константы в полноценные столбцы ("материализует" их).
  */
class MaterializingBlockOutputStream : public IBlockOutputStream
{
public:
	MaterializingBlockOutputStream(const BlockOutputStreamPtr & output)
		: output{output} {}

	void write(const Block & original_block) override
	{
		/// copy block to get rid of const
		auto block = original_block;

		for (const auto i : ext::range(0, block.columns()))
		{
			auto & src = block.getByPosition(i).column;
			ColumnPtr converted = src->convertToFullColumnIfConst();
			if (converted)
				src = converted;
		}

		output->write(block);
	}

	void flush() 										override { output->flush(); }
	void writePrefix() 									override { output->writePrefix(); }
	void writeSuffix() 									override { output->writeSuffix(); }
	void setRowsBeforeLimit(size_t rows_before_limit) 	override { output->setRowsBeforeLimit(rows_before_limit); }
	void setTotals(const Block & totals) 				override { output->setTotals(totals); }
	void setExtremes(const Block & extremes) 			override { output->setExtremes(extremes); }
	String getContentType() const 						override { return output->getContentType(); }

private:
	BlockOutputStreamPtr output;
};

}
