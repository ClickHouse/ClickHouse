#pragma once

#include <DB/Columns/ColumnConst.h>
#include <DB/DataStreams/IBlockOutputStream.h>
#include <statdaemons/ext/range.hpp>


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
			ColumnPtr & col = block.getByPosition(i).column;
			if (col->isConst())
				col = dynamic_cast<IColumnConst &>(*col).convertToFullColumn();
		}

		output->write(block);
	}

	void flush() override { output->flush(); }

	void writePrefix() override { output->writePrefix(); }
	void writeSuffix() override { output->writeSuffix(); }

private:
	BlockOutputStreamPtr output;
};

}
