#pragma once

#include <DB/DataStreams/IBlockOutputStream.h>

namespace DB
{

/** Преобразует столбцы-константы в полноценные столбцы ("материализует" их).
  */
class MaterializingBlockOutputStream : public IBlockOutputStream
{
public:
	MaterializingBlockOutputStream(const BlockOutputStreamPtr & output);
	void write(const Block & block) override;
	void flush();
	void writePrefix();
	void writeSuffix();
	void setRowsBeforeLimit(size_t rows_before_limit);
	void setTotals(const Block & totals);
	void setExtremes(const Block & extremes);
	String getContentType() const;

private:
	static Block materialize(const Block & original_block);

private:
	BlockOutputStreamPtr output;
};

}
