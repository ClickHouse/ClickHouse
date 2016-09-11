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
	void flush() override;
	void writePrefix() override;
	void writeSuffix() override;
	void setRowsBeforeLimit(size_t rows_before_limit) override;
	void setTotals(const Block & totals) override;
	void setExtremes(const Block & extremes) override;
	String getContentType() const override;

private:
	static Block materialize(const Block & original_block);

private:
	BlockOutputStreamPtr output;
};

}
