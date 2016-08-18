#pragma once

#include <DB/DataStreams/IBlockOutputStream.h>

#define PRETTY_FORMAT_DEFAULT_MAX_ROWS 10000


namespace DB
{

class WriteBuffer;


/** Выводит результат в виде красивых таблиц.
  */
class PrettyBlockOutputStream : public IBlockOutputStream
{
public:
	/// no_escapes - не использовать ANSI escape sequences - для отображения в браузере, а не в консоли.
	PrettyBlockOutputStream(WriteBuffer & ostr_, bool no_escapes_ = false, size_t max_rows_ = PRETTY_FORMAT_DEFAULT_MAX_ROWS);

	void write(const Block & block) override;
	void writeSuffix() override;

	void flush() override;

	void setTotals(const Block & totals_) override { totals = totals_; }
	void setExtremes(const Block & extremes_) override { extremes = extremes_; }

protected:
	void writeTotals();
	void writeExtremes();

	using Widths_t = std::vector<size_t>;

	/// Вычислить видимую (при выводе на консоль с кодировкой UTF-8) ширину значений и имён столбцов.
	void calculateWidths(Block & block, Widths_t & max_widths, Widths_t & name_widths);

	WriteBuffer & ostr;
	size_t max_rows;
	size_t total_rows;
	size_t terminal_width;

	bool no_escapes;

	Block totals;
	Block extremes;
};

}
