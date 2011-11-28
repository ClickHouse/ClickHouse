#pragma once

#include <DB/DataStreams/IBlockOutputStream.h>

#define PRETTY_FORMAT_DEFAULT_MAX_ROWS 1000


namespace DB
{

/** Выводит результат в виде красивых таблиц.
  */
class PrettyBlockOutputStream : public IBlockOutputStream
{
public:
	PrettyBlockOutputStream(WriteBuffer & ostr_, size_t max_rows_ = PRETTY_FORMAT_DEFAULT_MAX_ROWS);
	void write(const Block & block);
	void writeSuffix();
	BlockOutputStreamPtr clone() { return new PrettyBlockOutputStream(ostr); }

protected:
	typedef std::vector<size_t> Widths_t;

	/// Вычислить видимую (при выводе на консоль с кодировкой UTF-8) ширину значений и имён столбцов.
	void calculateWidths(Block & block, Widths_t & max_widths, Widths_t & name_widths);
	
	WriteBuffer & ostr;
	size_t max_rows;
	size_t total_rows;
	size_t terminal_width;
};

}
