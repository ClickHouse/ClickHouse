#pragma once

#include <DB/DataStreams/PrettyBlockOutputStream.h>


namespace DB
{

/** Выводит результат в виде красивых таблиц, но с меньшим количеством строк-разделителей.
  */
class PrettyCompactBlockOutputStream : public PrettyBlockOutputStream
{
public:
	PrettyCompactBlockOutputStream(WriteBuffer & ostr_, bool no_escapes_ = false, size_t max_rows_ = PRETTY_FORMAT_DEFAULT_MAX_ROWS)
		: PrettyBlockOutputStream(ostr_, no_escapes_, max_rows_) {}

	void write(const Block & block) override;

protected:
	void writeHeader(const Block & block, const Widths_t & max_widths, const Widths_t & name_widths);
	void writeBottom(const Widths_t & max_widths);
	void writeRow(size_t row_id, const Block & block, const Widths_t & max_widths, const Widths_t & name_widths);
};

}
