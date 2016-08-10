#include <sys/ioctl.h>
#include <unistd.h>

#include <DB/DataStreams/PrettyCompactBlockOutputStream.h>
#include <DB/DataTypes/NullSymbol.h>
#include <DB/Columns/ColumnNullable.h>


namespace DB
{

namespace ErrorCodes
{

extern const int ILLEGAL_COLUMN;

}

void PrettyCompactBlockOutputStream::writeHeader(
	const Block & block,
	const Widths_t & max_widths,
	const Widths_t & name_widths)
{
	/// Имена
	writeCString("┌─", ostr);
	for (size_t i = 0; i < max_widths.size(); ++i)
	{
		if (i != 0)
			writeCString("─┬─", ostr);

		const ColumnWithTypeAndName & col = block.getByPosition(i);

		if (col.type->isNumeric())
		{
			for (size_t k = 0; k < max_widths[i] - name_widths[i]; ++k)
				writeCString("─", ostr);

			if (!no_escapes)
				writeCString("\033[1m", ostr);
			writeEscapedString(col.name, ostr);
			if (!no_escapes)
				writeCString("\033[0m", ostr);
		}
		else
		{
			if (!no_escapes)
				writeCString("\033[1m", ostr);
			writeEscapedString(col.name, ostr);
			if (!no_escapes)
				writeCString("\033[0m", ostr);

			for (size_t k = 0; k < max_widths[i] - name_widths[i]; ++k)
				writeCString("─", ostr);
		}
	}
	writeCString("─┐\n", ostr);
}

void PrettyCompactBlockOutputStream::writeBottom(const Widths_t & max_widths)
{
	/// Создадим разделители
	std::stringstream bottom_separator;

	bottom_separator 		<< "└";
	for (size_t i = 0; i < max_widths.size(); ++i)
	{
		if (i != 0)
			bottom_separator 		<< "┴";

		for (size_t j = 0; j < max_widths[i] + 2; ++j)
			bottom_separator 		<< "─";
	}
	bottom_separator 		<< "┘\n";

	writeString(bottom_separator.str(), ostr);
}

void PrettyCompactBlockOutputStream::writeRow(
	size_t row_id,
	const Block & block,
	const Widths_t & max_widths,
	const Widths_t & name_widths)
{
	auto has_null_value = [](const ColumnPtr & col, size_t row)
	{
		if (col.get()->isNullable())
		{
			const ColumnNullable & nullable_col = static_cast<const ColumnNullable &>(*col);
			if (nullable_col.isNullAt(row))
				return true;
		}
		else if (col.get()->isNull())
			return true;

		return false;
	};

	size_t columns = max_widths.size();

	writeCString("│ ", ostr);

	for (size_t j = 0; j < columns; ++j)
	{
		if (j != 0)
			writeCString(" │ ", ostr);

		const ColumnWithTypeAndName & col = block.getByPosition(j);

		size_t width;

		if (has_null_value(col.column, row_id))
			width = NullSymbol::Escaped::length;
		else
		{
			ColumnPtr res_col = block.getByPosition(columns + j).column;

			IColumn * observed_col;
			if (res_col.get()->isNullable())
			{
				ColumnNullable & nullable_col = static_cast<ColumnNullable &>(*res_col);
				observed_col = nullable_col.getNestedColumn().get();
			}
			else
				observed_col = res_col.get();

			if (const ColumnUInt64 * concrete_col = typeid_cast<const ColumnUInt64 *>(observed_col))
			{
				const ColumnUInt64::Container_t & res = concrete_col->getData();
				width = res[row_id];
			}
			else if (const ColumnConstUInt64 * concrete_col = typeid_cast<const ColumnConstUInt64 *>(observed_col))
			{
				UInt64 res = concrete_col->getData();
				width = res;
			}
			else
				throw Exception{"Illegal column " + observed_col->getName(), ErrorCodes::ILLEGAL_COLUMN};
		}

		if (col.type->isNumeric())
		{
			for (size_t k = 0; k < max_widths[j] - width; ++k)
				writeChar(' ', ostr);
			col.type->serializeTextEscaped(*col.column.get(), row_id, ostr);
		}
		else
		{
			col.type->serializeTextEscaped(*col.column.get(), row_id, ostr);
			for (size_t k = 0; k < max_widths[j] - width; ++k)
				writeChar(' ', ostr);
		}
	}

	writeCString(" │\n", ostr);
}

void PrettyCompactBlockOutputStream::write(const Block & block_)
{
	if (total_rows >= max_rows)
	{
		total_rows += block_.rows();
		return;
	}

	/// Будем вставлять сюда столбцы с вычисленными значениями видимых длин.
	Block block = block_;

	size_t rows = block.rows();

	Widths_t max_widths;
	Widths_t name_widths;
	calculateWidths(block, max_widths, name_widths);

	writeHeader(block, max_widths, name_widths);

	for (size_t i = 0; i < rows && total_rows + i < max_rows; ++i)
		writeRow(i, block, max_widths, name_widths);

	writeBottom(max_widths);

	total_rows += rows;
}

}
