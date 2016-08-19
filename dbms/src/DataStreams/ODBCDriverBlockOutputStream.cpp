#include <DB/DataStreams/ODBCDriverBlockOutputStream.h>

namespace DB
{

ODBCDriverBlockOutputStream::ODBCDriverBlockOutputStream(WriteBuffer & out_)
	: out(out_) {}

void ODBCDriverBlockOutputStream::flush()
{
	out.next();
}

void ODBCDriverBlockOutputStream::write(const Block & block)
{
	size_t rows = block.rows();
	size_t columns = block.columns();

	/// Заголовок.
	if (is_first)
	{
		is_first = false;

		/// Количество столбцов.
		writeVarUInt(columns, out);

		/// Имена и типы столбцов.
		for (size_t j = 0; j < columns; ++j)
		{
			const ColumnWithTypeAndName & col = block.unsafeGetByPosition(j);

			writeStringBinary(col.name, out);
			writeStringBinary(col.type->getName(), out);
		}
	}

	String text_value;
	for (size_t i = 0; i < rows; ++i)
	{
		for (size_t j = 0; j < columns; ++j)
		{
			text_value.resize(0);
			const ColumnWithTypeAndName & col = block.unsafeGetByPosition(j);

			{
				WriteBufferFromString text_out(text_value);
				col.type->serializeText(*col.column.get(), i, text_out);
			}

			writeStringBinary(text_value, out);
		}
	}
}

}
