#include <DB/IO/WriteHelpers.h>
#include <DB/IO/VarInt.h>

#include <DB/DataStreams/NativeBlockOutputStream.h>


namespace DB
{

void NativeBlockOutputStream::write(const Block & block)
{
	/// Размеры
	size_t columns = block.columns();
	size_t rows = block.rows();
	writeVarUInt(columns, ostr);
	writeVarUInt(rows, ostr);

	for (size_t i = 0; i < columns; ++i)
	{
		const ColumnWithNameAndType & column = block.getByPosition(i);
		
		/// Имя
		writeStringBinary(column.name, ostr);

		/// Тип
		writeStringBinary(column.type->getName(), ostr);

		/// Данные
		column.type->serializeBinary(*column.column, ostr);
	}
}

}
