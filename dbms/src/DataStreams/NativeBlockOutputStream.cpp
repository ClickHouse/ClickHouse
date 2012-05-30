#include <DB/IO/WriteHelpers.h>
#include <DB/IO/VarInt.h>

#include <DB/Columns/ColumnConst.h>

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

		std::cerr << "write: " << column.type->getName() << std::endl;

		/// Данные
		if (column.column->isConst())
		{
			/** Перед сериализацией константного столбца, материализуем его.
			  * Так как тип данных не умеет сериализовывать/десериализовывать константы.
			  */ 
			ColumnPtr materialized = dynamic_cast<const IColumnConst &>(*column.column).convertToFullColumn();
			column.type->serializeBinary(*materialized, ostr);
		}
		else
			column.type->serializeBinary(*column.column, ostr);

		std::cerr << "write: done" << std::endl;
	}
}

}
