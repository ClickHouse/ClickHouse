#include <DB/Core/Defines.h>

#include <DB/IO/WriteHelpers.h>
#include <DB/IO/VarInt.h>

#include <DB/Columns/ColumnConst.h>
#include <DB/Columns/ColumnArray.h>

#include <DB/DataTypes/DataTypeArray.h>

#include <DB/DataStreams/NativeBlockOutputStream.h>


namespace DB
{


static void writeData(const IDataType & type, const IColumn & column, WriteBuffer & ostr)
{
	/** Для массивов требуется сначала сериализовать смещения, а потом значения.
	  */
	if (const DataTypeArray * type_arr = dynamic_cast<const DataTypeArray *>(&type))
	{
		type_arr->getOffsetsType()->serializeBinary(*dynamic_cast<const ColumnArray &>(column).getOffsetsColumn(), ostr);
		writeData(*type_arr->getNestedType(), dynamic_cast<const ColumnArray &>(column).getData(), ostr);
	}
	else
		type.serializeBinary(column, ostr);
}


void NativeBlockOutputStream::write(const Block & block)
{
	/// Размеры
	size_t columns = block.columns();
	size_t rows = block.rows();
	writeVarUInt(columns, ostr);
	writeVarUInt(rows, ostr);

	/** Если есть столбцы-константы - то материализуем их.
	  * (Так как тип данных не умеет сериализовывать/десериализовывать константы.)
	  */
	Block materialized_block = block;

	for (size_t i = 0; i < columns; ++i)
	{
		ColumnWithNameAndType & column = materialized_block.getByPosition(i);
		if (column.column->isConst())
			column.column = dynamic_cast<const IColumnConst &>(*column.column).convertToFullColumn();

		/// Имя
		writeStringBinary(column.name, ostr);

		/// Тип
		writeStringBinary(column.type->getName(), ostr);

		/// Данные
		writeData(*column.type, *column.column, ostr);
	}
}

}
