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
	if (const DataTypeArray * type_arr = typeid_cast<const DataTypeArray *>(&type))
	{
		type_arr->getOffsetsType()->serializeBinary(*typeid_cast<const ColumnArray &>(column).getOffsetsColumn(), ostr);

		if (!typeid_cast<const ColumnArray &>(column).getData().empty())
			writeData(*type_arr->getNestedType(), typeid_cast<const ColumnArray &>(column).getData(), ostr);
	}
	else
		type.serializeBinary(column, ostr);
}


void NativeBlockOutputStream::write(const Block & block)
{
	/// Дополнительная информация о блоке.
	if (client_revision >= DBMS_MIN_REVISION_WITH_BLOCK_INFO)
		block.info.write(ostr);

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

		/** Если есть столбцы-константы - то материализуем их.
		  * (Так как тип данных не умеет сериализовывать/десериализовывать константы.)
		  */
		ColumnPtr col = column.column->isConst()
			? static_cast<const IColumnConst &>(*column.column).convertToFullColumn()
			: column.column;

		writeData(*column.type, *col, ostr);
	}
}

}
