#include <DB/Core/Defines.h>

#include <DB/IO/WriteHelpers.h>
#include <DB/IO/VarInt.h>

#include <DB/Columns/ColumnConst.h>
#include <DB/Columns/ColumnArray.h>

#include <DB/DataTypes/DataTypeArray.h>

#include <DB/DataStreams/NativeBlockOutputStream.h>


namespace DB
{


void NativeBlockOutputStream::writeData(const IDataType & type, const ColumnPtr & column, WriteBuffer & ostr, size_t offset, size_t limit)
{
	/** Если есть столбцы-константы - то материализуем их.
	  * (Так как тип данных не умеет сериализовывать/десериализовывать константы.)
	  */
	ColumnPtr full_column = column->isConst()
		? static_cast<const IColumnConst &>(*column).convertToFullColumn()
		: column;

	/** Для массивов требуется сначала сериализовать смещения, а потом значения.
	  */
	if (const DataTypeArray * type_arr = typeid_cast<const DataTypeArray *>(&type))
	{
		const ColumnArray & column_array = typeid_cast<const ColumnArray &>(*full_column);
		type_arr->getOffsetsType()->serializeBinary(*column_array.getOffsetsColumn(), ostr, offset, limit);

		if (!typeid_cast<const ColumnArray &>(*full_column).getData().empty())
		{
			const ColumnArray::Offsets_t & offsets = column_array.getOffsets();

			if (offset > offsets.size())
				return;

			/** offset - с какого массива писать.
			  * limit - сколько массивов максимум записать, или 0, если писать всё, что есть.
			  * end - до какого массива заканчивается записываемый кусок.
			  *
			  * nested_offset - с какого элемента внутренностей писать.
			  * nested_limit - сколько элементов внутренностей писать, или 0, если писать всё, что есть.
			  */

			size_t end = std::min(offset + limit, offsets.size());

			size_t nested_offset = offset ? offsets[offset - 1] : 0;
			size_t nested_limit = limit
				? offsets[end - 1] - nested_offset
				: 0;

			if (limit == 0 || nested_limit)
				writeData(*type_arr->getNestedType(), typeid_cast<const ColumnArray &>(*full_column).getDataPtr(), ostr, nested_offset, nested_limit);
		}
	}
	else
		type.serializeBinary(*full_column, ostr, offset, limit);
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
		writeData(*column.type, column.column, ostr, 0, 0);
	}
}

}
