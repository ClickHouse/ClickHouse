#include <DB/Core/Defines.h>

#include <DB/IO/ReadHelpers.h>
#include <DB/IO/VarInt.h>

#include <DB/Columns/ColumnArray.h>
#include <DB/DataTypes/DataTypeArray.h>

#include <DB/DataStreams/NativeBlockInputStream.h>


namespace DB
{


void NativeBlockInputStream::readData(const IDataType & type, IColumn & column, ReadBuffer & istr, size_t rows)
{
	/** Для массивов требуется сначала десериализовать смещения, а потом значения.
	  */
	if (const DataTypeArray * type_arr = typeid_cast<const DataTypeArray *>(&type))
	{
		IColumn & offsets_column = *typeid_cast<ColumnArray &>(column).getOffsetsColumn();
		type_arr->getOffsetsType()->deserializeBinary(offsets_column, istr, rows, 0);

		if (offsets_column.size() != rows)
			throw Exception("Cannot read all data in NativeBlockInputStream.", ErrorCodes::CANNOT_READ_ALL_DATA);

		if (rows)
			readData(
				*type_arr->getNestedType(),
				typeid_cast<ColumnArray &>(column).getData(),
				istr,
				typeid_cast<const ColumnArray &>(column).getOffsets()[rows - 1]);
	}
	else
		type.deserializeBinary(column, istr, rows, 0);	/// TODO Использовать avg_value_size_hint.

	if (column.size() != rows)
		throw Exception("Cannot read all data in NativeBlockInputStream.", ErrorCodes::CANNOT_READ_ALL_DATA);
}


Block NativeBlockInputStream::readImpl()
{
	Block res;

	if (istr.eof())
		return res;

	/// Дополнительная информация о блоке.
	if (server_revision >= DBMS_MIN_REVISION_WITH_BLOCK_INFO)
		res.info.read(istr);

	/// Размеры
	size_t columns = 0;
	size_t rows = 0;
	readVarUInt(columns, istr);
	readVarUInt(rows, istr);

	for (size_t i = 0; i < columns; ++i)
	{
		ColumnWithNameAndType column;

		/// Имя
		readStringBinary(column.name, istr);

		/// Тип
		String type_name;
		readStringBinary(type_name, istr);
		column.type = data_type_factory.get(type_name);

		/// Данные
		column.column = column.type->createColumn();
		readData(*column.type, *column.column, istr, rows);

		res.insert(column);
	}

	return res;
}

}
