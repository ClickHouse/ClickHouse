#include <DB/IO/ReadHelpers.h>
#include <DB/IO/VarInt.h>

#include <DB/DataStreams/NativeBlockInputStream.h>


namespace DB
{

Block NativeBlockInputStream::readImpl()
{
	Block res;

	if (istr.eof())
		return res;
	
	/// Размеры
	size_t columns = 0;
	size_t rows = 0;
	readVarUInt(columns, istr);
	readVarUInt(rows, istr);

	std::cerr << "columns: " << columns << ", rows: " << rows << std::endl;

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
		column.type->deserializeBinary(*column.column, istr, rows);

		if (column.column->size() != rows)
			throw Exception("Cannot read all data in NativeBlockInputStream.", ErrorCodes::CANNOT_READ_ALL_DATA);

		res.insert(column);
	}

	return res;
}

}
