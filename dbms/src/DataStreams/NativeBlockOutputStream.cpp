#include <DB/Core/Defines.h>

#include <DB/IO/WriteHelpers.h>
#include <DB/IO/VarInt.h>
#include <DB/IO/CompressedWriteBuffer.h>

#include <DB/Columns/ColumnConst.h>
#include <DB/Columns/ColumnArray.h>

#include <DB/DataTypes/DataTypeArray.h>

#include <DB/DataStreams/MarkInCompressedFile.h>
#include <DB/DataStreams/NativeBlockOutputStream.h>


namespace DB
{


NativeBlockOutputStream::NativeBlockOutputStream(
	WriteBuffer & ostr_, UInt64 client_revision_,
	WriteBuffer * index_ostr_, size_t initial_size_of_file_)
	: ostr(ostr_), client_revision(client_revision_),
	index_ostr(index_ostr_), initial_size_of_file(initial_size_of_file_)
{
	if (index_ostr)
	{
		ostr_concrete = typeid_cast<CompressedWriteBuffer *>(&ostr);
		if (!ostr_concrete)
			throw Exception("When need to write index for NativeBlockOutputStream, ostr must be CompressedWriteBuffer.", ErrorCodes::LOGICAL_ERROR);
	}
}


void NativeBlockOutputStream::writeData(const IDataType & type, const ColumnPtr & column, WriteBuffer & ostr, size_t offset, size_t limit)
{
	/** Если есть столбцы-константы - то материализуем их.
	  * (Так как тип данных не умеет сериализовывать/десериализовывать константы.)
	  */
	ColumnPtr full_column;

	if (auto converted = column->convertToFullColumnIfConst())
		full_column = converted;
	else
		full_column = column;

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

	/** Индекс имеет ту же структуру, что и поток с данными.
	  * Но вместо значений столбца он содержит засечку, ссылающуюся на место в файле с данными, где находится этот кусочек столбца.
	  */
	if (index_ostr)
	{
		writeVarUInt(columns, *index_ostr);
		writeVarUInt(rows, *index_ostr);
	}

	for (size_t i = 0; i < columns; ++i)
	{
		/// Для индекса.
		MarkInCompressedFile mark;

		if (index_ostr)
		{
			ostr_concrete->next();	/// Заканчиваем сжатый блок.
			mark.offset_in_compressed_file = initial_size_of_file + ostr_concrete->getCompressedBytes();
			mark.offset_in_decompressed_block = ostr_concrete->getRemainingBytes();
		}

		const ColumnWithTypeAndName & column = block.getByPosition(i);

		/// Имя
		writeStringBinary(column.name, ostr);

		/// Тип
		writeStringBinary(column.type->getName(), ostr);

		/// Данные
		writeData(*column.type, column.column, ostr, 0, 0);

		if (index_ostr)
		{
			writeStringBinary(column.name, *index_ostr);
			writeStringBinary(column.type->getName(), *index_ostr);

			writeBinary(mark.offset_in_compressed_file, *index_ostr);
			writeBinary(mark.offset_in_decompressed_block, *index_ostr);
		}
	}
}

}
