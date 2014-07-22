#include <DB/Storages/MergeTree/MergeTreePartChecker.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypeDate.h>
#include <DB/DataTypes/DataTypeDateTime.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeFixedString.h>
#include <DB/IO/CompressedReadBuffer.h>
#include <DB/IO/HashingReadBuffer.h>
#include <DB/Columns/ColumnsNumber.h>


namespace DB
{

struct Stream
{
	DataTypePtr type;
	String path;
	String name;

	ReadBufferFromFile file_buf;
	HashingReadBuffer compressed_hashing_buf;
	CompressedReadBuffer uncompressing_buf;
	HashingReadBuffer uncompressed_hashing_buf;

	ReadBufferFromFile mrk_file_buf;
	HashingReadBuffer mrk_hashing_buf;

	Stream(const String & path_, const String & name_, DataTypePtr type_) : type(type_), path(path_), name(name_),
		file_buf(path + name + ".bin"), compressed_hashing_buf(file_buf), uncompressing_buf(compressed_hashing_buf),
		uncompressed_hashing_buf(uncompressing_buf), mrk_file_buf(path + name + ".mrk"), mrk_hashing_buf(mrk_file_buf)
	{
		std::cerr << "hi: " << compressed_hashing_buf.count() << ' ' << uncompressed_hashing_buf.offset() << std::endl;
	}

	bool marksEOF()
	{
		return mrk_hashing_buf.eof();
	}

	size_t read(size_t rows)
	{
		if (dynamic_cast<const DataTypeString *>(&*type))
		{
			for (size_t i = 0; i < rows; ++i)
			{
				if (uncompressed_hashing_buf.eof())
					return i;

				UInt64 size;
				readVarUInt(size, uncompressed_hashing_buf);

				if (size > (1ul << 31))
					throw Exception("A string of length " + toString(size) + " is too long.", ErrorCodes::CORRUPTED_DATA);

				uncompressed_hashing_buf.ignore(size);
			}
			return rows;
		}
		else
		{
			size_t length;
			if(		dynamic_cast<const DataTypeUInt8 *>(&*type) ||
					dynamic_cast<const DataTypeInt8 *>(&*type))
				length = sizeof(UInt8);
			else if(dynamic_cast<const DataTypeUInt16 *>(&*type) ||
					dynamic_cast<const DataTypeInt16 *>(&*type) ||
					dynamic_cast<const DataTypeDate *>(&*type))
				length = sizeof(UInt16);
			else if(dynamic_cast<const DataTypeUInt32 *>(&*type) ||
					dynamic_cast<const DataTypeInt32 *>(&*type) ||
					dynamic_cast<const DataTypeFloat32 *>(&*type) ||
					dynamic_cast<const DataTypeDateTime *>(&*type))
				length = sizeof(UInt32);
			else if(dynamic_cast<const DataTypeUInt64 *>(&*type) ||
					dynamic_cast<const DataTypeInt64 *>(&*type) ||
					dynamic_cast<const DataTypeFloat64 *>(&*type))
				length = sizeof(UInt64);
			else if (auto string = dynamic_cast<const DataTypeFixedString *>(&*type))
				length = string->getN();
			else
				throw Exception("Unexpected data type: " + type->getName() + " of column " + name, ErrorCodes::UNKNOWN_TYPE);

			size_t size = uncompressed_hashing_buf.tryIgnore(length * rows);
			if (size % length)
				throw Exception("Read " + toString(size) + " bytes, which is not divisible by " + toString(length),
								ErrorCodes::CORRUPTED_DATA);
			return size / length;
		}
	}

	size_t readUInt64(size_t rows, ColumnUInt64::Container_t & data)
	{
		if (data.size() < rows)
			data.resize(rows);
		size_t size = uncompressed_hashing_buf.readBig(reinterpret_cast<char *>(&data[0]), sizeof(UInt64) * rows);
		if (size % sizeof(UInt64))
			throw Exception("Read " + toString(size) + " bytes, which is not divisible by " + toString(sizeof(UInt64)),
							ErrorCodes::CORRUPTED_DATA);
		return size / sizeof(UInt64);
	}

	void assertMark(bool strict)
	{
		MarkInCompressedFile mrk_mark;
		readIntBinary(mrk_mark.offset_in_compressed_file, mrk_hashing_buf);
		readIntBinary(mrk_mark.offset_in_decompressed_block, mrk_hashing_buf);

		MarkInCompressedFile data_mark;

		if (!strict)
		{
			/// Если засечка должна быть ровно на границе блоков, нам подходит и засечка, указывающая на конец предыдущего блока,
			///  и на начало следующего.
			data_mark.offset_in_compressed_file = compressed_hashing_buf.count();
			data_mark.offset_in_decompressed_block = uncompressed_hashing_buf.offset();

			if (mrk_mark == data_mark)
				return;
		}

		uncompressed_hashing_buf.nextIfAtEnd();

		data_mark.offset_in_compressed_file = compressed_hashing_buf.count();
		data_mark.offset_in_decompressed_block = uncompressed_hashing_buf.offset();

		if (mrk_mark != data_mark)
			throw Exception("Incorrect mark: " + data_mark.toString() + " in data, " + mrk_mark.toString() + " in .mrk file",
							ErrorCodes::INCORRECT_MARK);
	}

	void assertEnd(MergeTreeData::DataPart::Checksums & checksums)
	{
		if (!uncompressed_hashing_buf.eof())
			throw Exception("EOF expected in column data", ErrorCodes::CORRUPTED_DATA);
		if (!mrk_hashing_buf.eof())
			throw Exception("EOF expected in .mrk file", ErrorCodes::CORRUPTED_DATA);

		checksums.files[name + ".bin"] = MergeTreeData::DataPart::Checksums::Checksum(
			compressed_hashing_buf.count(), compressed_hashing_buf.getHash(),
			uncompressed_hashing_buf.count(), uncompressed_hashing_buf.getHash());
		checksums.files[name + ".mrk"] = MergeTreeData::DataPart::Checksums::Checksum(
			mrk_hashing_buf.count(), mrk_hashing_buf.getHash());
	}
};

/// Возвращает количество строк. Добавляет в checksums чексуммы всех файлов столбца.
static size_t checkColumn(const String & path, const String & name, DataTypePtr type, size_t index_granularity, bool strict,
						  MergeTreeData::DataPart::Checksums & checksums)
{
	size_t rows = 0;

	try
	{
		if (auto array = dynamic_cast<const DataTypeArray *>(&*type))
		{
			String sizes_name = DataTypeNested::extractNestedTableName(name);
			Stream sizes_stream(path, escapeForFileName(sizes_name) + ".size0", new DataTypeUInt64);
			Stream data_stream(path, escapeForFileName(name), array->getNestedType());

			ColumnUInt64::Container_t sizes;
			while (true)
			{
				if (sizes_stream.marksEOF())
					break;

				sizes_stream.assertMark(strict);
				data_stream.assertMark(strict);

				size_t cur_rows = sizes_stream.readUInt64(index_granularity, sizes);

				size_t sum = 0;
				for (size_t i = 0; i < cur_rows; ++i)
				{
					size_t new_sum = sum + sizes[i];
					if (sizes[i] > (1ul << 31) || new_sum < sum)
						throw Exception("Array size " + toString(sizes[i]) + " is too long.", ErrorCodes::CORRUPTED_DATA);
					sum = new_sum;
				}

				data_stream.read(sum);

				rows += cur_rows;
				if (cur_rows < index_granularity)
					break;
			}

			sizes_stream.assertEnd(checksums);
			data_stream.assertEnd(checksums);

			return rows;
		}
		else
		{
			Stream data_stream(path, escapeForFileName(name), type);

			size_t rows = 0;
			while (true)
			{
				if (data_stream.marksEOF())
					break;

				data_stream.assertMark(strict);

				size_t cur_rows = data_stream.read(index_granularity);

				rows += cur_rows;
				if (cur_rows < index_granularity)
					break;
			}

			data_stream.assertEnd(checksums);

			return rows;
		}
	}
	catch (DB::Exception & e)
	{
		e.addMessage(" (column: " + path + name + ", last mark at " + toString(rows) + " rows)");
		throw;
	}
}

void MergeTreePartChecker::checkDataPart(String path, size_t index_granularity, bool strict, const DataTypeFactory & data_type_factory)
{
	if (!path.empty() && *path.rbegin() != '/')
		path += "/";

	NamesAndTypesList columns;
	MergeTreeData::DataPart::Checksums checksums_txt;

	{
		ReadBufferFromFile buf(path + "columns.txt");
		columns.readText(buf, data_type_factory);
		assertEOF(buf);
	}

	if (Poco::File(path + "checksums.txt").exists())
	{
		ReadBufferFromFile buf(path + "checksums.txt");
		checksums_txt.readText(buf);
		assertEOF(buf);
	}

	MergeTreeData::DataPart::Checksums checksums_data;

	bool first = true;
	size_t rows = 0;
	for (const NameAndTypePair & column : columns)
	{
		if (!strict && !Poco::File(path + escapeForFileName(column.name) + ".bin").exists())
			continue;

		size_t cur_rows = checkColumn(path, column.name, column.type, index_granularity, strict, checksums_data);
		if (first)
		{
			rows = cur_rows;
			first = false;
		}
		else if (rows != cur_rows)
		{
			throw Exception("Different number of rows in columns " + columns.begin()->name + " and " + column.name,
							ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);
		}

		std::cerr << "column " << column.name << " ok" << std::endl;
	}

	if (first)
		throw Exception("No columns", ErrorCodes::EMPTY_LIST_OF_COLUMNS_PASSED);

	checksums_txt.checkEqual(checksums_data, true);
}

}
