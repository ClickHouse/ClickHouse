#include <DB/Storages/MergeTree/MergeTreePartChecker.h>
#include <DB/DataStreams/MarkInCompressedFile.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypeDate.h>
#include <DB/DataTypes/DataTypeDateTime.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeFixedString.h>
#include <DB/DataTypes/DataTypeAggregateFunction.h>
#include <DB/DataTypes/DataTypeArray.h>
#include <DB/DataTypes/DataTypeNested.h>
#include <DB/IO/CompressedReadBuffer.h>
#include <DB/IO/HashingReadBuffer.h>
#include <DB/Columns/ColumnsNumber.h>
#include <DB/Common/CurrentMetrics.h>
#include <DB/Common/escapeForFileName.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int CORRUPTED_DATA;
	extern const int INCORRECT_MARK;
	extern const int EMPTY_LIST_OF_COLUMNS_PASSED;
	extern const int UNKNOWN_TYPE;
}


namespace
{

struct Stream
{
	static const size_t UNKNOWN = std::numeric_limits<size_t>::max();

	bool data_type_is_string = false;
	size_t data_type_fixed_length = 0;
	String path;
	String name;

	ReadBufferFromFile file_buf;
	HashingReadBuffer compressed_hashing_buf;
	CompressedReadBuffer uncompressing_buf;
	HashingReadBuffer uncompressed_hashing_buf;

	ReadBufferFromFile mrk_file_buf;
	HashingReadBuffer mrk_hashing_buf;

	Stream(const String & path_, const String & name_, DataTypePtr type) : path(path_), name(name_),
		file_buf(path + name + ".bin"), compressed_hashing_buf(file_buf), uncompressing_buf(compressed_hashing_buf),
		uncompressed_hashing_buf(uncompressing_buf), mrk_file_buf(path + name + ".mrk"), mrk_hashing_buf(mrk_file_buf)
	{
		data_type_is_string = typeid_cast<const DataTypeString *>(type.get());

		if (!data_type_is_string)
		{
			if (typeid_cast<const DataTypeUInt8 *>(type.get())
				|| typeid_cast<const DataTypeInt8 *>(type.get()))
				data_type_fixed_length = sizeof(UInt8);
			else if (typeid_cast<const DataTypeUInt16 *>(type.get())
				|| typeid_cast<const DataTypeInt16 *>(type.get())
				|| typeid_cast<const DataTypeDate *>(type.get()))
				data_type_fixed_length = sizeof(UInt16);
			else if (typeid_cast<const DataTypeUInt32 *>(type.get())
				|| typeid_cast<const DataTypeInt32 *>(type.get())
				|| typeid_cast<const DataTypeFloat32 *>(type.get())
				|| typeid_cast<const DataTypeDateTime *>(type.get()))
				data_type_fixed_length = sizeof(UInt32);
			else if (typeid_cast<const DataTypeUInt64 *>(type.get())
				|| typeid_cast<const DataTypeInt64 *>(type.get())
				|| typeid_cast<const DataTypeFloat64 *>(type.get()))
				data_type_fixed_length = sizeof(UInt64);
			else if (auto string = typeid_cast<const DataTypeFixedString *>(type.get()))
				data_type_fixed_length = string->getN();
			else
				throw Exception("Unexpected data type: " + type->getName() + " of column " + name, ErrorCodes::UNKNOWN_TYPE);
		}
	}

	bool marksEOF()
	{
		return mrk_hashing_buf.eof();
	}

	void ignore()
	{
		uncompressed_hashing_buf.ignore(std::numeric_limits<size_t>::max());
		mrk_hashing_buf.ignore(std::numeric_limits<size_t>::max());
	}

	size_t read(size_t rows)
	{
		if (data_type_is_string)
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
			size_t size = uncompressed_hashing_buf.tryIgnore(data_type_fixed_length * rows);
			if (size % data_type_fixed_length)
				throw Exception("Read " + toString(size) + " bytes, which is not divisible by " + toString(data_type_fixed_length),
								ErrorCodes::CORRUPTED_DATA);
			return size / data_type_fixed_length;
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

	void assertMark()
	{
		MarkInCompressedFile mrk_mark;
		readIntBinary(mrk_mark.offset_in_compressed_file, mrk_hashing_buf);
		readIntBinary(mrk_mark.offset_in_decompressed_block, mrk_hashing_buf);

		bool has_alternative_mark = false;
		MarkInCompressedFile alternative_data_mark;
		MarkInCompressedFile data_mark;

		/// Если засечка должна быть ровно на границе блоков, нам подходит и засечка, указывающая на конец предыдущего блока,
		///  и на начало следующего.
		if (!uncompressed_hashing_buf.hasPendingData())
		{
			/// Получим засечку, указывающую на конец предыдущего блока.
			has_alternative_mark = true;
			alternative_data_mark.offset_in_compressed_file = compressed_hashing_buf.count() - uncompressing_buf.getSizeCompressed();
			alternative_data_mark.offset_in_decompressed_block = uncompressed_hashing_buf.offset();

			if (mrk_mark == alternative_data_mark)
				return;

			uncompressed_hashing_buf.next();

			/// В конце файла compressed_hashing_buf.count() указывает на конец файла даже до вызова next(),
			///  и только что выполненная проверка работает неправильно. Для простоты не будем проверять последнюю засечку.
			if (uncompressed_hashing_buf.eof())
				return;
		}

		data_mark.offset_in_compressed_file = compressed_hashing_buf.count() - uncompressing_buf.getSizeCompressed();
		data_mark.offset_in_decompressed_block = uncompressed_hashing_buf.offset();

		if (mrk_mark != data_mark)
			throw Exception("Incorrect mark: " + data_mark.toString() +
				(has_alternative_mark ? " or " + alternative_data_mark.toString() : "") + " in data, " +
				mrk_mark.toString() + " in .mrk file", ErrorCodes::INCORRECT_MARK);
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
static size_t checkColumn(
	const String & path,
	const String & name,
	DataTypePtr type,
	const MergeTreePartChecker::Settings & settings,
	MergeTreeData::DataPart::Checksums & checksums,
	volatile bool * is_cancelled)
{
	size_t rows = 0;

	try
	{
		if (auto array = typeid_cast<const DataTypeArray *>(type.get()))
		{
			String sizes_name = DataTypeNested::extractNestedTableName(name);
			Stream sizes_stream(path, escapeForFileName(sizes_name) + ".size0", new DataTypeUInt64);
			Stream data_stream(path, escapeForFileName(name), array->getNestedType());

			ColumnUInt64::Container_t sizes;
			while (true)
			{
				if (is_cancelled && *is_cancelled)
					return 0;

				if (sizes_stream.marksEOF())
					break;

				sizes_stream.assertMark();
				data_stream.assertMark();

				size_t cur_rows = sizes_stream.readUInt64(settings.index_granularity, sizes);

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
				if (cur_rows < settings.index_granularity)
					break;
			}

			sizes_stream.assertEnd(checksums);
			data_stream.assertEnd(checksums);

			return rows;
		}
		else if (typeid_cast<const DataTypeAggregateFunction *>(type.get()))
		{
			Stream data_stream(path, escapeForFileName(name), type);
			data_stream.ignore();
			return Stream::UNKNOWN;
		}
		else
		{
			Stream data_stream(path, escapeForFileName(name), type);

			size_t rows = 0;
			while (true)
			{
				if (is_cancelled && *is_cancelled)
					return 0;

				if (data_stream.marksEOF())
					break;

				data_stream.assertMark();

				size_t cur_rows = data_stream.read(settings.index_granularity);

				if (cur_rows == Stream::UNKNOWN)
					rows = Stream::UNKNOWN;
				else
					rows += cur_rows;
				if (cur_rows < settings.index_granularity)
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

}


void MergeTreePartChecker::checkDataPart(
	String path,
	const Settings & settings,
	const DataTypes & primary_key_data_types,
	MergeTreeData::DataPart::Checksums * out_checksums,
	volatile bool * is_cancelled)
{
	CurrentMetrics::Increment metric_increment{CurrentMetrics::ReplicatedChecks};

	if (!path.empty() && path.back() != '/')
		path += "/";

	NamesAndTypesList columns;

	/// Чексуммы из файла checksums.txt. Могут отсутствовать. Если присутствуют - впоследствии сравниваются с реальными чексуммами данных.
	MergeTreeData::DataPart::Checksums checksums_txt;

	{
		ReadBufferFromFile buf(path + "columns.txt");
		columns.readText(buf);
		assertEOF(buf);
	}

	if (settings.require_checksums || Poco::File(path + "checksums.txt").exists())
	{
		ReadBufferFromFile buf(path + "checksums.txt");
		checksums_txt.read(buf);
		assertEOF(buf);
	}

	/// Реальные чексуммы по содержимому данных. Их несоответствие checksums_txt будет говорить о битых данных.
	MergeTreeData::DataPart::Checksums checksums_data;

	size_t marks_in_primary_key = 0;
	{
		ReadBufferFromFile file_buf(path + "primary.idx");
		HashingReadBuffer hashing_buf(file_buf);

		if (!primary_key_data_types.empty())
		{
			size_t key_size = primary_key_data_types.size();
			Columns tmp_columns(key_size);

			for (size_t j = 0; j < key_size; ++j)
				tmp_columns[j] = primary_key_data_types[j].get()->createColumn();

			while (!hashing_buf.eof())
			{
				if (is_cancelled && *is_cancelled)
					return;

				++marks_in_primary_key;
				for (size_t j = 0; j < key_size; ++j)
					primary_key_data_types[j].get()->deserializeBinary(*tmp_columns[j].get(), hashing_buf);
			}
		}
		else
		{
			hashing_buf.tryIgnore(std::numeric_limits<size_t>::max());
		}

		size_t primary_idx_size = hashing_buf.count();

		checksums_data.files["primary.idx"] = MergeTreeData::DataPart::Checksums::Checksum(primary_idx_size, hashing_buf.getHash());
	}

	if (is_cancelled && *is_cancelled)
		return;

	String any_column_name;
	size_t rows = Stream::UNKNOWN;
	std::exception_ptr first_exception;

	for (const NameAndTypePair & column : columns)
	{
		if (settings.verbose)
		{
			std::cerr << column.name << ":";
			std::cerr.flush();
		}

		bool ok = false;
		try
		{
			if (!settings.require_column_files && !Poco::File(path + escapeForFileName(column.name) + ".bin").exists())
			{
				if (settings.verbose)
					std::cerr << " no files" << std::endl;
				continue;
			}

			size_t cur_rows = checkColumn(path, column.name, column.type, settings, checksums_data, is_cancelled);

			if (is_cancelled && *is_cancelled)
				return;

			if (cur_rows != Stream::UNKNOWN)
			{
				if (rows == Stream::UNKNOWN)
				{
					rows = cur_rows;
					any_column_name = column.name;
				}
				else if (rows != cur_rows)
				{
					throw Exception("Different number of rows in columns " + any_column_name + " and " + column.name,
									ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);
				}
			}

			ok = true;
		}
		catch (...)
		{
			if (!settings.verbose)
				throw;

			std::exception_ptr e = std::current_exception();
			if (!first_exception)
				first_exception = e;

			std::cerr << getCurrentExceptionMessage(true) << std::endl;
		}

		if (settings.verbose && ok)
			std::cerr << " ok" << std::endl;
	}

	if (rows == Stream::UNKNOWN)
		throw Exception("No columns", ErrorCodes::EMPTY_LIST_OF_COLUMNS_PASSED);

	if (!primary_key_data_types.empty())
	{
		const size_t expected_marks = (rows - 1) / settings.index_granularity + 1;
		if (expected_marks != marks_in_primary_key)
			throw Exception("Size of primary key doesn't match expected number of marks."
				" Number of rows in columns: " + toString(rows)
				+ ", index_granularity: " + toString(settings.index_granularity)
				+ ", expected number of marks: " + toString(expected_marks)
				+ ", size of primary key: " + toString(marks_in_primary_key),
				ErrorCodes::CORRUPTED_DATA);
	}

	if (settings.require_checksums || !checksums_txt.files.empty())
		checksums_txt.checkEqual(checksums_data, true);

	if (first_exception)
		std::rethrow_exception(first_exception);

	if (out_checksums)
		*out_checksums = checksums_data;
}

}
