#include <map>

#include <DB/Common/escapeForFileName.h>

#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>

#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteHelpers.h>

#include <DB/DataTypes/DataTypeArray.h>

#include <DB/Columns/ColumnArray.h>

#include <DB/Storages/StorageLog.h>


#define DBMS_STORAGE_LOG_DATA_FILE_EXTENSION 	".bin"
#define DBMS_STORAGE_LOG_MARKS_FILE_EXTENSION 	".mrk"


namespace DB
{

using Poco::SharedPtr;


LogBlockInputStream::LogBlockInputStream(size_t block_size_, const Names & column_names_, StorageLog & storage_, size_t mark_number_, size_t rows_limit_)
	: block_size(block_size_), column_names(column_names_), storage(storage_), mark_number(mark_number_), rows_limit(rows_limit_), rows_read(0)
{
}


Block LogBlockInputStream::readImpl()
{
	Block res;

	if (rows_read == rows_limit)
		return res;

	/// Если файлы не открыты, то открываем их.
	if (streams.empty())
		for (Names::const_iterator it = column_names.begin(); it != column_names.end(); ++it)
			addStream(*it, *storage.getDataTypeByName(*it));

	/// Сколько строк читать для следующего блока.
	size_t max_rows_to_read = std::min(block_size, rows_limit - rows_read);

	for (Names::const_iterator it = column_names.begin(); it != column_names.end(); ++it)
	{
		ColumnWithNameAndType column;
		column.name = *it;
		column.type = storage.getDataTypeByName(*it);
		column.column = column.type->createColumn();
		readData(*it, *column.type, *column.column, max_rows_to_read);

		if (column.column->size())
			res.insert(column);
	}

	if (res)
		rows_read += res.rows();

	if (!res || rows_read == rows_limit)
	{
		/** Закрываем файлы (ещё до уничтожения объекта).
		  * Чтобы при создании многих источников, но одновременном чтении только из нескольких,
		  *  буферы не висели в памяти.
		  */
		streams.clear();
	}
		
	return res;
}


void LogBlockInputStream::addStream(const String & name, const IDataType & type, size_t level)
{
	/// Для массивов используются отдельные потоки для размеров.
	if (const DataTypeArray * type_arr = dynamic_cast<const DataTypeArray *>(&type))
	{
		String size_name = name + ARRAY_SIZES_COLUMN_NAME_SUFFIX + Poco::NumberFormatter::format(level);
		streams.insert(std::make_pair(size_name, new Stream(
			storage.files[size_name].data_file.path(),
			storage.files[size_name].marks[mark_number].offset)));

		addStream(name, *type_arr->getNestedType(), level + 1);
	}
	else
		streams.insert(std::make_pair(name, new Stream(
			storage.files[name].data_file.path(),
			storage.files[name].marks[mark_number].offset)));
}


void LogBlockInputStream::readData(const String & name, const IDataType & type, IColumn & column, size_t max_rows_to_read, size_t level)
{
	/// Для массивов требуется сначала десериализовать размеры, а потом значения.
	if (const DataTypeArray * type_arr = dynamic_cast<const DataTypeArray *>(&type))
	{
		type_arr->deserializeOffsets(
			column,
			streams[name + ARRAY_SIZES_COLUMN_NAME_SUFFIX + Poco::NumberFormatter::format(level)]->compressed,
			max_rows_to_read);

		if (column.size())
			readData(
				name,
				*type_arr->getNestedType(),
				dynamic_cast<ColumnArray &>(column).getData(),
				dynamic_cast<const ColumnArray &>(column).getOffsets()[column.size() - 1],
				level + 1);
	}
	else
		type.deserializeBinary(column, streams[name]->compressed, max_rows_to_read);
}


LogBlockOutputStream::LogBlockOutputStream(StorageLog & storage_)
	: storage(storage_)
{
	for (NamesAndTypesList::const_iterator it = storage.columns->begin(); it != storage.columns->end(); ++it)
		addStream(it->first, *it->second);
}


void LogBlockOutputStream::write(const Block & block)
{
	storage.check(block);

	for (size_t i = 0; i < block.columns(); ++i)
	{
		const ColumnWithNameAndType & column = block.getByPosition(i);
		writeData(column.name, *column.type, *column.column);
	}
}


void LogBlockOutputStream::addStream(const String & name, const IDataType & type, size_t level)
{
	/// Для массивов используются отдельные потоки для размеров.
	if (const DataTypeArray * type_arr = dynamic_cast<const DataTypeArray *>(&type))
	{
		String size_name = name + ARRAY_SIZES_COLUMN_NAME_SUFFIX + Poco::NumberFormatter::format(level);
		streams.insert(std::make_pair(size_name, new Stream(
			storage.files[size_name].data_file.path(),
			storage.files[size_name].marks_file.path())));

		addStream(name, *type_arr->getNestedType(), level + 1);
	}
	else
		streams.insert(std::make_pair(name, new Stream(
			storage.files[name].data_file.path(),
			storage.files[name].marks_file.path())));
}


void LogBlockOutputStream::writeData(const String & name, const IDataType & type, const IColumn & column, size_t level)
{
	/// Для массивов требуется сначала сериализовать размеры, а потом значения.
	if (const DataTypeArray * type_arr = dynamic_cast<const DataTypeArray *>(&type))
	{
		String size_name = name + ARRAY_SIZES_COLUMN_NAME_SUFFIX + Poco::NumberFormatter::format(level);

		Mark mark;
		mark.rows = (storage.files[size_name].marks.empty() ? 0 : storage.files[size_name].marks.back().rows) + column.size();
		mark.offset = streams[size_name]->plain_offset + streams[size_name]->plain.count();

		writeIntBinary(mark.rows, streams[size_name]->marks);
		writeIntBinary(mark.offset, streams[size_name]->marks);

		storage.files[size_name].marks.push_back(mark);

		type_arr->serializeOffsets(column, streams[size_name]->compressed);
		streams[size_name]->compressed.next();

		writeData(name, *type_arr->getNestedType(), dynamic_cast<const ColumnArray &>(column).getData(), level + 1);
	}
	else
	{
		Mark mark;
		mark.rows = (storage.files[name].marks.empty() ? 0 : storage.files[name].marks.back().rows) + column.size();
		mark.offset = streams[name]->plain_offset + streams[name]->plain.count();

		writeIntBinary(mark.rows, streams[name]->marks);
		writeIntBinary(mark.offset, streams[name]->marks);

		storage.files[name].marks.push_back(mark);

		type.serializeBinary(column, streams[name]->compressed);
		streams[name]->compressed.next();
	}
}


StorageLog::StorageLog(const std::string & path_, const std::string & name_, NamesAndTypesListPtr columns_)
	: path(path_), name(name_), columns(columns_), loaded_marks(false)
{
	if (columns->empty())
		throw Exception("Empty list of columns passed to StorageLog constructor", ErrorCodes::EMPTY_LIST_OF_COLUMNS_PASSED);
	
	/// создаём файлы, если их нет
	Poco::File(path + escapeForFileName(name) + '/').createDirectories();

	for (NamesAndTypesList::const_iterator it = columns->begin(); it != columns->end(); ++it)
		addFile(it->first, *it->second);
}


void StorageLog::addFile(const String & column_name, const IDataType & type, size_t level)
{
	if (files.end() != files.find(column_name))
		throw Exception("Duplicate column with name " + column_name + " in constructor of StorageTinyLog.",
			ErrorCodes::DUPLICATE_COLUMN);

	if (const DataTypeArray * type_arr = dynamic_cast<const DataTypeArray *>(&type))
	{
		String size_column_suffix = ARRAY_SIZES_COLUMN_NAME_SUFFIX + Poco::NumberFormatter::format(level);

		ColumnData column_data;
		files.insert(std::make_pair(column_name + size_column_suffix, column_data));
		files[column_name + size_column_suffix].data_file = Poco::File(
			path + escapeForFileName(name) + '/' + escapeForFileName(column_name) + size_column_suffix + DBMS_STORAGE_LOG_DATA_FILE_EXTENSION);
		files[column_name + size_column_suffix].marks_file = Poco::File(
			path + escapeForFileName(name) + '/' + escapeForFileName(column_name) + size_column_suffix + DBMS_STORAGE_LOG_MARKS_FILE_EXTENSION);

		addFile(column_name, *type_arr->getNestedType(), level + 1);
	}
	else
	{
		ColumnData column_data;
		files.insert(std::make_pair(column_name, column_data));
		files[column_name].data_file = Poco::File(
			path + escapeForFileName(name) + '/' + escapeForFileName(column_name) + DBMS_STORAGE_LOG_DATA_FILE_EXTENSION);
		files[column_name].marks_file = Poco::File(
			path + escapeForFileName(name) + '/' + escapeForFileName(column_name) + DBMS_STORAGE_LOG_MARKS_FILE_EXTENSION);
	}
}


void StorageLog::loadMarks()
{
	if (loaded_marks)
		return;
	
	ssize_t size_of_marks_file = -1;
	for (Files_t::iterator it = files.begin(); it != files.end(); ++it)
	{
		/// Считаем засечки
		if (it->second.marks_file.exists())
		{
			ssize_t size_of_current_marks_file = it->second.marks_file.getSize();

			if (size_of_current_marks_file % sizeof(Mark) != 0)
				throw Exception("Sizes of marks files are inconsistent", ErrorCodes::SIZES_OF_MARKS_FILES_ARE_INCONSISTENT);

			if (-1 == size_of_marks_file)
				size_of_marks_file = size_of_current_marks_file;
			else if (size_of_marks_file != size_of_current_marks_file)
				throw Exception("Sizes of marks files are inconsistent", ErrorCodes::SIZES_OF_MARKS_FILES_ARE_INCONSISTENT);

			it->second.marks.reserve(it->second.marks_file.getSize() / sizeof(Mark));
			ReadBufferFromFile marks_rb(it->second.marks_file.path(), 32768);
			while (!marks_rb.eof())
			{
				Mark mark;
				readIntBinary(mark.rows, marks_rb);
				readIntBinary(mark.offset, marks_rb);
				it->second.marks.push_back(mark);
			}
		}
	}

	loaded_marks = true;
}


void StorageLog::rename(const String & new_path_to_db, const String & new_name)
{
	/// Переименовываем директорию с данными.
	Poco::File(path + escapeForFileName(name)).renameTo(new_path_to_db + escapeForFileName(new_name));
	
	path = new_path_to_db;
	name = new_name;

	for (Files_t::iterator it = files.begin(); it != files.end(); ++it)
	{
		it->second.data_file = Poco::File(path + escapeForFileName(name) + '/' + escapeForFileName(it->first) + DBMS_STORAGE_LOG_DATA_FILE_EXTENSION);
		it->second.marks_file = Poco::File(path + escapeForFileName(name) + '/' + escapeForFileName(it->first) + DBMS_STORAGE_LOG_MARKS_FILE_EXTENSION);
	}
}


BlockInputStreams StorageLog::read(
	const Names & column_names,
	ASTPtr query,
	QueryProcessingStage::Enum & processed_stage,
	size_t max_block_size,
	unsigned threads)
{
	loadMarks();
	check(column_names);
	processed_stage = QueryProcessingStage::FetchColumns;

	Marks marks = files.begin()->second.marks;
	size_t marks_size = marks.size();

	if (threads > marks_size)
		threads = marks_size;

	BlockInputStreams res;

	for (size_t thread = 0; thread < threads; ++thread)
	{
/*		std::cerr << "Thread " << thread << ", mark " << thread * marks_size / max_threads
			<< ", rows " << (thread == 0
				? marks[marks_size / max_threads - 1].rows
				: (marks[(thread + 1) * marks_size / max_threads - 1].rows - marks[thread * marks_size / max_threads - 1].rows)) << std::endl;*/
		
		res.push_back(new LogBlockInputStream(
			max_block_size,
			column_names,
			*this,
			thread * marks_size / threads,
			thread == 0
				? marks[marks_size / threads - 1].rows
				: (marks[(thread + 1) * marks_size / threads - 1].rows - marks[thread * marks_size / threads - 1].rows)));
	}
	
	return res;
}

	
BlockOutputStreamPtr StorageLog::write(
	ASTPtr query)
{
	loadMarks();
	return new LogBlockOutputStream(*this);
}


void StorageLog::drop()
{
	for (Files_t::iterator it = files.begin(); it != files.end(); ++it)
	{
		if (it->second.data_file.exists())
			it->second.data_file.remove();

		if (it->second.marks_file.exists())
			it->second.marks_file.remove();
	}
}

}
