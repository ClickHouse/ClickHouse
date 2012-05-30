#include <map>

#include <DB/Common/escapeForFileName.h>

#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>

#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteHelpers.h>

#include <DB/Storages/StorageLog.h>


#define DBMS_STORAGE_LOG_DATA_FILE_EXTENSION 	".bin"
#define DBMS_STORAGE_LOG_MARKS_FILE_EXTENSION 	".mrk"


namespace DB
{

using Poco::SharedPtr;


LogBlockInputStream::LogBlockInputStream(size_t block_size_, const Names & column_names_, StorageLog & storage_, size_t mark_number_, size_t rows_limit_)
	: block_size(block_size_), column_names(column_names_), storage(storage_), mark_number(mark_number_), rows_limit(rows_limit_), rows_read(0)
{
	for (Names::const_iterator it = column_names.begin(); it != column_names.end(); ++it)
		streams.insert(std::make_pair(*it, new Stream(storage.files[*it].data_file.path(), storage.files[*it].marks[mark_number].offset)));
}


Block LogBlockInputStream::readImpl()
{
	Block res;

	for (Names::const_iterator it = column_names.begin(); it != column_names.end(); ++it)
	{
		ColumnWithNameAndType column;
		column.name = *it;
		column.type = storage.getDataTypeByName(*it);
		column.column = column.type->createColumn();
		column.type->deserializeBinary(*column.column, streams[column.name]->compressed, std::min(block_size, rows_limit - rows_read));

		if (column.column->size())
			res.insert(column);
	}

	if (res)
		rows_read += res.getByPosition(0).column->size();

	return res;
}


LogBlockOutputStream::LogBlockOutputStream(StorageLog & storage_)
	: storage(storage_)
{
	for (NamesAndTypesList::const_iterator it = storage.columns->begin(); it != storage.columns->end(); ++it)
		streams.insert(std::make_pair(it->first, new Stream(storage.files[it->first].data_file.path(), storage.files[it->first].marks_file.path())));
}


void LogBlockOutputStream::write(const Block & block)
{
	storage.check(block);

	for (size_t i = 0; i < block.columns(); ++i)
	{
		const ColumnWithNameAndType & column = block.getByPosition(i);

		Mark mark;
		mark.rows = (storage.files[column.name].marks.empty() ? 0 : storage.files[column.name].marks.back().rows) + column.column->size();
		mark.offset = streams[column.name]->plain.count();

		writeIntBinary(mark.rows, streams[column.name]->marks);
		writeIntBinary(mark.offset, streams[column.name]->marks);
		
		storage.files[column.name].marks.push_back(mark);
		
		column.type->serializeBinary(*column.column, streams[column.name]->compressed);
		streams[column.name]->compressed.next();
	}
}


StorageLog::StorageLog(const std::string & path_, const std::string & name_, NamesAndTypesListPtr columns_)
	: path(path_), name(name_), columns(columns_)
{
	if (columns->empty())
		throw Exception("Empty list of columns passed to StorageLog constructor", ErrorCodes::EMPTY_LIST_OF_COLUMNS_PASSED);
	
	/// создаём файлы, если их нет
	Poco::File(path + escapeForFileName(name) + '/').createDirectories();

	ssize_t size_of_marks_file = -1;
	for (NamesAndTypesList::const_iterator it = columns->begin(); it != columns->end(); ++it)
	{
		if (files.end() != files.find(it->first))
			throw Exception("Duplicate column with name " + it->first + " in constructor of StorageLog.",
				ErrorCodes::DUPLICATE_COLUMN);

		ColumnData column_data;
		files.insert(std::make_pair(it->first, column_data));
		files[it->first].data_file = Poco::File(path + escapeForFileName(name) + '/' + escapeForFileName(it->first) + DBMS_STORAGE_LOG_DATA_FILE_EXTENSION);
		files[it->first].marks_file = Poco::File(path + escapeForFileName(name) + '/' + escapeForFileName(it->first) + DBMS_STORAGE_LOG_MARKS_FILE_EXTENSION);

		/// Считаем засечки
		if (files[it->first].marks_file.exists())
		{
			ssize_t size_of_current_marks_file = files[it->first].marks_file.getSize();

			if (size_of_current_marks_file % sizeof(Mark) != 0)
				throw Exception("Sizes of marks files are inconsistent", ErrorCodes::SIZES_OF_MARKS_FILES_ARE_INCONSISTENT);
			
			if (-1 == size_of_marks_file)
				size_of_marks_file = size_of_current_marks_file;
			else if (size_of_marks_file != size_of_current_marks_file)
				throw Exception("Sizes of marks files are inconsistent", ErrorCodes::SIZES_OF_MARKS_FILES_ARE_INCONSISTENT);
			
			files[it->first].marks.reserve(files[it->first].marks_file.getSize() / sizeof(Mark));
			ReadBufferFromFile marks_rb(files[it->first].marks_file.path());
			while (!marks_rb.eof())
			{
				Mark mark;
				readIntBinary(mark.rows, marks_rb);
				readIntBinary(mark.offset, marks_rb);
				files[it->first].marks.push_back(mark);
			}
		}
	}
}


BlockInputStreams StorageLog::read(
	const Names & column_names,
	ASTPtr query,
	QueryProcessingStage::Enum & processed_stage,
	size_t max_block_size,
	unsigned threads)
{
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
	return new LogBlockOutputStream(*this);
}


void StorageLog::drop()
{
	for (Files_t::iterator it = files.begin(); it != files.end(); ++it)
	{
		it->second.data_file.remove();
		it->second.marks_file.remove();
	}
}

}
