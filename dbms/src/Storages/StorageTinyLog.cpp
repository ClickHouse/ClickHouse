#include <map>

#include <DB/Common/escapeForFileName.h>

#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>

#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteHelpers.h>

#include <DB/Storages/StorageTinyLog.h>


#define DBMS_STORAGE_LOG_DATA_FILE_EXTENSION 	".bin"


namespace DB
{

using Poco::SharedPtr;


TinyLogBlockInputStream::TinyLogBlockInputStream(size_t block_size_, const Names & column_names_, StorageTinyLog & storage_)
	: block_size(block_size_), column_names(column_names_), storage(storage_)
{
}


Block TinyLogBlockInputStream::readImpl()
{
	Block res;

	/// Если файлы не открыты, то открываем их.
	if (streams.empty())
	{
		for (Names::const_iterator it = column_names.begin(); it != column_names.end(); ++it)
			streams.insert(std::make_pair(*it, new Stream(storage.files[*it].data_file.path())));
	}
	else if (streams[0]->compressed.eof())
		return res;

	for (Names::const_iterator it = column_names.begin(); it != column_names.end(); ++it)
	{
		ColumnWithNameAndType column;
		column.name = *it;
		column.type = storage.getDataTypeByName(*it);
		column.column = column.type->createColumn();
		column.type->deserializeBinary(*column.column, streams[column.name]->compressed, block_size);

		if (column.column->size())
			res.insert(column);
	}

	if (!res || streams[0]->compressed.eof())
	{
		/** Закрываем файлы (ещё до уничтожения объекта).
		  * Чтобы при создании многих источников, но одновременном чтении только из нескольких,
		  *  буферы не висели в памяти.
		  */
		streams.clear();
	}

	return res;
}


TinyLogBlockOutputStream::TinyLogBlockOutputStream(StorageTinyLog & storage_)
	: storage(storage_)
{
	for (NamesAndTypesList::const_iterator it = storage.columns->begin(); it != storage.columns->end(); ++it)
		streams.insert(std::make_pair(it->first, new Stream(storage.files[it->first].data_file.path())));
}


void TinyLogBlockOutputStream::write(const Block & block)
{
	storage.check(block);

	for (size_t i = 0; i < block.columns(); ++i)
	{
		const ColumnWithNameAndType & column = block.getByPosition(i);
		column.type->serializeBinary(*column.column, streams[column.name]->compressed);
	}
}


StorageTinyLog::StorageTinyLog(const std::string & path_, const std::string & name_, NamesAndTypesListPtr columns_)
	: path(path_), name(name_), columns(columns_)
{
	if (columns->empty())
		throw Exception("Empty list of columns passed to StorageTinyLog constructor", ErrorCodes::EMPTY_LIST_OF_COLUMNS_PASSED);
	
	/// создаём файлы, если их нет
	Poco::File(path + escapeForFileName(name) + '/').createDirectories();

	for (NamesAndTypesList::const_iterator it = columns->begin(); it != columns->end(); ++it)
	{
		if (files.end() != files.find(it->first))
			throw Exception("Duplicate column with name " + it->first + " in constructor of StorageTinyLog.",
				ErrorCodes::DUPLICATE_COLUMN);

		ColumnData column_data;
		files.insert(std::make_pair(it->first, column_data));
		files[it->first].data_file = Poco::File(path + escapeForFileName(name) + '/' + escapeForFileName(it->first) + DBMS_STORAGE_LOG_DATA_FILE_EXTENSION);
	}
}


void StorageTinyLog::rename(const String & new_path_to_db, const String & new_name)
{
	/// Переименовываем директорию с данными.
	Poco::File(path + escapeForFileName(name)).renameTo(new_path_to_db + escapeForFileName(new_name));
	
	path = new_path_to_db;
	name = new_name;

	for (NamesAndTypesList::const_iterator it = columns->begin(); it != columns->end(); ++it)
		files[it->first].data_file = Poco::File(path + escapeForFileName(name) + '/' + escapeForFileName(it->first) + DBMS_STORAGE_LOG_DATA_FILE_EXTENSION);
}


BlockInputStreams StorageTinyLog::read(
	const Names & column_names,
	ASTPtr query,
	QueryProcessingStage::Enum & processed_stage,
	size_t max_block_size,
	unsigned threads)
{
	check(column_names);
	processed_stage = QueryProcessingStage::FetchColumns;
	return BlockInputStreams(1, new TinyLogBlockInputStream(max_block_size, column_names, *this));
}

	
BlockOutputStreamPtr StorageTinyLog::write(
	ASTPtr query)
{
	return new TinyLogBlockOutputStream(*this);
}


void StorageTinyLog::drop()
{
	for (Files_t::iterator it = files.begin(); it != files.end(); ++it)
		if (it->second.data_file.exists())
			it->second.data_file.remove();
}

}
