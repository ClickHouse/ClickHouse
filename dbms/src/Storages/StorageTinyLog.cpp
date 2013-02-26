#include <map>
#include <Poco/Path.h>

#include <DB/Common/escapeForFileName.h>

#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>

#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteHelpers.h>

#include <DB/DataTypes/DataTypeArray.h>

#include <DB/Columns/ColumnArray.h>

#include <DB/Storages/StorageTinyLog.h>


#define DBMS_STORAGE_LOG_DATA_FILE_EXTENSION 	".bin"


namespace DB
{

using Poco::SharedPtr;


TinyLogBlockInputStream::TinyLogBlockInputStream(size_t block_size_, const Names & column_names_, StoragePtr owned_storage)
	: IProfilingBlockInputStream(owned_storage), block_size(block_size_), column_names(column_names_), storage(dynamic_cast<StorageTinyLog &>(*owned_storage)), finished(false)
{
}


Block TinyLogBlockInputStream::readImpl()
{
	Block res;

	if (finished || (!streams.empty() && streams.begin()->second->compressed.eof()))
	{
		/** Закрываем файлы (ещё до уничтожения объекта).
		  * Чтобы при создании многих источников, но одновременном чтении только из нескольких,
		  *  буферы не висели в памяти.
		  */
		finished = true;
		streams.clear();
		return res;
	}

	/// Если файлы не открыты, то открываем их.
	if (streams.empty())
		for (Names::const_iterator it = column_names.begin(); it != column_names.end(); ++it)
			addStream(*it, *storage.getDataTypeByName(*it));

	for (Names::const_iterator it = column_names.begin(); it != column_names.end(); ++it)
	{
		ColumnWithNameAndType column;
		column.name = *it;
		column.type = storage.getDataTypeByName(*it);
		column.column = column.type->createColumn();
		readData(*it, *column.type, *column.column);

		if (column.column->size())
			res.insert(column);
	}

	if (!res || streams.begin()->second->compressed.eof())
	{
		finished = true;
		streams.clear();
	}

	return res;
}


void TinyLogBlockInputStream::addStream(const String & name, const IDataType & type, size_t level)
{
	/// Для массивов используются отдельные потоки для размеров.
	if (const DataTypeArray * type_arr = dynamic_cast<const DataTypeArray *>(&type))
	{
		String size_name = name + ARRAY_SIZES_COLUMN_NAME_SUFFIX + Poco::NumberFormatter::format(level);
		streams.insert(std::make_pair(size_name, new Stream(storage.files[size_name].data_file.path())));

		addStream(name, *type_arr->getNestedType(), level + 1);
	}
	else
		streams.insert(std::make_pair(name, new Stream(storage.files[name].data_file.path())));
}


void TinyLogBlockInputStream::readData(const String & name, const IDataType & type, IColumn & column, size_t level)
{
	/// Для массивов требуется сначала десериализовать размеры, а потом значения.
	if (const DataTypeArray * type_arr = dynamic_cast<const DataTypeArray *>(&type))
	{
		type_arr->deserializeOffsets(
			column,
			streams[name + ARRAY_SIZES_COLUMN_NAME_SUFFIX + Poco::NumberFormatter::format(level)]->compressed,
			block_size);

		if (column.size())
		{
			IColumn & nested_column = dynamic_cast<ColumnArray &>(column).getData();
			readData(name, *type_arr->getNestedType(), nested_column, level + 1);

			if (nested_column.size() != dynamic_cast<ColumnArray &>(column).getOffsets()[column.size() - 1])
				throw Exception("Cannot read array data for all offsets", ErrorCodes::CANNOT_READ_ALL_DATA);
		}
	}
	else
		type.deserializeBinary(column, streams[name]->compressed, block_size);
}


TinyLogBlockOutputStream::TinyLogBlockOutputStream(StoragePtr owned_storage)
	: IBlockOutputStream(owned_storage), storage(dynamic_cast<StorageTinyLog &>(*owned_storage))
{
	for (NamesAndTypesList::const_iterator it = storage.columns->begin(); it != storage.columns->end(); ++it)
		addStream(it->first, *it->second);
}


void TinyLogBlockOutputStream::addStream(const String & name, const IDataType & type, size_t level)
{
	/// Для массивов используются отдельные потоки для размеров.
	if (const DataTypeArray * type_arr = dynamic_cast<const DataTypeArray *>(&type))
	{
		String size_name = name + ARRAY_SIZES_COLUMN_NAME_SUFFIX + Poco::NumberFormatter::format(level);
		streams.insert(std::make_pair(size_name, new Stream(storage.files[size_name].data_file.path())));
		
		addStream(name, *type_arr->getNestedType(), level + 1);
	}
	else
		streams.insert(std::make_pair(name, new Stream(storage.files[name].data_file.path())));
}


void TinyLogBlockOutputStream::writeData(const String & name, const IDataType & type, const IColumn & column, size_t level)
{
	/// Для массивов требуется сначала сериализовать размеры, а потом значения.
	if (const DataTypeArray * type_arr = dynamic_cast<const DataTypeArray *>(&type))
	{
		type_arr->serializeOffsets(
			column,
			streams[name + ARRAY_SIZES_COLUMN_NAME_SUFFIX + Poco::NumberFormatter::format(level)]->compressed);
		
		writeData(name, *type_arr->getNestedType(), dynamic_cast<const ColumnArray &>(column).getData(), level + 1);
	}
	else
		type.serializeBinary(column, streams[name]->compressed);
}


void TinyLogBlockOutputStream::write(const Block & block)
{
	storage.check(block, true);

	for (size_t i = 0; i < block.columns(); ++i)
	{
		const ColumnWithNameAndType & column = block.getByPosition(i);
		writeData(column.name, *column.type, *column.column);
	}
}


StorageTinyLog::StorageTinyLog(const std::string & path_, const std::string & name_, NamesAndTypesListPtr columns_, bool attach)
	: path(path_), name(name_), columns(columns_)
{
	if (columns->empty())
		throw Exception("Empty list of columns passed to StorageTinyLog constructor", ErrorCodes::EMPTY_LIST_OF_COLUMNS_PASSED);

	if (!attach)
	{
		/// создаём файлы, если их нет
		String full_path = path + escapeForFileName(name) + '/';
		if (0 != mkdir(full_path.c_str(), S_IRWXU | S_IRWXG | S_IRWXO) && errno != EEXIST)
			throwFromErrno("Cannot create directory " + full_path, ErrorCodes::CANNOT_CREATE_DIRECTORY);
	}

	for (NamesAndTypesList::const_iterator it = columns->begin(); it != columns->end(); ++it)
		addFile(it->first, *it->second);
}

StoragePtr StorageTinyLog::create(const std::string & path_, const std::string & name_, NamesAndTypesListPtr columns_, bool attach)
{
	return (new StorageTinyLog(path_, name_, columns_, attach))->thisPtr();
}


void StorageTinyLog::addFile(const String & column_name, const IDataType & type, size_t level)
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
		
		addFile(column_name, *type_arr->getNestedType(), level + 1);
	}
	else
	{
		ColumnData column_data;
		files.insert(std::make_pair(column_name, column_data));
		files[column_name].data_file = Poco::File(
			path + escapeForFileName(name) + '/' + escapeForFileName(column_name) + DBMS_STORAGE_LOG_DATA_FILE_EXTENSION);
	}
}


void StorageTinyLog::rename(const String & new_path_to_db, const String & new_name)
{
	/// Переименовываем директорию с данными.
	Poco::File(path + escapeForFileName(name)).renameTo(new_path_to_db + escapeForFileName(new_name));
	
	path = new_path_to_db;
	name = new_name;

	for (Files_t::iterator it = files.begin(); it != files.end(); ++it)
		it->second.data_file = Poco::File(path + escapeForFileName(name) + '/' + Poco::Path(it->second.data_file.path()).getFileName());
}


BlockInputStreams StorageTinyLog::read(
	const Names & column_names,
	ASTPtr query,
	const Settings & settings,
	QueryProcessingStage::Enum & processed_stage,
	size_t max_block_size,
	unsigned threads)
{
	check(column_names);
	processed_stage = QueryProcessingStage::FetchColumns;
	return BlockInputStreams(1, new TinyLogBlockInputStream(max_block_size, column_names, thisPtr()));
}

	
BlockOutputStreamPtr StorageTinyLog::write(
	ASTPtr query)
{
	return new TinyLogBlockOutputStream(thisPtr());
}


void StorageTinyLog::dropImpl()
{
	for (Files_t::iterator it = files.begin(); it != files.end(); ++it)
		if (it->second.data_file.exists())
			it->second.data_file.remove();
}

}
