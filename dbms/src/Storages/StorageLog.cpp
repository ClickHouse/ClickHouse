#include <map>

#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>

#include <DB/Storages/StorageLog.h>


namespace DB
{

using Poco::SharedPtr;


LogBlockInputStream::LogBlockInputStream(size_t block_size_, const ColumnNames & column_names_, StorageLog & storage_)
	: block_size(block_size_), column_names(column_names_), storage(storage_)
{
}


Block LogBlockInputStream::read()
{
	Block res;

	for (ColumnNames::const_iterator it = column_names.begin(); it != column_names.end(); ++it)
	{
		if (storage.columns->end() == storage.columns->find(*it))
			throw Exception("There is no column with name " + *it + " in table.",
				ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);

		streams.insert(std::make_pair(*it, new Stream(storage.files[*it].path())));
	}

	for (ColumnNames::const_iterator it = column_names.begin(); it != column_names.end(); ++it)
	{
		ColumnWithNameAndType column;
		column.name = *it;
		column.type = (*storage.columns)[*it];
		column.column = column.type->createColumn();
		column.type->deserializeBinary(*column.column, streams[column.name]->compressed, block_size);

		res.insert(column);
	}

	return res;
}


LogBlockOutputStream::LogBlockOutputStream(StorageLog & storage_)
	: storage(storage_)
{
}


void LogBlockOutputStream::write(const Block & block)
{
	for (size_t i = 0; i < block.columns(); ++i)
	{
		const std::string & name = block.getByPosition(i).name;
		if (storage.columns->end() == storage.columns->find(name))
			throw Exception("There is no column with name " + name + " in table.",
				ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);

		streams.insert(std::make_pair(name, new Stream(storage.files[name].path())));
	}

	for (size_t i = 0; i < block.columns(); ++i)
	{
		const ColumnWithNameAndType & column = block.getByPosition(i);
		column.type->serializeBinary(*column.column, streams[column.name]->compressed);
	}
}


StorageLog::StorageLog(const std::string & path_, const std::string & name_, SharedPtr<NamesAndTypes> columns_,
	const std::string & extension_)
	: path(path_), name(name_), columns(columns_), extension(extension_)
{
	/// создаём файлы, если их нет
	Poco::File dir(path + name + '/');
	dir.createDirectories();
	
	for (NamesAndTypes::const_iterator it = columns->begin(); it != columns->end(); ++it)
	{
		if (files.end() != files.find(it->first))
			throw Exception("Duplicate column with name " + it->first + " in constructor of StorageLog.",
				ErrorCodes::DUPLICATE_COLUMN);

		files.insert(std::make_pair(it->first, Poco::File(path + name + '/' + it->first + extension)));
	}
}


SharedPtr<IBlockInputStream> StorageLog::read(
	const ColumnNames & column_names,
	const ptree & query,
	size_t max_block_size)
{
	return new LogBlockInputStream(max_block_size, column_names, *this);
}

	
SharedPtr<IBlockOutputStream> StorageLog::write(
	const ptree & query)
{
	return new LogBlockOutputStream(*this);
}

}
