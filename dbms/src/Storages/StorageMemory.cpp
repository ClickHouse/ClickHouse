#include <map>

#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>

#include <DB/Storages/StorageMemory.h>


namespace DB
{

using Poco::SharedPtr;


MemoryBlockInputStream::MemoryBlockInputStream(const Names & column_names_, StorageMemory & storage_)
	: column_names(column_names_), storage(storage_), it(storage.data.begin())
{
}


Block MemoryBlockInputStream::readImpl()
{
	if (it == storage.data.end())
		return Block();
	else
		return *it++;
}


MemoryBlockOutputStream::MemoryBlockOutputStream(StorageMemory & storage_)
	: storage(storage_)
{
}


void MemoryBlockOutputStream::write(const Block & block)
{
	storage.check(block);
	storage.data.push_back(block);
}


StorageMemory::StorageMemory(const std::string & name_, NamesAndTypesListPtr columns_)
	: name(name_), columns(columns_)
{
}


BlockInputStreamPtr StorageMemory::read(
	const Names & column_names,
	ASTPtr query,
	size_t max_block_size)
{
	check(column_names);
	return new MemoryBlockInputStream(column_names, *this);
}

	
BlockOutputStreamPtr StorageMemory::write(
	ASTPtr query)
{
	return new MemoryBlockOutputStream(*this);
}


void StorageMemory::drop()
{
	data.clear();
}

}
