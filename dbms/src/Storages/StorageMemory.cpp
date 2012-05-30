#include <map>

#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>

#include <DB/Storages/StorageMemory.h>


namespace DB
{

using Poco::SharedPtr;


MemoryBlockInputStream::MemoryBlockInputStream(const Names & column_names_, Blocks::iterator begin_, Blocks::iterator end_)
	: column_names(column_names_), begin(begin_), end(end_), it(begin)
{
}


Block MemoryBlockInputStream::readImpl()
{
	if (it == end)
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


BlockInputStreams StorageMemory::read(
	const Names & column_names,
	ASTPtr query,
	QueryProcessingStage::Enum & processed_stage,
	size_t max_block_size,
	unsigned threads)
{
	check(column_names);
	processed_stage = QueryProcessingStage::FetchColumns;

	if (threads > data.size())
		threads = data.size();

	BlockInputStreams res;

	for (size_t thread = 0; thread < threads; ++thread)
		res.push_back(new MemoryBlockInputStream(
			column_names,
			data.begin() + thread * data.size() / threads,
			data.begin() + (thread + 1) * data.size() / threads));
	
	return res;
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
