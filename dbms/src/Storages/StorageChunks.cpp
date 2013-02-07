#include <DB/Storages/StorageChunks.h>
#include <DB/Common/escapeForFileName.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteHelpers.h>
#include <DB/Interpreters/InterpreterDropQuery.h>
#include <DB/Parsers/ASTDropQuery.h>


namespace DB
{
	
StoragePtr StorageChunks::create(const std::string & path_, const std::string & name_, NamesAndTypesListPtr columns_, Context & context_)
{
	return (new StorageChunks(path_, name_, columns_, context_))->thisPtr();
}

void StorageChunks::addReference()
{
	reference_counter.add(1, true);
}

void StorageChunks::removeReference()
{
	Int64 c = reference_counter.add(-1, false);
	if (c < 0)
		throw Exception("Negative refcount on table " + getName(), ErrorCodes::NEGATIVE_REFCOUNT);
	if (c == 0)
		dropThis();
}

BlockInputStreams StorageChunks::readFromChunk(
	const std::string & chunk_name,
	const Names & column_names,
	ASTPtr query,
	const Settings & settings,
	QueryProcessingStage::Enum & processed_stage,
	size_t max_block_size,
	unsigned threads)
{
	loadIndex();
	
	size_t mark1;
	size_t mark2;
	
	{
		Poco::ScopedReadRWLock lock(rwlock);
		
		if (!chunk_indices.count(chunk_name))
			throw Exception("No chunk " + chunk_name + " in table " + getName(), ErrorCodes::CHUNK_NOT_FOUND);
		size_t index = chunk_indices[chunk_name];
		mark1 = marks[index];
		mark2 = index + 1 == marks.size() ? marksCount() : marks[index + 1];
	}
	
	return read(mark1, mark2, column_names, query, settings, processed_stage, max_block_size, threads);
}
	
BlockOutputStreamPtr StorageChunks::writeToNewChunk(
	const std::string & chunk_name)
{
	loadIndex();
	
	{
		Poco::ScopedWriteRWLock lock(rwlock);
		
		if (chunk_indices.count(chunk_name))
			throw Exception("Duplicate chunk name in table " + getName(), ErrorCodes::DUPLICATE_CHUNK_NAME);
		
		size_t mark = marksCount();
		chunk_indices[chunk_name] = marks.size();
		appendChunkToIndex(chunk_name, mark);
		marks.push_back(mark);
	}
	
	return StorageLog::write(this, NULL);
}
	
StorageChunks(const std::string& path_, const std::string& name_, const std::string & database_name_, NamesAndTypesListPtr columns_, Context & context_)
	: StorageLog(path_, name_, columns_), database_name(database_name_), index_loaded(false), reference_counter(path_ + escapeForFileName(name_) + "/refcount.txt"), context(context_) {}
	
void StorageChunks::loadIndex()
{
	loadMarks();
	Poco::ScopedWriteRWLock lock(rwlock);
	if (index_loaded)
		return;
	index_loaded = true;
	
	String index_path = path + escapeForFileName(name) + "/chunks.chn";
	ReadBufferFromFile index(index_path, 4096);
	while (!index.eof())
	{
		String name;
		size_t mark;
		
		readStringBinary(name, index);
		readIntBinary<UInt64>(mark);
		
		chunk_indices[name] = marks.size();
		marks.push_back(mark);
	}
}

void StorageChunks::appendChunkToIndex(const std::string & name, size_t mark)
{
	String index_path = path + escapeForFileName(name) + "/chunks.chn";
	WriteBufferFromFile index(index_path, 4096, O_APPEND | O_CREAT | O_WRONLY);
	writeStringBinary(name, index);
	writeIntBinary<UInt64>(mark, index);
}

void StorageChunks::dropThis()
{
	ASTDropQuery * query = new ASTDropQuery();
	ASTPtr query_ptr = query;
	query->detach = false;
	query->if_exists = false;
	query->database = database_name;
	query->table = name;
	
	InterpreterDropQuery interpreter(query_ptr, context);
	interpreter.execute();
}
