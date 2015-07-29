#include <DB/Storages/StorageChunks.h>
#include <DB/Storages/StorageChunkRef.h>
#include <DB/Common/escapeForFileName.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteHelpers.h>
#include <DB/IO/ReadBufferFromFile.h>
#include <DB/IO/WriteBufferFromFile.h>
#include <DB/Interpreters/InterpreterDropQuery.h>
#include <DB/Parsers/ASTDropQuery.h>
#include <DB/Common/VirtualColumnUtils.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/Columns/ColumnString.h>


namespace DB
{

StoragePtr StorageChunks::create(
	const std::string & path_,
	const std::string & name_,
	const std::string & database_name_,
	NamesAndTypesListPtr columns_,
	const NamesAndTypesList & materialized_columns_,
	const NamesAndTypesList & alias_columns_,
	const ColumnDefaults & column_defaults_,
	Context & context_,
	bool attach)
{
	return (new StorageChunks{
		path_, name_, database_name_, columns_,
		materialized_columns_, alias_columns_, column_defaults_,
		context_, attach
	})->thisPtr();
}

void StorageChunks::addReference()
{
	reference_counter.add(1, false);
}

void StorageChunks::removeReference()
{
	Int64 c = reference_counter.add(-1, false);
	if (c < 0)
		throw Exception("Negative refcount on table " + name, ErrorCodes::NEGATIVE_REFCOUNT);
	if (c == 0)
		dropThis();
}

BlockInputStreams StorageChunks::read(
	const Names & column_names,
	ASTPtr query,
	const Context & context,
	const Settings & settings,
	QueryProcessingStage::Enum & processed_stage,
	const size_t max_block_size,
	const unsigned threads)
{
	bool has_virtual_column = false;

	for (const auto & column : column_names)
		if (column == _table_column_name)
			has_virtual_column = true;

	/// Если виртуальных столбцов нет, просто считать данные из таблицы
	if (!has_virtual_column)
		return read(0, std::numeric_limits<size_t>::max(), column_names,
					query, context, settings,
					processed_stage, max_block_size, threads);

	Block virtual_columns_block = getBlockWithVirtualColumns();
	if (!VirtualColumnUtils::filterBlockWithQuery(query, virtual_columns_block, context))
		return read(0, std::numeric_limits<size_t>::max(), column_names,
					query, context, settings,
					processed_stage, max_block_size, threads);
	std::multiset<String> values = VirtualColumnUtils::extractSingleValueFromBlock<String>(virtual_columns_block, _table_column_name);

	BlockInputStreams res;
	for (const auto & it : values)
	{
		BlockInputStreams temp = readFromChunk(it, column_names, query, context, settings, processed_stage, max_block_size, threads);
		res.insert(res.end(), temp.begin(), temp.end());
	}
	return res;
}

/// Построить блок состоящий только из возможных значений виртуальных столбцов
Block StorageChunks::getBlockWithVirtualColumns() const
{
	Block res;
	ColumnWithNameAndType _table(new ColumnString, new DataTypeString, _table_column_name);

	for (const auto & it : chunk_names)
		_table.column->insert(it);

	res.insert(_table);
	return res;
}

BlockInputStreams StorageChunks::readFromChunk(
	const std::string & chunk_name,
	const Names & column_names,
	ASTPtr query,
	const Context & context,
	const Settings & settings,
	QueryProcessingStage::Enum & processed_stage,
	const size_t max_block_size,
	const unsigned threads)
{
	size_t mark1;
	size_t mark2;

	{
		Poco::ScopedReadRWLock lock(rwlock);

		if (!chunk_indices.count(chunk_name))
			throw Exception("No chunk " + chunk_name + " in table " + name, ErrorCodes::CHUNK_NOT_FOUND);
		size_t index = chunk_indices[chunk_name];
		mark1 = chunk_num_to_marks[index];
		mark2 = index + 1 == chunk_num_to_marks.size() ? marksCount() : chunk_num_to_marks[index + 1];
	}

	return read(mark1, mark2, column_names, query, context, settings, processed_stage, max_block_size, threads);
}

BlockOutputStreamPtr StorageChunks::writeToNewChunk(
	const std::string & chunk_name)
{
	{
		Poco::ScopedWriteRWLock lock(rwlock);

		if (chunk_indices.count(chunk_name))
			throw Exception("Duplicate chunk name in table " + name, ErrorCodes::DUPLICATE_CHUNK_NAME);

		size_t mark = marksCount();
		chunk_indices[chunk_name] = chunk_num_to_marks.size();
		appendChunkToIndex(chunk_name, mark);
		chunk_num_to_marks.push_back(mark);
		chunk_names.push_back(chunk_name);
	}

	return StorageLog::write(nullptr);
}

StorageChunks::StorageChunks(
	const std::string & path_,
	const std::string & name_,
	const std::string & database_name_,
	NamesAndTypesListPtr columns_,
	const NamesAndTypesList & materialized_columns_,
	const NamesAndTypesList & alias_columns_,
	const ColumnDefaults & column_defaults_,
	Context & context_,
	bool attach)
	:
	StorageLog(path_, name_, columns_,
			   materialized_columns_, alias_columns_, column_defaults_,
			   context_.getSettings().max_compress_block_size),
	database_name(database_name_),
	reference_counter(path_ + escapeForFileName(name_) + "/refcount.txt"),
	context(context_),
	log(&Logger::get("StorageChunks"))
{
	if (!attach)
		reference_counter.add(1, true);

	_table_column_name = "_table" + VirtualColumnUtils::chooseSuffix(getColumnsList(), "_table");

	try
	{
		loadIndex();
	}
	catch (Exception & e)
	{
		if (e.code() != ErrorCodes::SIZES_OF_MARKS_FILES_ARE_INCONSISTENT)
			throw;

		e.addMessage("Table " + name_ + " is broken and loaded as empty.");
		tryLogCurrentException(__PRETTY_FUNCTION__);
		return;
	}

	/// Создадим все таблицы типа ChunkRef. Они должны располагаться в той же БД.
	{
		Poco::ScopedLock<Poco::Mutex> lock(context.getMutex());
		for (ChunkIndices::const_iterator it = chunk_indices.begin(); it != chunk_indices.end(); ++it)
		{
			if (context.isTableExist(database_name, it->first))
			{
				LOG_WARNING(log, "Chunk " << it->first << " exists in more than one Chunks tables.");
				context.detachTable(database_name, it->first);
			}

			context.addTable(database_name, it->first, StorageChunkRef::create(it->first, context, database_name, name, true));
		}
	}
}

NameAndTypePair StorageChunks::getColumn(const String & column_name) const
{
	if (column_name == _table_column_name)
		return NameAndTypePair(_table_column_name, new DataTypeString);
	return getRealColumn(column_name);
}

bool StorageChunks::hasColumn(const String & column_name) const
{
	if (column_name == _table_column_name)
		return true;
	return IStorage::hasColumn(column_name);
}

std::pair<String, size_t> StorageChunks::getTableFromMark(size_t mark) const
{
	/// Находим последний <= элемент в массие
	size_t pos = std::upper_bound(chunk_num_to_marks.begin(), chunk_num_to_marks.end(), mark) - chunk_num_to_marks.begin() - 1;
	/// Вычисляем номер засечки до которой будет длится текущая таблица
	size_t last = std::numeric_limits<size_t>::max();
	if (pos + 1 < chunk_num_to_marks.size())
		last = chunk_num_to_marks[pos + 1] - 1;
	return std::make_pair(chunk_names[pos], last);
}

void StorageChunks::loadIndex()
{
	loadMarks();

	Poco::ScopedWriteRWLock lock(rwlock);

	String index_path = path + escapeForFileName(name) + "/chunks.chn";

	if (!Poco::File(index_path).exists())
		return;

	ReadBufferFromFile index(index_path, 4096);
	while (!index.eof())
	{
		String name;
		size_t mark;

		readStringBinary(name, index);
		readIntBinary<UInt64>(mark, index);

		chunk_indices[name] = chunk_num_to_marks.size();
		chunk_num_to_marks.push_back(mark);
		chunk_names.push_back(name);
	}
}

void StorageChunks::appendChunkToIndex(const std::string & chunk_name, size_t mark)
{
	String index_path = path + escapeForFileName(name) + "/chunks.chn";
	WriteBufferFromFile index(index_path, 4096, O_APPEND | O_CREAT | O_WRONLY);
	writeStringBinary(chunk_name, index);
	writeIntBinary<UInt64>(mark, index);
	index.next();
	file_checker.update(Poco::File(index_path));
}

void StorageChunks::dropThis()
{
	LOG_TRACE(log, "Table " << name << " will drop itself.");

	ASTDropQuery * query = new ASTDropQuery();
	ASTPtr query_ptr = query;
	query->detach = false;
	query->if_exists = false;
	query->database = database_name;
	query->table = name;

	InterpreterDropQuery interpreter(query_ptr, context);
	interpreter.execute();
}

}
