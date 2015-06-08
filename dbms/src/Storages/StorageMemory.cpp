#include <map>

#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>

#include <DB/DataStreams/IProfilingBlockInputStream.h>

#include <DB/Storages/StorageMemory.h>


namespace DB
{

using Poco::SharedPtr;


class MemoryBlockInputStream : public IProfilingBlockInputStream
{
public:
	MemoryBlockInputStream(const Names & column_names_, BlocksList::iterator begin_, BlocksList::iterator end_)
		: column_names(column_names_), begin(begin_), end(end_), it(begin) {}

	String getName() const { return "Memory"; }

	String getID() const
	{
		std::stringstream res;
		res << "Memory(" << &*begin << ", " << &*end;

		for (const auto & name : column_names)
			res << ", " << name;

		res << ")";
		return res.str();
	}

protected:
	Block readImpl()
	{
		if (it == end)
		{
			return Block();
		}
		else
		{
			Block src = *it;
			Block res;

			/// Добавляем только нужные столбцы в res.
			for (size_t i = 0, size = column_names.size(); i < size; ++i)
				res.insert(src.getByName(column_names[i]));

			++it;
			return res;
		}
	}
private:
	Names column_names;
	BlocksList::iterator begin;
	BlocksList::iterator end;
	BlocksList::iterator it;
};


class MemoryBlockOutputStream : public IBlockOutputStream
{
public:
	MemoryBlockOutputStream(StorageMemory & storage_) : storage(storage_) {}

	void write(const Block & block)
	{
		storage.check(block, true);
		Poco::ScopedLock<Poco::FastMutex> lock(storage.mutex);
		storage.data.push_back(block);
	}
private:
	StorageMemory & storage;
};


StorageMemory::StorageMemory(
	const std::string & name_,
	NamesAndTypesListPtr columns_)
	: name(name_), columns(columns_)
{
}


StorageMemory::StorageMemory(
	const std::string & name_,
	NamesAndTypesListPtr columns_,
	const NamesAndTypesList & materialized_columns_,
	const NamesAndTypesList & alias_columns_,
	const ColumnDefaults & column_defaults_)
	: IStorage{materialized_columns_, alias_columns_, column_defaults_},
	name(name_), columns(columns_)
{
}


StoragePtr StorageMemory::create(
	const std::string & name_,
	NamesAndTypesListPtr columns_)
{
	return (new StorageMemory{
		name_, columns_
	})->thisPtr();
}

StoragePtr StorageMemory::create(
	const std::string & name_,
	NamesAndTypesListPtr columns_,
	const NamesAndTypesList & materialized_columns_,
	const NamesAndTypesList & alias_columns_,
	const ColumnDefaults & column_defaults_)
{
	return (new StorageMemory{
		name_, columns_,
		materialized_columns_, alias_columns_, column_defaults_
	})->thisPtr();
}


BlockInputStreams StorageMemory::read(
	const Names & column_names,
	ASTPtr query,
	const Context & context,
	const Settings & settings,
	QueryProcessingStage::Enum & processed_stage,
	size_t max_block_size,
	unsigned threads)
{
	check(column_names);
	processed_stage = QueryProcessingStage::FetchColumns;

	Poco::ScopedLock<Poco::FastMutex> lock(mutex);

	size_t size = data.size();

	if (threads > size)
		threads = size;

	BlockInputStreams res;

	for (size_t thread = 0; thread < threads; ++thread)
	{
		BlocksList::iterator begin = data.begin();
		BlocksList::iterator end = data.begin();

		std::advance(begin, thread * size / threads);
		std::advance(end, (thread + 1) * size / threads);

		res.push_back(new MemoryBlockInputStream(column_names, begin, end));
	}

	return res;
}


BlockOutputStreamPtr StorageMemory::write(
	ASTPtr query)
{
	return new MemoryBlockOutputStream(*this);
}


void StorageMemory::drop()
{
	Poco::ScopedLock<Poco::FastMutex> lock(mutex);
	data.clear();
}

}
