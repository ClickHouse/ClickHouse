#include <map>

#include <DB/Common/Exception.h>

#include <DB/DataStreams/IProfilingBlockInputStream.h>

#include <DB/Storages/StorageMemory.h>


namespace DB
{

class MemoryBlockInputStream : public IProfilingBlockInputStream
{
public:
	MemoryBlockInputStream(const Names & column_names_, BlocksList::iterator begin_, BlocksList::iterator end_)
		: column_names(column_names_), begin(begin_), end(end_), it(begin) {}

	String getName() const override { return "Memory"; }

	String getID() const override
	{
		std::stringstream res;
		res << "Memory(" << &*begin << ", " << &*end;

		for (const auto & name : column_names)
			res << ", " << name;

		res << ")";
		return res.str();
	}

protected:
	Block readImpl() override
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

	void write(const Block & block) override
	{
		storage.check(block, true);
		std::lock_guard<std::mutex> lock(storage.mutex);
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
	return make_shared(name_, columns_);
}

StoragePtr StorageMemory::create(
	const std::string & name_,
	NamesAndTypesListPtr columns_,
	const NamesAndTypesList & materialized_columns_,
	const NamesAndTypesList & alias_columns_,
	const ColumnDefaults & column_defaults_)
{
	return make_shared(name_, columns_, materialized_columns_, alias_columns_, column_defaults_);
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

	std::lock_guard<std::mutex> lock(mutex);

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

		res.push_back(std::make_shared<MemoryBlockInputStream>(column_names, begin, end));
	}

	return res;
}


BlockOutputStreamPtr StorageMemory::write(
	ASTPtr query, const Settings & settings)
{
	return std::make_shared<MemoryBlockOutputStream>(*this);
}


void StorageMemory::drop()
{
	std::lock_guard<std::mutex> lock(mutex);
	data.clear();
}

}
