#pragma once

#include <Poco/Mutex.h>

#include <DB/Core/NamesAndTypes.h>
#include <DB/Storages/IStorage.h>
#include <DB/DataStreams/IProfilingBlockInputStream.h>


namespace DB
{

class StorageMemory;

class MemoryBlockInputStream : public IProfilingBlockInputStream
{
public:
	MemoryBlockInputStream(const Names & column_names_, BlocksList::iterator begin_, BlocksList::iterator end_, StoragePtr owned_storage);
	String getName() const { return "MemoryBlockInputStream"; }

	String getID() const
	{
		std::stringstream res;
		res << "Memory(" << owned_storage->getTableName() << ", " << &*owned_storage << ", " << &*begin << ", " << &*end;

		for (size_t i = 0; i < column_names.size(); ++i)
			res << ", " << column_names[i];

		res << ")";
		return res.str();
	}
	
protected:
	Block readImpl();
private:
	Names column_names;
	BlocksList::iterator begin;
	BlocksList::iterator end;
	BlocksList::iterator it;
};


class MemoryBlockOutputStream : public IBlockOutputStream
{
public:
	MemoryBlockOutputStream(StoragePtr owned_storage);
	void write(const Block & block);
private:
	StorageMemory & storage;
};


/** Реализует хранилище в оперативке.
  * Подходит для временных данных.
  * В нём не поддерживаются ключи.
  * Данные хранятся в виде набора блоков и никуда дополнительно не сохраняются.
  */
class StorageMemory : public IStorage
{
friend class MemoryBlockInputStream;
friend class MemoryBlockOutputStream;

public:
	static StoragePtr create(const std::string & name_, NamesAndTypesListPtr columns_);

	std::string getName() const { return "Memory"; }
	std::string getTableName() const { return name; }

	const NamesAndTypesList & getColumnsList() const { return *columns; }

	BlockInputStreams read(
		const Names & column_names,
		ASTPtr query,
		const Settings & settings,
		QueryProcessingStage::Enum & processed_stage,
		size_t max_block_size = DEFAULT_BLOCK_SIZE,
		unsigned threads = 1);

	BlockOutputStreamPtr write(
		ASTPtr query);

	void dropImpl();
	void rename(const String & new_path_to_db, const String & new_name) { name = new_name; }

private:
	String name;
	NamesAndTypesListPtr columns;

	/// Сами данные. list - чтобы при вставке в конец, существующие итераторы не инвалидировались.
	BlocksList data;

	Poco::FastMutex mutex;
	
	StorageMemory(const std::string & name_, NamesAndTypesListPtr columns_);
};

}
