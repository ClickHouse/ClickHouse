#pragma once

#include <DB/Core/NamesAndTypes.h>
#include <DB/Storages/IStorage.h>
#include <DB/DataStreams/IProfilingBlockInputStream.h>


namespace DB
{

class StorageMemory;
typedef std::vector<Block> Blocks;

class MemoryBlockInputStream : public IProfilingBlockInputStream
{
public:
	MemoryBlockInputStream(const Names & column_names_, StorageMemory & storage_);
	Block readImpl();
	String getName() const { return "MemoryBlockInputStream"; }
	BlockInputStreamPtr clone() { return new MemoryBlockInputStream(column_names, storage); }
private:
	Names column_names;
	StorageMemory & storage;
	Blocks::iterator it;
};


class MemoryBlockOutputStream : public IBlockOutputStream
{
public:
	MemoryBlockOutputStream(StorageMemory & storage_);
	void write(const Block & block);
	BlockOutputStreamPtr clone() { return new MemoryBlockOutputStream(storage); }
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
	StorageMemory(const std::string & name_, NamesAndTypesListPtr columns_);

	std::string getName() const { return "Memory"; }
	std::string getTableName() const { return name; }

	const NamesAndTypesList & getColumnsList() const { return *columns; }

	BlockInputStreamPtr read(
		const Names & column_names,
		ASTPtr query,
		size_t max_block_size = DEFAULT_BLOCK_SIZE);

	BlockOutputStreamPtr write(
		ASTPtr query);

private:
	const std::string name;
	NamesAndTypesListPtr columns;

	/// Сами данные
	Blocks data;
};

}
