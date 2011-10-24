#ifndef DBMS_STORAGES_STORAGE_LOG_H
#define DBMS_STORAGES_STORAGE_LOG_H

#include <map>

#include <Poco/SharedPtr.h>
#include <Poco/File.h>

#include <DB/Core/NamesAndTypes.h>
#include <DB/IO/ReadBufferFromFile.h>
#include <DB/IO/WriteBufferFromFile.h>
#include <DB/IO/CompressedReadBuffer.h>
#include <DB/IO/CompressedWriteBuffer.h>
#include <DB/Storages/IStorage.h>
#include <DB/DataStreams/IProfilingBlockInputStream.h>


namespace DB
{

using Poco::SharedPtr;
class StorageLog;

class LogBlockInputStream : public IProfilingBlockInputStream
{
public:
	LogBlockInputStream(size_t block_size_, const Names & column_names_, StorageLog & storage_);
	Block readImpl();
	String getName() const { return "LogBlockInputStream"; }
	BlockInputStreamPtr clone() { return new LogBlockInputStream(block_size, column_names, storage); }
private:
	size_t block_size;
	Names column_names;
	StorageLog & storage;

	struct Stream
	{
		Stream(const std::string & path)
			: plain(path), compressed(plain) {}
		
		ReadBufferFromFile plain;
		CompressedReadBuffer compressed;
	};
	
	typedef std::map<std::string, SharedPtr<Stream> > FileStreams;
	FileStreams streams;
};


class LogBlockOutputStream : public IBlockOutputStream
{
public:
	LogBlockOutputStream(StorageLog & storage_);
	void write(const Block & block);
	BlockOutputStreamPtr clone() { return new LogBlockOutputStream(storage); }
private:
	StorageLog & storage;

	struct Stream
	{
		Stream(const std::string & path)
			: plain(path), compressed(plain) {}
		
		WriteBufferFromFile plain;
		CompressedWriteBuffer compressed;
	};

	typedef std::map<std::string, SharedPtr<Stream> > FileStreams;
	FileStreams streams;
};


/** Реализует хранилище, подходящее для логов.
  * В нём не поддерживаются ключи; запись блокирует всю таблицу.
  * Данные хранятся в сжатом виде.
  */
class StorageLog : public IStorage
{
friend class LogBlockInputStream;
friend class LogBlockOutputStream;

public:
	/** Подцепить таблицу с соответствующим именем, по соответствующему пути (с / на конце),
	  *  (корректность имён и путей не проверяется)
	  *  состоящую из указанных столбцов; создать файлы, если их нет.
	  */
	StorageLog(const std::string & path_, const std::string & name_, SharedPtr<NamesAndTypes> columns_,
		const std::string & extension_ = ".bin");

	std::string getName() const { return "Log"; }
	std::string getTableName() const { return name; }

	const NamesAndTypes & getColumns() const { return *columns; }

	BlockInputStreamPtr read(
		const Names & column_names,
		ASTPtr query,
		size_t max_block_size = DEFAULT_BLOCK_SIZE);

	BlockOutputStreamPtr write(
		ASTPtr query);

private:
	const std::string path;
	const std::string name;
	SharedPtr<NamesAndTypes> columns;
	const std::string extension;

	typedef std::map<std::string, Poco::File> Files_t;
	Files_t files;
};

}

#endif
