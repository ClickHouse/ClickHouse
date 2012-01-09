#pragma once

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
		Stream(const std::string & data_path, const std::string & marks_path)
			: plain(data_path), compressed(plain)/*, marks(marks_path)*/ {}
		
		ReadBufferFromFile plain;
		CompressedReadBuffer compressed;

		/** В отдельный файл пишутся смещения до каждой некоторой пачки значений.
		  * Эти пачки имеют одинаковый размер в разных столбцах.
		  * Они нужны, чтобы можно было читать данные в несколько потоков.
		  */
		//ReadBufferFromFile marks;
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
		Stream(const std::string & data_path, const std::string & marks_path)
			: plain(data_path), compressed(plain, CompressionMethod::LZ4), marks(marks_path) {}
		
		WriteBufferFromFile plain;
		CompressedWriteBuffer compressed;
		WriteBufferFromFile marks;
	};

	typedef std::map<std::string, SharedPtr<Stream> > FileStreams;
	FileStreams streams;
};


/** Реализует хранилище, подходящее для логов.
  * Ключи не поддерживаются.
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
	StorageLog(const std::string & path_, const std::string & name_, NamesAndTypesListPtr columns_);

	std::string getName() const { return "Log"; }
	std::string getTableName() const { return name; }

	const NamesAndTypesList & getColumnsList() const { return *columns; }

	BlockInputStreams read(
		const Names & column_names,
		ASTPtr query,
		size_t max_block_size = DEFAULT_BLOCK_SIZE,
		unsigned max_threads = 1);

	BlockOutputStreamPtr write(
		ASTPtr query);

	void drop();

private:
	const std::string path;
	const std::string name;
	NamesAndTypesListPtr columns;

	/// Пара файлов .bin и .mrk
	typedef std::pair<Poco::File, Poco::File> FilePair_t;
	typedef std::map<std::string, FilePair_t> Files_t;
	Files_t files;
};

}
