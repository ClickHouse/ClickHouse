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


/** Смещение до каждой некоторой пачки значений.
  * Эти пачки имеют одинаковый размер в разных столбцах.
  * Они нужны, чтобы можно было читать данные в несколько потоков.
  */
struct Mark
{
	size_t rows;	/// Сколько строк содержится в этой пачке и всех предыдущих.
	size_t offset;	/// Смещение до пачки в сжатом файле.
};
typedef std::vector<Mark> Marks;


class LogBlockInputStream : public IProfilingBlockInputStream
{
public:
	LogBlockInputStream(size_t block_size_, const Names & column_names_, StorageLog & storage_, size_t mark_number_, size_t rows_limit_);
	Block readImpl();
	String getName() const { return "LogBlockInputStream"; }
	BlockInputStreamPtr clone() { return new LogBlockInputStream(block_size, column_names, storage, mark_number, rows_limit); }
private:
	size_t block_size;
	Names column_names;
	StorageLog & storage;
	size_t mark_number;		/// С какой засечки читать данные
	size_t rows_limit;		/// Максимальное количество строк, которых можно прочитать

	size_t rows_read;

	struct Stream
	{
		Stream(const std::string & data_path, size_t offset)
			: plain(data_path), compressed(plain)
		{
			plain.seek(offset);
		}
		
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
		Stream(const std::string & data_path, const std::string & marks_path) :
			plain(data_path, DBMS_DEFAULT_BUFFER_SIZE, O_APPEND | O_CREAT | O_WRONLY),
			compressed(plain),
			marks(marks_path, DBMS_DEFAULT_BUFFER_SIZE, O_APPEND | O_CREAT | O_WRONLY) {}
		
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
		QueryProcessingStage::Enum & processed_stage,
		size_t max_block_size = DEFAULT_BLOCK_SIZE,
		unsigned threads = 1);

	BlockOutputStreamPtr write(
		ASTPtr query);

	void drop();
	
	void rename(const String & new_path_to_db, const String & new_name);

private:
	String path;
	String name;
	NamesAndTypesListPtr columns;

	/// Данные столбца
	struct ColumnData
	{
		Poco::File data_file;
		Poco::File marks_file;
		Marks marks;
	};
	typedef std::map<String, ColumnData> Files_t;
	Files_t files;
};

}
