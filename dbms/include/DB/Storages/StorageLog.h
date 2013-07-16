#pragma once

#include <set>

#include <Poco/File.h>
#include <Poco/RWLock.h>

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
	LogBlockInputStream(size_t block_size_, const Names & column_names_, StoragePtr owned_storage, size_t mark_number_, size_t rows_limit_);
	String getName() const { return "LogBlockInputStream"; }

	String getID() const
	{
		std::stringstream res;
		res << "Log(" << owned_storage->getTableName() << ", " << &*owned_storage << ", " << mark_number << ", " << rows_limit;

		for (size_t i = 0; i < column_names.size(); ++i)
			res << ", " << column_names[i];

		res << ")";
		return res.str();
	}

protected:
	Block readImpl();
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
			: plain(data_path, std::min(static_cast<size_t>(DBMS_DEFAULT_BUFFER_SIZE), Poco::File(data_path).getSize())),
			compressed(plain)
		{
			if (offset)
				plain.seek(offset);
		}
		
		ReadBufferFromFile plain;
		CompressedReadBuffer compressed;
	};
	
	typedef std::map<std::string, SharedPtr<Stream> > FileStreams;
	FileStreams streams;

	void addStream(const String & name, const IDataType & type, size_t level = 0);
	void readData(const String & name, const IDataType & type, IColumn & column, size_t max_rows_to_read, size_t level = 0, bool read_offsets = true);
};


class LogBlockOutputStream : public IBlockOutputStream
{
public:
	LogBlockOutputStream(StoragePtr owned_storage);
	void write(const Block & block);
private:
	StorageLog & storage;
	Poco::ScopedWriteRWLock lock;

	struct Stream
	{
		Stream(const std::string & data_path) :
			plain(data_path, DBMS_DEFAULT_BUFFER_SIZE, O_APPEND | O_CREAT | O_WRONLY),
			compressed(plain)
		{
			plain_offset = Poco::File(data_path).getSize();
		}
		
		WriteBufferFromFile plain;
		CompressedWriteBuffer compressed;

		size_t plain_offset;	/// Сколько байт было в файле на момент создания LogBlockOutputStream.
	};

	typedef std::vector<std::pair<size_t, Mark> > MarksForColumns;
	
	typedef std::map<std::string, SharedPtr<Stream> > FileStreams;
	FileStreams streams;
	
	typedef std::set<std::string> OffsetColumns;
	
	WriteBufferFromFile marks_stream; /// Объявлен ниже lock, чтобы файл открывался при захваченном rwlock.

	void addStream(const String & name, const IDataType & type, size_t level = 0);
	void writeData(const String & name, const IDataType & type, const IColumn & column, MarksForColumns & out_marks, OffsetColumns & offset_columns, size_t level = 0);
	void writeMarks(MarksForColumns marks);
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
	static StoragePtr create(const std::string & path_, const std::string & name_, NamesAndTypesListPtr columns_);
	
	std::string getName() const { return "Log"; }
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

	void rename(const String & new_path_to_db, const String & new_name);

protected:
	String path;
	String name;
	NamesAndTypesListPtr columns;

	Poco::RWLock rwlock;
	
	StorageLog(const std::string & path_, const std::string & name_, NamesAndTypesListPtr columns_);
	
	/// Прочитать файлы с засечками, если они ещё не прочитаны.
	/// Делается лениво, чтобы при большом количестве таблиц, сервер быстро стартовал.
	/// Нельзя вызывать с залоченным на запись rwlock.
	void loadMarks();
	
	/// Можно вызывать при любом состоянии rwlock.
	size_t marksCount();
	
	BlockInputStreams read(
		size_t from_mark,
		size_t to_mark,
		const Names & column_names,
		ASTPtr query,
		const Settings & settings,
		QueryProcessingStage::Enum & processed_stage,
		size_t max_block_size = DEFAULT_BLOCK_SIZE,
		unsigned threads = 1);
	
private:
	/// Данные столбца
	struct ColumnData
	{
		/// Задает номер столбца в файле с засечками.
		/// Не обязательно совпадает с номером столбца среди столбцов таблицы: здесь нумеруются также столбцы с длинами массивов.
		size_t column_index;
		
		Poco::File data_file;
		Marks marks;
	};
	typedef std::map<String, ColumnData> Files_t;
	Files_t files; /// name -> data
	Names column_names; /// column_index -> name
	
	Poco::File marks_file;
	
	/// Порядок добавления файлов не должен меняться: он соответствует порядку столбцов в файле с засечками.
	void addFile(const String & column_name, const IDataType & type, size_t level = 0);

	bool loaded_marks;
};

}
