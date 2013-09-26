#pragma once

#include <set>

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

class StorageTinyLog;


class TinyLogBlockInputStream : public IProfilingBlockInputStream
{
public:
	TinyLogBlockInputStream(size_t block_size_, const Names & column_names_, StoragePtr owned_storage);
	String getName() const { return "TinyLogBlockInputStream"; }

	String getID() const
	{
		std::stringstream res;
		res << "TinyLog(" << owned_storage->getTableName() << ", " << &*owned_storage;
		
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
	StorageTinyLog & storage;
	bool finished;

	struct Stream
	{
		Stream(const std::string & data_path)
			: plain(data_path, std::min(static_cast<size_t>(DBMS_DEFAULT_BUFFER_SIZE), Poco::File(data_path).getSize())),
			compressed(plain)
		{
		}

		ReadBufferFromFile plain;
		CompressedReadBuffer compressed;
	};

	typedef std::map<std::string, SharedPtr<Stream> > FileStreams;
	FileStreams streams;

	void addStream(const String & name, const IDataType & type, size_t level = 0);
	void readData(const String & name, const IDataType & type, IColumn & column, size_t limit, size_t level = 0, bool read_offsets = true);
};


class TinyLogBlockOutputStream : public IBlockOutputStream
{
public:
	TinyLogBlockOutputStream(StoragePtr owned_storage);
	void write(const Block & block);
	void writeSuffix();
private:
	StorageTinyLog & storage;

	struct Stream
	{
		Stream(const std::string & data_path) :
			plain(data_path, DBMS_DEFAULT_BUFFER_SIZE, O_APPEND | O_CREAT | O_WRONLY),
			compressed(plain)
		{
		}

		WriteBufferFromFile plain;
		CompressedWriteBuffer compressed;

		void finalize()
		{
			compressed.next();
			plain.next();
		}
	};

	typedef std::map<std::string, SharedPtr<Stream> > FileStreams;
	FileStreams streams;
	
	typedef std::set<std::string> OffsetColumns;

	void addStream(const String & name, const IDataType & type, size_t level = 0);
	void writeData(const String & name, const IDataType & type, const IColumn & column, OffsetColumns & offset_columns, size_t level = 0);
};


/** Реализует хранилище, подходящее для маленьких кусочков лога.
  * Отличается от StorageLog отсутствием файлов с засечками.
  */
class StorageTinyLog : public IStorage
{
friend class TinyLogBlockInputStream;
friend class TinyLogBlockOutputStream;

public:
	/** Подцепить таблицу с соответствующим именем, по соответствующему пути (с / на конце),
	  *  (корректность имён и путей не проверяется)
	  *  состоящую из указанных столбцов.
	  * Если не указано attach - создать директорию, если её нет.
	  */
	static StoragePtr create(const std::string & path_, const std::string & name_, NamesAndTypesListPtr columns_, bool attach);

	std::string getName() const { return "TinyLog"; }
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
	
	void rename(const String & new_path_to_db, const String & new_name);

private:
	String path;
	String name;
	NamesAndTypesListPtr columns;

	/// Данные столбца
	struct ColumnData
	{
		Poco::File data_file;
	};
	typedef std::map<String, ColumnData> Files_t;
	Files_t files;

	StorageTinyLog(const std::string & path_, const std::string & name_, NamesAndTypesListPtr columns_, bool attach);
	
	void addFile(const String & column_name, const IDataType & type, size_t level = 0);
};

}
