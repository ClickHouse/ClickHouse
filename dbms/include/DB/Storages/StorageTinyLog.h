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
#include <DB/DataStreams/IBlockOutputStream.h>
#include <DB/Common/FileChecker.h>
#include <Poco/Util/XMLConfiguration.h>


namespace DB
{

class StorageTinyLog;


class TinyLogBlockInputStream : public IProfilingBlockInputStream
{
public:
	TinyLogBlockInputStream(size_t block_size_, const Names & column_names_, StorageTinyLog & storage_);
	String getName() const { return "TinyLogBlockInputStream"; }

	String getID() const;

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

	typedef std::map<std::string, std::unique_ptr<Stream> > FileStreams;
	FileStreams streams;

	void addStream(const String & name, const IDataType & type, size_t level = 0);
	void readData(const String & name, const IDataType & type, IColumn & column, size_t limit, size_t level = 0, bool read_offsets = true);
};

class TinyLogBlockOutputStream : public IBlockOutputStream
{
public:
	TinyLogBlockOutputStream(StorageTinyLog & storage_);

	~TinyLogBlockOutputStream();

	void write(const Block & block);
	void writeSuffix();
private:
	StorageTinyLog & storage;

	struct Stream
	{
		Stream(const std::string & data_path, size_t max_compress_block_size) :
			plain(data_path, max_compress_block_size, O_APPEND | O_CREAT | O_WRONLY),
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

	typedef std::map<std::string, std::unique_ptr<Stream> > FileStreams;
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
	static StoragePtr create(
		const std::string & path_,
		const std::string & name_,
		NamesAndTypesListPtr columns_,
		const NamesAndTypesList & materialized_columns_,
		const NamesAndTypesList & alias_columns_,
		const ColumnDefaults & column_defaults_,
		bool attach,
		size_t max_compress_block_size_ = DEFAULT_MAX_COMPRESS_BLOCK_SIZE);

	std::string getName() const override { return "TinyLog"; }
	std::string getTableName() const override { return name; }

	const NamesAndTypesList & getColumnsListImpl() const override { return *columns; }

	BlockInputStreams read(
		const Names & column_names,
		ASTPtr query,
		const Context & context,
		const Settings & settings,
		QueryProcessingStage::Enum & processed_stage,
		size_t max_block_size = DEFAULT_BLOCK_SIZE,
		unsigned threads = 1) override;

	BlockOutputStreamPtr write(ASTPtr query) override;

	void drop() override;

	void rename(const String & new_path_to_db, const String & new_database_name, const String & new_table_name) override;

	bool checkData() const override;

	/// Данные столбца
	struct ColumnData
	{
		Poco::File data_file;
	};
	typedef std::map<String, ColumnData> Files_t;

	Files_t & getFiles();

	std::string full_path() { return path + escapeForFileName(name) + '/';}

private:
	String path;
	String name;
	NamesAndTypesListPtr columns;

	size_t max_compress_block_size;

	Files_t files;

	FileChecker<StorageTinyLog> file_checker;

	Logger * log;

	StorageTinyLog(
		const std::string & path_,
		const std::string & name_,
		NamesAndTypesListPtr columns_,
		const NamesAndTypesList & materialized_columns_,
		const NamesAndTypesList & alias_columns_,
		const ColumnDefaults & column_defaults_,
		bool attach,
		size_t max_compress_block_size_);

	void addFile(const String & column_name, const IDataType & type, size_t level = 0);
};

}
