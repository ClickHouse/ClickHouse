#ifndef DBMS_STORAGES_STORAGE_LOG_H
#define DBMS_STORAGES_STORAGE_LOG_H

#include <map>

#include <Poco/SharedPtr.h>
#include <Poco/File.h>
#include <Poco/FileStream.h>

#include <DB/Core/NamesAndTypes.h>
#include <DB/Common/CompressedInputStream.h>
#include <DB/Common/CompressedOutputStream.h>
#include <DB/Storages/IStorage.h>


namespace DB
{

using Poco::SharedPtr;
class StorageLog;

class LogBlockInputStream : public IBlockInputStream
{
public:
	LogBlockInputStream(size_t block_size_, const ColumnNames & column_names_, StorageLog & storage_);
	Block read();
private:
	size_t block_size;
	const ColumnNames & column_names;
	StorageLog & storage;

	struct Stream
	{
		Stream(const std::string & path)
			: plain(path, std::ios::in | std::ios::binary), compressed(plain) {}
		
		Poco::FileInputStream plain;
		CompressedInputStream compressed;
	};
	
	typedef std::map<std::string, SharedPtr<Stream> > FileStreams;
	FileStreams streams;
};


class LogBlockOutputStream : public IBlockOutputStream
{
public:
	LogBlockOutputStream(StorageLog & storage_);
	void write(const Block & block);
private:
	StorageLog & storage;

	struct Stream
	{
		Stream(const std::string & path)
			: plain(path, std::ios::out | std::ios::ate | std::ios::binary), compressed(plain) {}
		
		Poco::FileOutputStream plain;
		CompressedOutputStream compressed;
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

	SharedPtr<IBlockInputStream> read(
		const ColumnNames & column_names,
		const ptree & query,
		size_t max_block_size = DEFAULT_BLOCK_SIZE);

	SharedPtr<IBlockOutputStream> write(
		const ptree & query);

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
