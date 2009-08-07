#ifndef DBMS_STORAGE_NO_KEY_H
#define DBMS_STORAGE_NO_KEY_H

#include <Poco/SharedPtr.h>
#include <Poco/File.h>
#include <Poco/FileStream.h>

#include <DB/Storage.h>
#include <DB/TablePartReader.h>
#include <DB/CompressedOutputStream.h>
#include <DB/CompressedInputStream.h>


namespace DB
{
	
/** Самое простое хранилище - в нём первичный ключ ничего не индексирует;
  * для чтения или обновления приходится читать файл целиком.
  * - удобно для логов.
  */
class StorageNoKey : public StorageBase
{
friend class StorageNoKeyTablePartReader;
private:
	std::string path;
	std::string name;
	std::string data_file_name;

	Poco::File data_file;

public:
	/** Путь со слешем на конце. */
	StorageNoKey(const std::string & path_, const std::string & name_);

	/** Просто дописывает данные в конец. */
	void merge(const AggregatedRowSet & data, const ColumnMask & mask);

	/** Прочитать данные, префикс ключа которых совпадает с key */
	Poco::SharedPtr<ITablePartReader> read(const Row & key);
};


class StorageNoKeyTablePartReader : public ITablePartReader
{
friend class StorageNoKey;
private:
	const Row key;
	/// слабый указатель на хранилище
	StorageNoKey * pk;
	Poco::FileInputStream istr;
	CompressedInputStream decompressor;

	StorageNoKeyTablePartReader(const Row & key_, StorageNoKey * pk_);

public:
	bool fetch(Row & row);
};

}

#endif
