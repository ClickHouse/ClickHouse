#ifndef DBMS_PRIMARY_KEY_NONE_H
#define DBMS_PRIMARY_KEY_NONE_H

#include <Poco/SharedPtr.h>
#include <Poco/File.h>
#include <Poco/FileStream.h>

#include <DB/PrimaryKey.h>
#include <DB/TablePartReader.h>


namespace DB
{
	
/** Самый простой первичный ключ - ничего не индексирует;
  * для чтения или обновления приходится читать файл целиком.
  * - удобно для логов.
  */
class PrimaryKeyNone : public PrimaryKeyBase
{
friend class PrimaryKeyNoneTablePartReader;
private:
	std::string path;
	std::string name;
	std::string data_file_name;

	Poco::File data_file;

public:
	/** Путь со слешем на конце. */
	PrimaryKeyNone(const std::string & path_, const std::string & name_);

	/** Просто дописывает данные в конец. */
	void merge(const AggregatedRowSet & data, const ColumnMask & mask);

	/** Прочитать данные, префикс ключа которых совпадает с key */
	Poco::SharedPtr<ITablePartReader> read(const Row & key);
};


class PrimaryKeyNoneTablePartReader : public ITablePartReader
{
friend class PrimaryKeyNone;
private:
	const Row key;
	/// слабый указатель на первичный ключ
	PrimaryKeyNone * pk;
	Poco::FileInputStream istr;

	PrimaryKeyNoneTablePartReader(const Row & key_, PrimaryKeyNone * pk_);

public:
	bool fetch(Row & row);
};

}

#endif
