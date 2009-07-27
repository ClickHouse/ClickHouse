#ifndef DBMS_STORAGE_PLAIN_H
#define DBMS_STORAGE_PLAIN_H

#include <set>
#include <map>

#include <Poco/SharedPtr.h>
#include <Poco/BinaryReader.h>

#include <DB/Table.h>

#include <DB/Storage.h>


namespace DB
{
	
/** Простое хранилище с "плоским" первичным ключом.
  * Хранит список смещений в бинарном файле, список свободных блоков - в другом бинарном файле.
  * Поиск в этом файле линейный.
  * При обновлении данных, индексный файл полностью перезаписывается.
  * Файл с данными не сжатый.
  * Индекс полностью загружается в память во время работы.
  */
class StoragePlain : public StorageBase
{
private:
	std::string path;
	std::string name;

	std::string data_file_name;
	std::string offsets_file_name;
	std::string free_blocks_file_name;

	Poco::File data_file;
	Poco::File offsets_file;
	Poco::File free_blocks_file;

	struct Offset
	{
		size_t offset;	/// смещение от начала файла
		size_t size;	/// размер данных

		bool operator< (const Offset & rhs) const
		{
			return size < rhs.size;
		}
	};

	std::map<Row, Offset> offsets;
	std::set<Offset> free_blocks;

public:
	/** Путь со слешем на конце. */
	StoragePlain(const std::string & path_, const std::string & name_);

	void addToTable(Table * table_, ColumnGroup * column_group_);

	void merge(const AggregatedRowSet & data, const ColumnMask & mask);
};

}

#endif
