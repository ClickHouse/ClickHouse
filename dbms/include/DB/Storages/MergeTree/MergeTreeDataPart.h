#pragma once

#include <DB/Core/Field.h>
#include <DB/Core/NamesAndTypes.h>
#include <DB/Storages/MergeTree/ActiveDataPartSet.h>
#include <Poco/RWLock.h>


class SipHash;


namespace DB
{


/// Чексумма одного файла.
struct MergeTreeDataPartChecksum
{
	size_t file_size;
	uint128 file_hash;

	bool is_compressed = false;
	size_t uncompressed_size;
	uint128 uncompressed_hash;

	MergeTreeDataPartChecksum() {}
	MergeTreeDataPartChecksum(size_t file_size_, uint128 file_hash_) : file_size(file_size_), file_hash(file_hash_) {}
	MergeTreeDataPartChecksum(size_t file_size_, uint128 file_hash_, size_t uncompressed_size_, uint128 uncompressed_hash_)
		: file_size(file_size_), file_hash(file_hash_), is_compressed(true),
		uncompressed_size(uncompressed_size_), uncompressed_hash(uncompressed_hash_) {}

	void checkEqual(const MergeTreeDataPartChecksum & rhs, bool have_uncompressed, const String & name) const;
	void checkSize(const String & path) const;
};


/** Контрольные суммы всех не временных файлов.
  * Для сжатых файлов хранятся чексумма и размер разжатых данных, чтобы не зависеть от способа сжатия.
  */
struct MergeTreeDataPartChecksums
{
	using Checksum = MergeTreeDataPartChecksum;

	using FileChecksums = std::map<String, Checksum>;
	FileChecksums files;

	void addFile(const String & file_name, size_t file_size, uint128 file_hash);

	/// Проверяет, что множество столбцов и их контрольные суммы совпадают. Если нет - бросает исключение.
	/// Если have_uncompressed, для сжатых файлов сравнивает чексуммы разжатых данных. Иначе сравнивает только чексуммы файлов.
	void checkEqual(const MergeTreeDataPartChecksums & rhs, bool have_uncompressed) const;

	/// Проверяет, что в директории есть все нужные файлы правильных размеров. Не проверяет чексуммы.
	void checkSizes(const String & path) const;

	/// Сериализует и десериализует в человекочитаемом виде.
	bool read(ReadBuffer & in); /// Возвращает false, если чексуммы в слишком старом формате.
	bool read_v2(ReadBuffer & in);
	bool read_v3(ReadBuffer & in);
	bool read_v4(ReadBuffer & in);
	void write(WriteBuffer & out) const;

	bool empty() const
	{
		return files.empty();
	}

	/// Контрольная сумма от множества контрольных сумм .bin файлов.
	void summaryDataChecksum(SipHash & hash) const;

	String toString() const;
	static MergeTreeDataPartChecksums parse(const String & s);
};


class MergeTreeData;


/// Описание куска с данными.
struct MergeTreeDataPart : public ActiveDataPartSet::Part
{
	using Checksums = MergeTreeDataPartChecksums;

	MergeTreeDataPart(MergeTreeData & storage_) : storage(storage_) {}

	/// Returns the size of .bin file for column `name` if found, zero otherwise
	std::size_t getColumnSize(const String & name) const;

	/** Returns the name of a column with minimum compressed size (as returned by getColumnSize()).
		*	If no checksums are present returns the name of the first physically existing column. */
	String getMinimumSizeColumnName() const;

	MergeTreeData & storage;

	size_t size = 0;				/// в количестве засечек.
	std::atomic<size_t> size_in_bytes {0}; 	/// размер в байтах, 0 - если не посчитано;
											/// используется из нескольких потоков без блокировок (изменяется при ALTER).
	time_t modification_time = 0;
	mutable time_t remove_time = std::numeric_limits<time_t>::max(); /// Когда кусок убрали из рабочего набора.

	/// Если true, деструктор удалит директорию с куском.
	bool is_temp = false;

	/// Для перешардирования.
	bool is_sharded = false;
	size_t shard_no = 0;

	/// Первичный ключ. Всегда загружается в оперативку. Содержит каждое index_granularity значение первичного ключа.
	using Index = Columns;
	Index index;

	/// NOTE Засечки кэшируются в оперативке. См. MarkCache.h.

	Checksums checksums;

	/// Описание столбцов.
	NamesAndTypesList columns;

	using ColumnToSize = std::map<std::string, size_t>;

	/** Блокируется на запись при изменении columns, checksums или любых файлов куска.
		* Блокируется на чтение при    чтении columns, checksums или любых файлов куска.
		*/
	mutable Poco::RWLock columns_lock;

	/** Берется на все время ALTER куска: от начала записи временных фалов до их переименования в постоянные.
		* Берется при разлоченном columns_lock.
		*
		* NOTE: "Можно" было бы обойтись без этого мьютекса, если бы можно было превращать ReadRWLock в WriteRWLock, не снимая блокировку.
		* Такое превращение невозможно, потому что создало бы дедлок, если делать его из двух потоков сразу.
		* Взятие этого мьютекса означает, что мы хотим заблокировать columns_lock на чтение с намерением потом, не
		*  снимая блокировку, заблокировать его на запись.
		*/
	mutable std::mutex alter_mutex;

	~MergeTreeDataPart();

	/// Вычисляем суммарный размер всей директории со всеми файлами
	static size_t calcTotalSize(const String & from);

	void remove() const;
	void renameTo(const String & new_name) const;

	/// Переименовывает кусок, дописав к имени префикс. to_detached - также перенести в директорию detached.
	void renameAddPrefix(bool to_detached, const String & prefix) const;

	/// Загрузить индекс и вычислить размер. Если size=0, вычислить его тоже.
	void loadIndex();

	/// Прочитать контрольные суммы, если есть.
	void loadChecksums(bool require);

	void accumulateColumnSizes(ColumnToSize & column_to_size) const;

	void loadColumns(bool require);

	void checkNotBroken(bool require_part_metadata);

	bool hasColumnFiles(const String & column) const;
};

}
