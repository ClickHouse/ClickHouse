#pragma once

#include <statdaemons/Stopwatch.h>

#include <Yandex/logger_useful.h>

#include <DB/Interpreters/AggregationCommon.h>
#include <DB/Interpreters/Set.h>

#include <DB/Common/Arena.h>
#include <DB/Common/HashTable/HashMap.h>

#include <DB/DataStreams/IBlockInputStream.h>


namespace DB
{


/** Структура данных для реализации JOIN-а.
  * По сути, хэш-таблица: ключи -> строки присоединяемой таблицы.
  */
class Join
{
public:
	Join(const Names & key_names_, const Limits & limits)
		: key_names(key_names_),
		max_bytes_to_transfer(limits.max_bytes_to_transfer),
		max_rows_to_transfer(limits.max_rows_to_transfer),
		transfer_overflow_mode(limits.transfer_overflow_mode),
		bytes_in_external_table(0),
		rows_in_external_table(0),
		only_external(false),
		log(&Logger::get("Join")),
		max_rows(limits.max_rows_in_set),
		max_bytes(limits.max_bytes_in_set),
		overflow_mode(limits.set_overflow_mode)
	{
	}
	
	bool empty() { return type == Set::EMPTY; }

	/** Запомнить поток блоков, чтобы потом его можно было прочитать и создать отображение.
	  */
	void setSource(BlockInputStreamPtr stream) { source = stream; }
	BlockInputStreamPtr getSource() { return source; }

	void setExternalOutput(StoragePtr storage) { external_table = storage; }
	void setOnlyExternal(bool flag) { only_external = flag; }

	/// Возвращает false, если превышено какое-нибудь ограничение, и больше не нужно вставлять.
	bool insertFromBlock(Block & block);

	size_t size() const { return getTotalRowCount(); }
	
private:
	/// Имена ключевых столбцов - по которым производится соединение.
	const Names key_names;
	ColumnNumbers key_numbers;

	/// Ссылка на строку в блоке.
	struct RowRef
	{
		const Block * block;
		size_t row_num;

		RowRef() {}
		RowRef(const Block * block_, size_t row_num_) : block(block_), row_num(row_num_) {}
	};

	/// Односвязный список ссылок на строки.
	struct RowRefList : RowRef
	{
		RowRefList * next = nullptr;

		RowRefList() {}
		RowRefList(const Block * block_, size_t row_num_) : RowRef(block_, row_num_) {}
	};

	/** Блоки данных таблицы, с которой идёт соединение.
	  */
	Blocks blocks;

	/** Разные структуры данных, которые могут использоваться для соединения.
	  */
	typedef HashMap<UInt64, RowRef> MapUInt64;
	typedef HashMapWithSavedHash<StringRef, RowRef> MapString;
	typedef HashMap<UInt128, RowRef, UInt128Hash> MapHashed;

	BlockInputStreamPtr source;

	/// Информация о внешней таблице, заполняемой этим классом
	StoragePtr external_table;
	size_t max_bytes_to_transfer;
	size_t max_rows_to_transfer;
	OverflowMode transfer_overflow_mode;
	size_t bytes_in_external_table;
	size_t rows_in_external_table;
	bool only_external;

	/// Специализация для случая, когда есть один числовой ключ.
	std::unique_ptr<MapUInt64> key64;

	/// Специализация для случая, когда есть один строковый ключ.
	std::unique_ptr<MapString> key_string;

	/** Сравнивает 128 битные хэши.
	  * Если все ключи фиксированной длины, влезающие целиком в 128 бит, то укладывает их без изменений в 128 бит.
	  * Иначе - вычисляет SipHash от набора из всех ключей.
	  * (При этом, строки, содержащие нули посередине, могут склеиться.)
	  */
	std::unique_ptr<MapHashed> hashed;

	/// Дополнительные данные - строки, а также продолжения односвязных списков строк.
	Arena pool;

	Set::Type type = Set::EMPTY;
	
	bool keys_fit_128_bits;
	Sizes key_sizes;

	Logger * log;
	
	/// Ограничения на максимальный размер множества
	size_t max_rows;
	size_t max_bytes;
	OverflowMode overflow_mode;

	void init(Set::Type type_)
	{
		type = type_;

		switch (type)
		{
			case Set::EMPTY:											break;
			case Set::KEY_64:		key64		.reset(new MapUInt64); 	break;
			case Set::KEY_STRING:	key_string	.reset(new MapString); 	break;
			case Set::HASHED:		hashed		.reset(new MapHashed);	break;

			default:
				throw Exception("Unknown JOIN keys variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
		}
	}

	/// Проверить не превышены ли допустимые размеры множества
	bool checkSizeLimits() const;
	/// Проверить не превышены ли допустимые размеры внешней таблицы для передачи данных
	bool checkExternalSizeLimits() const;

	/// Считает суммарное число ключей во всех Set'ах
	size_t getTotalRowCount() const;
	/// Считает суммарный размер в байтах буфферов всех Set'ов + размер string_pool'а
	size_t getTotalByteCount() const;
};

typedef Poco::SharedPtr<Join> JoinPtr;
typedef std::vector<JoinPtr> Joins;


}
