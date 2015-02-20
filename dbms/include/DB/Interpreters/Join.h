#pragma once

#include <Yandex/logger_useful.h>

#include <DB/Parsers/ASTJoin.h>

#include <DB/Interpreters/AggregationCommon.h>
#include <DB/Interpreters/Set.h>

#include <DB/Common/Arena.h>
#include <DB/Common/HashTable/HashMap.h>

#include <DB/DataStreams/IBlockInputStream.h>


namespace DB
{


/** Структура данных для реализации JOIN-а.
  * По сути, хэш-таблица: ключи -> строки присоединяемой таблицы.
  *
  * JOIN-ы бывают четырёх типов: ANY/ALL x LEFT/INNER.
  *
  * Если указано ANY - выбрать из "правой" таблицы только одну, первую попавшуюся строку, даже если там более одной соответствующей строки.
  * Если указано ALL - обычный вариант JOIN-а, при котором строки могут размножаться по числу соответствующих строк "правой" таблицы.
  * Вариант ANY работает более оптимально.
  *
  * Если указано INNER - оставить только строки, для которых есть хотя бы одна строка "правой" таблицы.
  * Если указано LEFT - в случае, если в "правой" таблице нет соответствующей строки, заполнить её значениями "по-умолчанию".
  *
  * Все соединения делаются по равенству кортежа столбцов "ключей" (эквисоединение).
  * Неравенства и прочие условия не поддерживаются.
  *
  * Реализация такая:
  *
  * 1. "Правая" таблица засовывается в хэш-таблицу в оперативке.
  * Она имеет вид keys -> row в случае ANY или keys -> [rows...] в случае ALL.
  * Это делается в функции insertFromBlock.
  *
  * 2. При обработке "левой" таблицы, присоединяем к ней данные из сформированной хэш-таблицы.
  * Это делается в функции joinBlock.
  *
  * В случае ANY LEFT JOIN - формируем новые столбцы с найденной строкой или значениями по-умолчанию.
  * Самый простой вариант. Количество строк при JOIN-е не меняется.
  *
  * В случае ANY INNER JOIN - формируем новые столбцы с найденной строкой;
  *  а также заполняем фильтр - для каких строк значения не нашлось.
  * После чего, фильтруем столбцы "левой" таблицы.
  *
  * В случае ALL ... JOIN - формируем новые столбцы со всеми найденными строками;
  *  а также заполняем массив offsets, описывающий, во сколько раз надо размножить строки "левой" таблицы.
  * После чего, размножаем столбцы "левой" таблицы.
  */
class Join
{
public:
	Join(const Names & key_names_left_, const Names & key_names_right_,
		 const Limits & limits, ASTJoin::Kind kind_, ASTJoin::Strictness strictness_)
		: kind(kind_), strictness(strictness_),
		key_names_left(key_names_left_),
		key_names_right(key_names_right_),
		log(&Logger::get("Join")),
		max_rows(limits.max_rows_in_join),
		max_bytes(limits.max_bytes_in_join),
		overflow_mode(limits.join_overflow_mode)
	{
	}

	bool empty() { return type == Set::EMPTY; }

	/** Добавить в отображение для соединения блок "правой" таблицы.
	  * Возвращает false, если превышено какое-нибудь ограничение, и больше не нужно вставлять.
	  */
	bool insertFromBlock(const Block & block);

	/** Присоединить к блоку "левой" таблицы новые столбцы из сформированного отображения.
	  */
	void joinBlock(Block & block) const;

	/// Считает суммарное число ключей во всех Join'ах
	size_t getTotalRowCount() const;
	/// Считает суммарный размер в байтах буфферов всех Join'ов + размер string_pool'а
	size_t getTotalByteCount() const;


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

	/** Разные структуры данных, которые могут использоваться для соединения.
	  */
	struct MapsAny
	{
		/// Специализация для случая, когда есть один числовой ключ.
		typedef HashMap<UInt64, RowRef, HashCRC32<UInt64>> MapUInt64;

		/// Специализация для случая, когда есть один строковый ключ.
		typedef HashMapWithSavedHash<StringRef, RowRef> MapString;

		/** Сравнивает 128 битные хэши.
		  * Если все ключи фиксированной длины, влезающие целиком в 128 бит, то укладывает их без изменений в 128 бит.
		  * Иначе - вычисляет SipHash от набора из всех ключей.
		  * (При этом, строки, содержащие нули посередине, могут склеиться.)
		  */
		typedef HashMap<UInt128, RowRef, UInt128HashCRC32> MapHashed;

		std::unique_ptr<MapUInt64> key64;
		std::unique_ptr<MapString> key_string;
		std::unique_ptr<MapHashed> hashed;
	};

	struct MapsAll
	{
		typedef HashMap<UInt64, RowRefList, HashCRC32<UInt64>> MapUInt64;
		typedef HashMapWithSavedHash<StringRef, RowRefList> MapString;
		typedef HashMap<UInt128, RowRefList, UInt128HashCRC32> MapHashed;

		std::unique_ptr<MapUInt64> key64;
		std::unique_ptr<MapString> key_string;
		std::unique_ptr<MapHashed> hashed;
	};

private:
	ASTJoin::Kind kind;
	ASTJoin::Strictness strictness;

	/// Имена ключевых столбцов (по которым производится соединение) в "левой" таблице.
	const Names key_names_left;
	/// Имена ключевых столбцов (по которым производится соединение) в "правой" таблице.
	const Names key_names_right;

	/** Блоки данных таблицы, с которой идёт соединение.
	  */
	BlocksList blocks;

	MapsAny maps_any;
	MapsAll maps_all;

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

	/** Защищает работу с состоянием в функциях insertFromBlock и joinBlock.
	  * Эти функции могут вызываться одновременно из разных потоков только при использовании StorageJoin,
	  *  и StorageJoin вызывает только эти две функции.
	  * Поэтому остальные функции не защинены.
	  */
	mutable Poco::RWLock rwlock;

	void init(Set::Type type_);

	template <ASTJoin::Strictness STRICTNESS, typename Maps>
	void insertFromBlockImpl(Maps & maps, size_t rows, const ConstColumnPlainPtrs & key_columns, size_t keys_size, Block * stored_block);

	template <ASTJoin::Kind KIND, ASTJoin::Strictness STRICTNESS, typename Maps>
	void joinBlockImpl(Block & block, const Maps & maps) const;

	/// Проверить не превышены ли допустимые размеры множества
	bool checkSizeLimits() const;
};

typedef Poco::SharedPtr<Join> JoinPtr;
typedef std::vector<JoinPtr> Joins;


}
