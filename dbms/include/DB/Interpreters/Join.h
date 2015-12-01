#pragma once

#include <common/logger_useful.h>

#include <DB/Parsers/ASTJoin.h>

#include <DB/Interpreters/AggregationCommon.h>

#include <DB/Common/Arena.h>
#include <DB/Common/HashTable/HashMap.h>

#include <DB/DataStreams/IBlockInputStream.h>


namespace DB
{


/** Структура данных для реализации JOIN-а.
  * По сути, хэш-таблица: ключи -> строки присоединяемой таблицы.
  * Исключение - CROSS JOIN, где вместо хэш-таблицы просто набор блоков без ключей.
  *
  * JOIN-ы бывают девяти типов: ANY/ALL × LEFT/INNER/RIGHT/FULL, а также CROSS.
  *
  * Если указано ANY - выбрать из "правой" таблицы только одну, первую попавшуюся строку, даже если там более одной соответствующей строки.
  * Если указано ALL - обычный вариант JOIN-а, при котором строки могут размножаться по числу соответствующих строк "правой" таблицы.
  * Вариант ANY работает более оптимально.
  *
  * Если указано INNER - оставить только строки, для которых есть хотя бы одна строка "правой" таблицы.
  * Если указано LEFT - в случае, если в "правой" таблице нет соответствующей строки, заполнить её значениями "по-умолчанию".
  * Если указано RIGHT - выполнить так же, как INNER, запоминая те строки из правой таблицы, которые были присоединены,
  *  в конце добавить строки из правой таблицы, которые не были присоединены, подставив в качестве значений для левой таблицы, значения "по-умолчанию".
  * Если указано FULL - выполнить так же, как LEFT, запоминая те строки из правой таблицы, которые были присоединены,
  *  в конце добавить строки из правой таблицы, которые не были присоединены, подставив в качестве значений для левой таблицы, значения "по-умолчанию".
  *
  * То есть, LEFT и RIGHT JOIN-ы не являются симметричными с точки зрения реализации.
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

	bool empty() { return type == Type::EMPTY; }

	/** Передать информацию о структуре блока.
	  * Следует обязательно вызвать до вызовов insertFromBlock.
	  */
	void setSampleBlock(const Block & block);

	/** Добавить в отображение для соединения блок "правой" таблицы.
	  * Возвращает false, если превышено какое-нибудь ограничение, и больше не нужно вставлять.
	  */
	bool insertFromBlock(const Block & block);

	/** Присоединить к блоку "левой" таблицы новые столбцы из сформированного отображения.
	  */
	void joinBlock(Block & block) const;

	/** Запомнить тотальные значения для последующего использования.
	  */
	void setTotals(const Block & block) { totals = block; }
	bool hasTotals() const { return totals; };

	void joinTotals(Block & block) const;

	/** Для RIGHT и FULL JOIN-ов.
	  * Поток, в котором значения по-умолчанию из левой таблицы соединены с неприсоединёнными ранее строками из правой таблицы.
	  * Использовать только после того, как были сделаны все вызовы joinBlock.
	  */
	BlockInputStreamPtr createStreamWithNonJoinedRows(Block & left_sample_block, size_t max_block_size) const;

	/// Считает суммарное число ключей во всех Join'ах
	size_t getTotalRowCount() const;
	/// Считает суммарный размер в байтах буфферов всех Join'ов + размер string_pool'а
	size_t getTotalByteCount() const;

	ASTJoin::Kind getKind() const { return kind; }


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


	/** Добавляет или не добавляет флаг - был ли элемент использован.
	  * Для реализации RIGHT и FULL JOIN-ов.
	  * NOTE: Можно сохранять флаг в один из бит указателя block или номера row_num.
	  */
	template <bool enable, typename Base>
	struct WithUsedFlag;

	template <typename Base>
	struct WithUsedFlag<true, Base> : Base
	{
		mutable bool used = false;
		using Base::Base;
		using Base_t = Base;
		void setUsed() const { used = true; }	/// Может выполняться из разных потоков.
		bool getUsed() const { return used; }
	};

	template <typename Base>
	struct WithUsedFlag<false, Base> : Base
	{
		using Base::Base;
		using Base_t = Base;
		void setUsed() const {}
		bool getUsed() const { return true; }
	};


	/** Разные структуры данных, которые могут использоваться для соединения.
	  */
	template <typename Mapped>
	struct MapsTemplate
	{
		/// Специализация для случая, когда есть один числовой ключ.
		typedef HashMap<UInt64, Mapped, HashCRC32<UInt64>> MapUInt64;

		/// Специализация для случая, когда есть один строковый ключ.
		typedef HashMapWithSavedHash<StringRef, Mapped> MapString;

		/** Сравнивает 128 битные хэши.
		  * Если все ключи фиксированной длины, влезающие целиком в 128 бит, то укладывает их без изменений в 128 бит.
		  * Иначе - вычисляет SipHash от набора из всех ключей.
		  * (При этом, строки, содержащие нули посередине, могут склеиться.)
		  */
		typedef HashMap<UInt128, Mapped, UInt128HashCRC32> MapHashed;

		std::unique_ptr<MapUInt64> key64;
		std::unique_ptr<MapString> key_string;
		std::unique_ptr<MapHashed> hashed;
	};

	using MapsAny = MapsTemplate<WithUsedFlag<false, RowRef>>;
	using MapsAll = MapsTemplate<WithUsedFlag<false, RowRefList>>;
	using MapsAnyFull = MapsTemplate<WithUsedFlag<true, RowRef>>;
	using MapsAllFull = MapsTemplate<WithUsedFlag<true, RowRefList>>;

private:
	friend class NonJoinedBlockInputStream;

	ASTJoin::Kind kind;
	ASTJoin::Strictness strictness;

	/// Имена ключевых столбцов (по которым производится соединение) в "левой" таблице.
	const Names key_names_left;
	/// Имена ключевых столбцов (по которым производится соединение) в "правой" таблице.
	const Names key_names_right;

	/** Блоки данных таблицы, с которой идёт соединение.
	  */
	BlocksList blocks;

	MapsAny maps_any;			/// Для ANY LEFT|INNER JOIN
	MapsAll maps_all;			/// Для ALL LEFT|INNER JOIN
	MapsAnyFull maps_any_full;	/// Для ANY RIGHT|FULL JOIN
	MapsAllFull maps_all_full;	/// Для ALL RIGHT|FULL JOIN

	/// Дополнительные данные - строки, а также продолжения односвязных списков строк.
	Arena pool;

	enum class Type
	{
		EMPTY,
		KEY_64,
		KEY_STRING,
		HASHED,
		CROSS,
	};

	Type type = Type::EMPTY;

	static Type chooseMethod(const ConstColumnPlainPtrs & key_columns, bool & keys_fit_128_bits, Sizes & key_sizes);

	bool keys_fit_128_bits;
	Sizes key_sizes;

	Block sample_block_with_columns_to_add;
	Block sample_block_with_keys;

	Logger * log;

	/// Ограничения на максимальный размер множества
	size_t max_rows;
	size_t max_bytes;
	OverflowMode overflow_mode;

	Block totals;

	/** Защищает работу с состоянием в функциях insertFromBlock и joinBlock.
	  * Эти функции могут вызываться одновременно из разных потоков только при использовании StorageJoin,
	  *  и StorageJoin вызывает только эти две функции.
	  * Поэтому остальные функции не защинены.
	  */
	mutable Poco::RWLock rwlock;

	void init(Type type_);

	template <ASTJoin::Strictness STRICTNESS, typename Maps>
	void insertFromBlockImpl(Maps & maps, size_t rows, const ConstColumnPlainPtrs & key_columns, size_t keys_size, Block * stored_block);

	template <ASTJoin::Kind KIND, ASTJoin::Strictness STRICTNESS, typename Maps>
	void joinBlockImpl(Block & block, const Maps & maps) const;

	void joinBlockImplCross(Block & block) const;

	/// Проверить не превышены ли допустимые размеры множества
	bool checkSizeLimits() const;

	/// Кинуть исключение, если в блоках не совпадают типы ключей.
	void checkTypesOfKeys(const Block & block_left, const Block & block_right) const;
};

typedef std::shared_ptr<Join> JoinPtr;
typedef std::vector<JoinPtr> Joins;


}
