#pragma once

#include <Poco/RWLock.h>

#include <DB/Parsers/ASTTablesInSelectQuery.h>

#include <DB/Interpreters/AggregationCommon.h>
#include <DB/Interpreters/SettingsCommon.h>

#include <DB/Common/Arena.h>
#include <DB/Common/HashTable/HashMap.h>

#include <DB/Columns/ColumnString.h>
#include <DB/Columns/ColumnFixedString.h>

#include <DB/DataStreams/IBlockInputStream.h>


namespace DB
{

/// Helpers to obtain keys (to use in a hash table or similar data structure) for various equi-JOINs.

/// UInt8/16/32/64 or another types with same number of bits.
template <typename FieldType>
struct JoinKeyGetterOneNumber
{
	using Key = FieldType;

	const FieldType * vec;

	/** Created before processing of each block.
	  * Initialize some members, used in another methods, called in inner loops.
	  */
	JoinKeyGetterOneNumber(const ConstColumnPlainPtrs & key_columns)
	{
		vec = &static_cast<const ColumnVector<FieldType> *>(key_columns[0])->getData()[0];
	}

	Key getKey(
		const ConstColumnPlainPtrs & key_columns,
		size_t keys_size,				/// number of key columns.
		size_t i,						/// row number to get key from.
		const Sizes & key_sizes) const	/// If keys are of fixed size - their sizes. Not used for methods with variable-length keys.
	{
		return unionCastToUInt64(vec[i]);
	}

	/// Place additional data into memory pool, if needed, when new key was inserted into hash table.
	static void onNewKey(Key & key, Arena & pool) {}
};

/// For single String key.
struct JoinKeyGetterString
{
	using Key = StringRef;

	const ColumnString::Offsets_t * offsets;
	const ColumnString::Chars_t * chars;

	JoinKeyGetterString(const ConstColumnPlainPtrs & key_columns)
	{
		const IColumn & column = *key_columns[0];
		const ColumnString & column_string = static_cast<const ColumnString &>(column);
		offsets = &column_string.getOffsets();
		chars = &column_string.getChars();
	}

	Key getKey(
		const ConstColumnPlainPtrs & key_columns,
		size_t keys_size,
		size_t i,
		const Sizes & key_sizes) const
	{
		return StringRef(
			&(*chars)[i == 0 ? 0 : (*offsets)[i - 1]],
			(i == 0 ? (*offsets)[i] : ((*offsets)[i] - (*offsets)[i - 1])) - 1);
	}

	static void onNewKey(Key & key, Arena & pool)
	{
		key.data = pool.insert(key.data, key.size);
	}
};

/// For single FixedString key.
struct JoinKeyGetterFixedString
{
	using Key = StringRef;

	size_t n;
	const ColumnFixedString::Chars_t * chars;

	JoinKeyGetterFixedString(const ConstColumnPlainPtrs & key_columns)
	{
		const IColumn & column = *key_columns[0];
		const ColumnFixedString & column_string = static_cast<const ColumnFixedString &>(column);
		n = column_string.getN();
		chars = &column_string.getChars();
	}

	Key getKey(
		const ConstColumnPlainPtrs & key_columns,
		size_t keys_size,
		size_t i,
		const Sizes & key_sizes) const
	{
		return StringRef(&(*chars)[i * n], n);
	}

	static void onNewKey(Key & key, Arena & pool)
	{
		key.data = pool.insert(key.data, key.size);
	}
};

/// For keys of fixed size, that could be packed in sizeof TKey width.
template <typename TKey>
struct JoinKeyGetterFixed
{
	using Key = TKey;

	JoinKeyGetterFixed(const ConstColumnPlainPtrs & key_columns)
	{
	}

	Key getKey(
		const ConstColumnPlainPtrs & key_columns,
		size_t keys_size,
		size_t i,
		const Sizes & key_sizes) const
	{
		return packFixed<Key>(i, keys_size, key_columns, key_sizes);
	}

	static void onNewKey(Key & key, Arena & pool) {}
};

/// Generic method, use crypto hash function.
struct JoinKeyGetterHashed
{
	using Key = UInt128;

	JoinKeyGetterHashed(const ConstColumnPlainPtrs & key_columns)
	{
	}

	Key getKey(
		const ConstColumnPlainPtrs & key_columns,
		size_t keys_size,
		size_t i,
		const Sizes & key_sizes) const
	{
		return hash128(i, keys_size, key_columns);
	}

	static void onNewKey(Key & key, Arena & pool) {}
};



struct Limits;


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
		 const Limits & limits, ASTTableJoin::Kind kind_, ASTTableJoin::Strictness strictness_);

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

	ASTTableJoin::Kind getKind() const { return kind; }


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


	#define APPLY_FOR_JOIN_VARIANTS(M) \
		M(key8) 			\
		M(key16) 			\
		M(key32) 			\
		M(key64) 			\
		M(key_string) 		\
		M(key_fixed_string) \
		M(keys128) 			\
		M(keys256) 			\
		M(hashed)

	enum class Type
	{
		EMPTY,
		CROSS,
		#define M(NAME) NAME,
			APPLY_FOR_JOIN_VARIANTS(M)
		#undef M
	};


	/** Разные структуры данных, которые могут использоваться для соединения.
	  */
	template <typename Mapped>
	struct MapsTemplate
	{
		std::unique_ptr<HashMap<UInt8, Mapped, TrivialHash, HashTableFixedGrower<8>>> 	key8;
		std::unique_ptr<HashMap<UInt16, Mapped, TrivialHash, HashTableFixedGrower<16>>> key16;
		std::unique_ptr<HashMap<UInt32, Mapped, HashCRC32<UInt32>>> 					key32;
		std::unique_ptr<HashMap<UInt64, Mapped, HashCRC32<UInt64>>> 					key64;
		std::unique_ptr<HashMapWithSavedHash<StringRef, Mapped>> 						key_string;
		std::unique_ptr<HashMapWithSavedHash<StringRef, Mapped>> 						key_fixed_string;
		std::unique_ptr<HashMap<UInt128, Mapped, UInt128HashCRC32>> 					keys128;
		std::unique_ptr<HashMap<UInt256, Mapped, UInt256HashCRC32>> 					keys256;
		std::unique_ptr<HashMap<UInt128, Mapped, UInt128TrivialHash>> 					hashed;
	};

	using MapsAny = MapsTemplate<WithUsedFlag<false, RowRef>>;
	using MapsAll = MapsTemplate<WithUsedFlag<false, RowRefList>>;
	using MapsAnyFull = MapsTemplate<WithUsedFlag<true, RowRef>>;
	using MapsAllFull = MapsTemplate<WithUsedFlag<true, RowRefList>>;

private:
	friend class NonJoinedBlockInputStream;

	ASTTableJoin::Kind kind;
	ASTTableJoin::Strictness strictness;

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

private:
	Type type = Type::EMPTY;

	static Type chooseMethod(const ConstColumnPlainPtrs & key_columns, Sizes & key_sizes);

	bool keys_fit_128_bits;
	Sizes key_sizes;

	Block sample_block_with_columns_to_add;
	Block sample_block_with_keys;

	Poco::Logger * log;

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

	template <ASTTableJoin::Strictness STRICTNESS, typename Maps>
	void insertFromBlockImpl(Maps & maps, size_t rows, const ConstColumnPlainPtrs & key_columns, size_t keys_size, Block * stored_block);

	template <ASTTableJoin::Strictness STRICTNESS, typename KeyGetter, typename Map>
	void insertFromBlockImplType(Map & map, size_t rows, const ConstColumnPlainPtrs & key_columns, size_t keys_size, Block * stored_block);

	template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename Maps>
	void joinBlockImpl(Block & block, const Maps & maps) const;

	template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename KeyGetter, typename Map>
	void joinBlockImplType(
		Block & block, const Map & map, size_t rows, const ConstColumnPlainPtrs & key_columns, size_t keys_size,
		size_t num_columns_to_add, size_t num_columns_to_skip, ColumnPlainPtrs & added_columns,
		std::unique_ptr<IColumn::Filter> & filter,
		IColumn::Offset_t & current_offset, std::unique_ptr<IColumn::Offsets_t> & offsets_to_replicate) const;

	void joinBlockImplCross(Block & block) const;

	/// Проверить не превышены ли допустимые размеры множества
	bool checkSizeLimits() const;

	/// Кинуть исключение, если в блоках не совпадают типы ключей.
	void checkTypesOfKeys(const Block & block_left, const Block & block_right) const;
};

using JoinPtr = std::shared_ptr<Join>;
using Joins = std::vector<JoinPtr>;


}
