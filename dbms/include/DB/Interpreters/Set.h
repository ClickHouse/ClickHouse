#pragma once

#include <set>

#include <Yandex/logger_useful.h>

#include <DB/Core/ColumnNumbers.h>
#include <DB/Common/Arena.h>

#include <DB/DataStreams/IBlockInputStream.h>

#include <DB/Parsers/IAST.h>

#include <DB/Common/HashTable/HashSet.h>
#include <DB/Interpreters/AggregationCommon.h>
#include <DB/Interpreters/Limits.h>
#include <DB/Columns/ColumnConst.h>
#include <DB/Columns/ColumnArray.h>

#include <DB/Storages/MergeTree/BoolMask.h>
#include <DB/Storages/MergeTree/PKCondition.h>

namespace DB
{


/** Методы для разных вариантов реализации множеств.
  * Используются в качестве параметра шаблона.
  */


/// Для случая, когда есть один числовой ключ.
template <typename FieldType, typename TData>	/// UInt8/16/32/64 для любых типов соответствующей битности.
struct SetMethodOneNumber
{
	typedef TData Data;
	typedef typename Data::key_type Key;

	Data data;

	/// Для использования одного Method в разных потоках, используйте разные State.
	struct State
	{
		const FieldType * vec;

		/** Вызывается в начале обработки каждого блока.
		  * Устанавливает переменные, необходимые для остальных методов, вызываемых во внутренних циклах.
		  */
		void init(const ConstColumnPlainPtrs & key_columns)
		{
			vec = &static_cast<const ColumnVector<FieldType> *>(key_columns[0])->getData()[0];
		}

		/// Достать из ключевых столбцов ключ для вставки в хэш-таблицу.
		Key getKey(
			const ConstColumnPlainPtrs & key_columns,	/// Ключевые столбцы.
			size_t keys_size,							/// Количество ключевых столбцов.
			size_t i,						/// Из какой строки блока достать ключ.
			const Sizes & key_sizes) const	/// Если ключи фиксированной длины - их длины. Не используется в методах по ключам переменной длины.
		{
			return unionCastToUInt64(vec[i]);
		}
	};

	/** Разместить дополнительные данные, если это необходимо, в случае, когда в хэш-таблицу был вставлен новый ключ.
	  */
	static void onNewKey(typename Data::value_type & value, size_t keys_size, size_t i, Arena & pool) {}
};

/// Для случая, когда есть один строковый ключ.
template <typename TData>
struct SetMethodString
{
	typedef TData Data;
	typedef typename Data::key_type Key;

	Data data;

	struct State
	{
		const ColumnString::Offsets_t * offsets;
		const ColumnString::Chars_t * chars;

		void init(const ConstColumnPlainPtrs & key_columns)
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
	};

	static void onNewKey(typename Data::value_type & value, size_t keys_size, size_t i, Arena & pool)
	{
		value.data = pool.insert(value.data, value.size);
	}
};

/// Для случая, когда есть один строковый ключ фиксированной длины.
template <typename TData>
struct SetMethodFixedString
{
	typedef TData Data;
	typedef typename Data::key_type Key;

	Data data;

	struct State
	{
		size_t n;
		const ColumnFixedString::Chars_t * chars;

		void init(const ConstColumnPlainPtrs & key_columns)
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
	};

	static void onNewKey(typename Data::value_type & value, size_t keys_size, size_t i, Arena & pool)
	{
		value.data = pool.insert(value.data, value.size);
	}
};

/// Для случая, когда все ключи фиксированной длины, и они помещаются в N (например, 128) бит.
template <typename TData>
struct SetMethodKeysFixed
{
	typedef TData Data;
	typedef typename Data::key_type Key;

	Data data;

	struct State
	{
		void init(const ConstColumnPlainPtrs & key_columns)
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
	};

	static void onNewKey(typename Data::value_type & value, size_t keys_size, size_t i, Arena & pool) {}
};

/// Для остальных случаев. По 128 битному хэшу от ключа. (При этом, строки, содержащие нули посередине, могут склеиться.)
template <typename TData>
struct SetMethodHashed
{
	typedef TData Data;
	typedef typename Data::key_type Key;

	Data data;

	struct State
	{
		void init(const ConstColumnPlainPtrs & key_columns)
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
	};

	static void onNewKey(typename Data::value_type & value, size_t keys_size, size_t i, Arena & pool) {}
};


/** Разные варианты реализации множества.
  */
struct SetVariants
{
	/// TODO Использовать для этих двух вариантов bit- или byte- set.
	std::unique_ptr<SetMethodOneNumber<UInt8, HashSet<UInt8, TrivialHash, HashTableFixedGrower<8>>>> 	key8;
	std::unique_ptr<SetMethodOneNumber<UInt16, HashSet<UInt16, TrivialHash, HashTableFixedGrower<16>>>> key16;

	/** Также для эксперимента проверялась возможность использовать SmallSet,
	  *  пока количество элементов в множестве небольшое (и, при необходимости, конвертировать в полноценный HashSet).
	  * Но этот эксперимент показал, что преимущество есть только в редких случаях.
	  */
	std::unique_ptr<SetMethodOneNumber<UInt32, HashSet<UInt32, HashCRC32<UInt32>>>> key32;
	std::unique_ptr<SetMethodOneNumber<UInt64, HashSet<UInt64, HashCRC32<UInt64>>>> key64;
	std::unique_ptr<SetMethodString<HashSetWithSavedHash<StringRef>>> 				key_string;
	std::unique_ptr<SetMethodFixedString<HashSetWithSavedHash<StringRef>>> 			key_fixed_string;
	std::unique_ptr<SetMethodKeysFixed<HashSet<UInt128, UInt128HashCRC32>>> 		keys128;
	std::unique_ptr<SetMethodKeysFixed<HashSet<UInt256, UInt256HashCRC32>>> 		keys256;
	std::unique_ptr<SetMethodHashed<HashSet<UInt128, UInt128TrivialHash>>> 			hashed;

	/** В отличие от Aggregator, здесь не используется метод concat.
	  * Это сделано потому что метод hashed, хоть и медленнее, но в данном случае, использует меньше оперативки.
	  *  так как при его использовании, сами значения ключей не сохраняются.
	  */

	Arena string_pool;

	#define APPLY_FOR_SET_VARIANTS(M) \
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

	#define M(NAME) NAME,
		APPLY_FOR_SET_VARIANTS(M)
	#undef M
	};

	Type type = Type::EMPTY;

	bool empty() const { return type == Type::EMPTY; }

	static Type chooseMethod(const ConstColumnPlainPtrs & key_columns, Sizes & key_sizes);

	void init(Type type_);

	size_t getTotalRowCount() const;
	/// Считает размер в байтах буфера Set и размер string_pool'а
	size_t getTotalByteCount() const;
};


/** Структура данных для реализации выражения IN.
  */
class Set
{
public:
	Set(const Limits & limits) :
		log(&Logger::get("Set")),
		max_rows(limits.max_rows_in_set),
		max_bytes(limits.max_bytes_in_set),
		overflow_mode(limits.set_overflow_mode)
	{
	}

	bool empty() const { return data.empty(); }

	/** Создать множество по выражению (для перечисления в самом запросе).
	  * types - типы того, что стоит слева от IN.
	  * node - это список значений: 1, 2, 3 или список tuple-ов: (1, 2), (3, 4), (5, 6).
	  * create_ordered_set - создавать ли вектор упорядоченных элементов. Нужен для работы индекса
	  */
	void createFromAST(DataTypes & types, ASTPtr node, const Context & context, bool create_ordered_set);

	// Возвращает false, если превышено какое-нибудь ограничение, и больше не нужно вставлять.
	bool insertFromBlock(const Block & block, bool create_ordered_set = false);

	/** Для указанных столбцов блока проверить принадлежность их значений множеству.
	  * Записать результат в столбец в позиции result.
	  */
	void execute(Block & block, const ColumnNumbers & arguments, size_t result, bool negative) const;

	std::string describe() const
	{
		if (!ordered_set_elements)
			return "{}";

		bool first = true;
		std::stringstream ss;

		ss << "{";
		for (const Field & f : *ordered_set_elements)
		{
			ss << (first ? "" : ", ") << apply_visitor(FieldVisitorToString(), f);
			first = false;
		}
		ss << "}";
		return ss.str();
	}

	/// проверяет есть ли в Set элементы для заданного диапазона индекса
	BoolMask mayBeTrueInRange(const Range & range) const;

	size_t getTotalRowCount() const { return data.getTotalRowCount(); }
	size_t getTotalByteCount() const { return data.getTotalByteCount(); }

private:
	Sizes key_sizes;

	SetVariants data;

	/** Типы данных, из которых было создано множество.
	  * При проверке на принадлежность множеству, типы проверяемых столбцов должны с ними совпадать.
	  */
	DataTypes data_types;

	Logger * log;

	/// Ограничения на максимальный размер множества
	size_t max_rows;
	size_t max_bytes;
	OverflowMode overflow_mode;

	/// Если в левой части IN стоит массив. Проверяем, что хоть один элемент массива лежит в множестве.
	void executeArray(const ColumnArray * key_column, ColumnUInt8::Container_t & vec_res, bool negative) const;

	/// Если в левой части набор столбцов тех же типов, что элементы множества.
	void executeOrdinary(const ConstColumnPlainPtrs & key_columns, ColumnUInt8::Container_t & vec_res, bool negative) const;

	/// Проверить не превышены ли допустимые размеры множества ключей
	bool checkSetSizeLimits() const;

	/// Вектор упорядоченных элементов Set.
	/// Нужен для работы индекса по первичному ключу в операторе IN.
	typedef std::vector<Field> OrderedSetElements;
	typedef std::unique_ptr<OrderedSetElements> OrderedSetElementsPtr;
	OrderedSetElementsPtr ordered_set_elements;

	/** Защищает работу с множеством в функциях insertFromBlock и execute.
	  * Эти функции могут вызываться одновременно из разных потоков только при использовании StorageSet,
	  *  и StorageSet вызывает только эти две функции.
	  * Поэтому остальные функции по работе с множеством, не защинены.
	  */
	mutable Poco::RWLock rwlock;


	template <typename Method>
	void insertFromBlockImpl(
		Method & method,
		const ConstColumnPlainPtrs & key_columns,
		size_t rows,
		SetVariants & variants);

	template <typename Method>
	void executeImpl(
		Method & method,
		const ConstColumnPlainPtrs & key_columns,
		ColumnUInt8::Container_t & vec_res,
		bool negative,
		size_t rows) const;

	template <typename Method>
	void executeArrayImpl(
		Method & method,
		const ConstColumnPlainPtrs & key_columns,
		const ColumnArray::Offsets_t & offsets,
		ColumnUInt8::Container_t & vec_res,
		bool negative,
		size_t rows) const;
};

typedef Poco::SharedPtr<Set> SetPtr;
typedef std::vector<SetPtr> Sets;


}
