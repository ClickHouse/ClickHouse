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

	bool empty() { return type == EMPTY; }

	/** Создать множество по выражению (для перечисления в самом запросе).
	  * types - типы того, что стоит слева от IN.
	  * node - это список значений: 1, 2, 3 или список tuple-ов: (1, 2), (3, 4), (5, 6).
	  * create_ordered_set - создавать ли вектор упорядоченных элементов. Нужен для работы индекса
	  */
	void createFromAST(DataTypes & types, ASTPtr node, bool create_ordered_set);

	// Возвращает false, если превышено какое-нибудь ограничение, и больше не нужно вставлять.
	bool insertFromBlock(const Block & block, bool create_ordered_set = false);

	/// Считает суммарное число ключей во всех Set'ах
	size_t getTotalRowCount() const;
	/// Считает суммарный размер в байтах буфферов всех Set'ов + размер string_pool'а
	size_t getTotalByteCount() const;

	/** Для указанных столбцов блока проверить принадлежность их значений множеству.
	  * Записать результат в столбец в позиции result.
	  */
	void execute(Block & block, const ColumnNumbers & arguments, size_t result, bool negative) const;

	std::string describe()
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
	BoolMask mayBeTrueInRange(const Range & range);

	enum Type
	{
		EMPTY 		= 0,
		KEY_64		= 1,
		KEY_STRING	= 2,
		HASHED		= 3,
	};

	static Type chooseMethod(const ConstColumnPlainPtrs & key_columns, bool & keys_fit_128_bits, Sizes & key_sizes);

private:
	/** Разные структуры данных, которые могут использоваться для проверки принадлежности
	  *  одного или нескольких столбцов значений множеству.
	  */
	typedef HashSet<UInt64, HashCRC32<UInt64>> SetUInt64;
	typedef HashSetWithSavedHash<StringRef> SetString;
	typedef HashSet<UInt128, UInt128HashCRC32> SetHashed;

	/// Специализация для случая, когда есть один числовой ключ.
	std::unique_ptr<SetUInt64> key64;

	/// Специализация для случая, когда есть один строковый ключ.
	std::unique_ptr<SetString> key_string;
	Arena string_pool;

	/** Сравнивает 128 битные хэши.
	  * Если все ключи фиксированной длины, влезающие целиком в 128 бит, то укладывает их без изменений в 128 бит.
	  * Иначе - вычисляет SipHash от набора из всех ключей.
	  * (При этом, строки, содержащие нули посередине, могут склеиться.)
	  */
	std::unique_ptr<SetHashed> hashed;

	Type type = EMPTY;

	bool keys_fit_128_bits;
	Sizes key_sizes;

	/** Типы данных, из которых было создано множество.
	  * При проверке на принадлежность множеству, типы проверяемых столбцов должны с ними совпадать.
	  */
	DataTypes data_types;

	Logger * log;

	/// Ограничения на максимальный размер множества
	size_t max_rows;
	size_t max_bytes;
	OverflowMode overflow_mode;

	void init(Type type_)
	{
		type = type_;

		switch (type)
		{
			case EMPTY:			break;
			case KEY_64:		key64		.reset(new SetUInt64); 	break;
			case KEY_STRING:	key_string	.reset(new SetString); 	break;
			case HASHED:		hashed		.reset(new SetHashed);	break;

			default:
				throw Exception("Unknown Set variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
		}
	}

	/// Если в левой части IN стоит массив. Проверяем, что хоть один элемент массива лежит в множестве.
	void executeConstArray(const ColumnConstArray * key_column, ColumnUInt8::Container_t & vec_res, bool negative) const;
	void executeArray(const ColumnArray * key_column, ColumnUInt8::Container_t & vec_res, bool negative) const;

	/// Если в левой части набор столбцов тех же типов, что элементы множества.
	void executeOrdinary(const ConstColumnPlainPtrs & key_columns, ColumnUInt8::Container_t & vec_res, bool negative) const;

	/// Проверить не превышены ли допустимые размеры множества ключей
	bool checkSetSizeLimits() const;

	/// вектор упорядоченных элементов Set
	/// нужен для работы индекса по первичному ключу в секции In
	typedef std::vector<Field> OrderedSetElements;
	typedef std::unique_ptr<OrderedSetElements> OrderedSetElementsPtr;
	OrderedSetElementsPtr ordered_set_elements;

	/** Защищает работу с множеством в функциях insertFromBlock и execute.
	  * Эти функции могут вызываться одновременно из разных потоков только при использовании StorageSet,
	  *  и StorageSet вызывает только эти две функции.
	  * Поэтому остальные функции по работе с множеством, не защинены.
	  */
	mutable Poco::RWLock rwlock;
};

typedef Poco::SharedPtr<Set> SetPtr;
typedef std::vector<SetPtr> Sets;


}
