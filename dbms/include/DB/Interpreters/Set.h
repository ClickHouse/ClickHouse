#pragma once

#include <set>

#include <statdaemons/Stopwatch.h>

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
friend class Join;

public:
	Set(const Limits & limits)
		: max_bytes_to_transfer(limits.max_bytes_to_transfer),
		max_rows_to_transfer(limits.max_rows_to_transfer),
		transfer_overflow_mode(limits.transfer_overflow_mode),
		bytes_in_external_table(0),
		rows_in_external_table(0),
		only_external(false),
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

	/** Запомнить поток блоков (для подзапросов), чтобы потом его можно было прочитать и создать множество.
	  */
	void setSource(BlockInputStreamPtr stream) { source = stream; }

	void setExternalOutput(StoragePtr storage) { external_table = storage; }
	void setOnlyExternal(bool flag) { only_external = flag; }

	BlockInputStreamPtr getSource() { return source; }

	// Возвращает false, если превышено какое-нибудь ограничение, и больше не нужно вставлять.
	bool insertFromBlock(Block & block, bool create_ordered_set = false);

	size_t size() const { return getTotalRowCount(); }

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
	
private:
	/** Разные структуры данных, которые могут использоваться для проверки принадлежности
	  *  одного или нескольких столбцов значений множеству.
	  */
	typedef HashSet<UInt64> SetUInt64;
	typedef HashSetWithSavedHash<StringRef> SetString;
	typedef HashSet<UInt128, UInt128Hash> SetHashed;

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

	enum Type
	{
		EMPTY 		= 0,
		KEY_64		= 1,
		KEY_STRING	= 2,
		HASHED		= 3,
	};
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
	
	static Type chooseMethod(const ConstColumnPlainPtrs & key_columns, bool & keys_fit_128_bits, Sizes & key_sizes);

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
	/// Проверить не превышены ли допустимые размеры внешней таблицы для передачи данных
	bool checkExternalSizeLimits() const;

	/// Считает суммарное число ключей во всех Set'ах
	size_t getTotalRowCount() const;
	/// Считает суммарный размер в байтах буфферов всех Set'ов + размер string_pool'а
	size_t getTotalByteCount() const;

	/// вектор упорядоченных элементов Set
	/// нужен для работы индекса по первичному ключу в секции In
	typedef std::vector<Field> OrderedSetElements;
	typedef std::unique_ptr<OrderedSetElements> OrderedSetElementsPtr;
	OrderedSetElementsPtr ordered_set_elements;
};

typedef Poco::SharedPtr<Set> SetPtr;
typedef std::vector<SetPtr> Sets;


}
