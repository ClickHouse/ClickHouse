#pragma once

#include <set>

#include <statdaemons/Stopwatch.h>

#include <Yandex/logger_useful.h>

#include <DB/Core/ColumnNumbers.h>
#include <DB/Common/Arena.h>

#include <DB/DataStreams/IBlockInputStream.h>

#include <DB/Parsers/IAST.h>

#include <DB/Interpreters/HashSet.h>
#include <DB/Interpreters/AggregationCommon.h>
#include <DB/Interpreters/Limits.h>
#include <DB/Columns/ColumnConst.h>
#include <DB/Columns/ColumnArray.h>


namespace DB
{


/** Структура данных для реализации выражения IN.
  */
class Set
{
public:
	Set(const Limits & limits)
		: type(EMPTY), log(&Logger::get("Set")),
		max_rows(limits.max_rows_in_set),
		max_bytes(limits.max_bytes_in_set),
		overflow_mode(limits.set_overflow_mode)
	{
	}
	
	bool empty() { return type == EMPTY; }

	/** Создать множество по потоку блоков (для подзапроса). */
	void create(BlockInputStreamPtr stream);

	/** Создать множество по выражению (для перечисления в самом запросе).
	  * types - типы того, что стоит слева от IN.
	  * node - это список значений: 1, 2, 3 или список tuple-ов: (1, 2), (3, 4), (5, 6).
	  */
	void create(DataTypes & types, ASTPtr node);

	/** Для указанных столбцов блока проверить принадлежность их значений множеству.
	  * Записать результат в столбец в позиции result.
	  */
	void execute(Block & block, const ColumnNumbers & arguments, size_t result, bool negative) const;
	
private:
	/** Разные структуры данных, которые могут использоваться для проверки принадлежности
	  *  одного или нескольких столбцов значений множеству.
	  */
	typedef HashSet<UInt64> SetUInt64;
	typedef HashSet<StringRef, StringRefHash, StringRefZeroTraits> SetString;
	typedef HashSet<UInt128, UInt128Hash, UInt128ZeroTraits> SetHashed;

	/// Специализация для случая, когда есть один числовой ключ.
	SetUInt64 key64;

	/// Специализация для случая, когда есть один строковый ключ.
	SetString key_string;
	Arena string_pool;

	/** Сравнивает 128 битные хэши.
	  * Если все ключи фиксированной длины, влезающие целиком в 128 бит, то укладывает их без изменений в 128 бит.
	  * Иначе - вычисляет SipHash от набора из всех ключей.
	  * (При этом, строки, содержащие нули посередине, могут склеиться.)
	  */
	SetHashed hashed;

	enum Type
	{
		EMPTY 		= 0,
		KEY_64		= 1,
		KEY_STRING	= 2,
		HASHED		= 3,
	};
	Type type;
	
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
	Limits::OverflowMode overflow_mode;
	
	static Type chooseMethod(const DataTypes & key_types, bool & keys_fit_128_bits, Sizes & key_sizes);

	/// Если в левой части IN стоит массив. Проверяем, что хоть один элемент массива лежит в множестве.
	void executeConstArray(const ColumnConstArray * key_column, ColumnUInt8::Container_t & vec_res, bool negative) const;
	void executeArray(const ColumnArray * key_column, ColumnUInt8::Container_t & vec_res, bool negative) const;
	
	/// Если в левой части набор столбцов тех же типов, что элементы множества.
	void executeOrdinary(const ConstColumnPlainPtrs & key_columns, ColumnUInt8::Container_t & vec_res, bool negative) const;
	
	/** Вывести в лог информацию о скорости создания множества.
	  */
	void logProfileInfo(Stopwatch & watch, IBlockInputStream & in, size_t entries);
	
	/// Проверить не превышены ли допустимые размеры множества ключей
	bool checkSetSizeLimits() const;
	
	/// Считает суммарное число ключей во всех Set'ах
	size_t getTotalRowCount() const;
	/// Считает суммарный размер в байтах буфферов всех Set'ов + размер string_pool'а
	size_t getTotalByteCount() const;
};

typedef SharedPtr<Set> SetPtr;


}
