#pragma once

#include <set>

#include <statdaemons/Stopwatch.h>

#include <DB/Core/ColumnNumbers.h>
#include <DB/Core/StringPool.h>

#include <DB/DataStreams/IBlockInputStream.h>

#include <DB/Parsers/IAST.h>

#include <DB/Interpreters/HashSet.h>
#include <DB/Interpreters/AggregationCommon.h>


namespace DB
{


/** Структура данных для реализации выражения IN.
  */
class Set
{
public:
	Set() : type(EMPTY), log(&Logger::get("Set")) {}
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
	typedef std::set<Row> SetGeneric;
	typedef HashSet<UInt64> SetUInt64;
	typedef HashSet<StringRef, StringRefHash, StringRefZeroTraits> SetString;
	typedef HashSet<UInt128, UInt128Hash, UInt128ZeroTraits> SetHashed;

	/// Наиболее общий вариант. Самый медленный. На данный момент, не используется.
	SetGeneric generic;

	/// Специализация для случая, когда есть один числовой ключ (не с плавающей запятой).
	SetUInt64 key64;

	/// Специализация для случая, когда есть один строковый ключ.
	SetString key_string;
	StringPool string_pool;

	/** Сравнивает 128 битные хэши.
	  * Если все ключи фиксированной длины, влезающие целиком в 128 бит, то укладывает их без изменений в 128 бит.
	  * Иначе - вычисляет md5 от набора из всех ключей.
	  * (При этом, строки, содержащие нули посередине, могут склеиться.)
	  */
	SetHashed hashed;

	enum Type
	{
		EMPTY 		= 0,
		GENERIC 	= 1,
		KEY_64		= 2,
		KEY_STRING	= 3,
		HASHED		= 4,
	};
	Type type;

	/** Типы данных, из которых было создано множество.
	  * При проверке на принадлежность множеству, типы проверяемых столбцов должны с ними совпадать.
	  */
	DataTypes data_types;
	
	Logger * log;
	
	typedef std::vector<size_t> Sizes;
	static Type chooseMethod(Columns & key_columns, bool & keys_fit_128_bits, Sizes & key_sizes);

	/** Вывести в лог информацию о скорости создания множества.
	  */
	void logProfileInfo(Stopwatch & watch, IBlockInputStream & in, size_t entries);
};

typedef SharedPtr<Set> SetPtr;


}
