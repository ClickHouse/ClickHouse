#pragma once

#include <set>

#include <DB/Parsers/IAST.h>

#include <DB/Interpreters/HashSet.h>
#include <DB/Interpreters/Aggregator.h>


namespace DB
{


/** Структура данных для реализации выражения IN.
  */
struct Set
{
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

	Set() : type(EMPTY) {}
	bool empty() { return type == EMPTY; }


	/** Создать множество по потоку блоков (для подзапроса). */
	void create(BlockInputStreamPtr stream) { /* TODO */ }

	/** Создать множество по выражению (для перечисления в самом запросе). */
	void create(ASTPtr node);

	/** Для указанных столбцов блока проверить принадлежность их значений множеству.
	  * Записать результат в столбец в позиции result.
	  */
	void execute(Block & block, const ColumnNumbers & arguments, size_t result) const { /* TODO */ }
};

typedef SharedPtr<Set> SetPtr;


}
