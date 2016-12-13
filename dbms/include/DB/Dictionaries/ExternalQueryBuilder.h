#pragma once

#include <string>
#include <DB/Columns/IColumn.h>


namespace DB
{

struct DictionaryStructure;
class WriteBuffer;


/** Генерирует запрос для загрузки данных из внешней БД.
  */
struct ExternalQueryBuilder
{
	const DictionaryStructure & dict_struct;
	const std::string & db;
	const std::string & table;
	const std::string & where;


	ExternalQueryBuilder(
		const DictionaryStructure & dict_struct,
		const std::string & db,
		const std::string & table,
		const std::string & where);

	/** Получить запрос на загрузку всех данных. */
	std::string composeLoadAllQuery() const;

	/** Получить запрос на загрузку данных по множеству простых ключей. */
	std::string composeLoadIdsQuery(const std::vector<UInt64> & ids);

	/** Получить запрос на загрузку данных по множеству сложных ключей.
	  * Есть два метода их указания в секции WHERE:
	  * 1. (x = c11 AND y = c12) OR (x = c21 AND y = c22) ...
	  * 2. (x, y) IN ((c11, c12), (c21, c22), ...)
	  */
	enum LoadKeysMethod
	{
		AND_OR_CHAIN,
		IN_WITH_TUPLES,
	};

	std::string composeLoadKeysQuery(
		const ConstColumnPlainPtrs & key_columns,
		const std::vector<size_t> & requested_rows,
		LoadKeysMethod method);


private:
	/// Выражение вида (x = c1 AND y = c2 ...)
	void composeKeyCondition(const ConstColumnPlainPtrs & key_columns, const size_t row, WriteBuffer & out) const;

	/// Выражение вида (x, y, ...)
	std::string composeKeyTupleDefinition() const;

	/// Выражение вида (c1, c2, ...)
	void composeKeyTuple(const ConstColumnPlainPtrs & key_columns, const size_t row, WriteBuffer & out) const;
};

}
