#pragma once

#include <set>

#include <DB/Core/Block.h>
#include <DB/Core/NamesAndTypes.h>
#include <DB/Parsers/IAST.h>


namespace DB
{

class Context;


namespace VirtualColumnUtils
{

/// Вычислить минимальный числовый суффикс, который надо добавить к строке, чтобы она не присутствовала в множестве
String chooseSuffix(const NamesAndTypesList & columns, const String & name);

/// Вычислить минимальный общий числовый суффикс, который надо добавить к каждой строке,
/// чтобы ни одна не присутствовала в множестве.
String chooseSuffixForSet(const NamesAndTypesList & columns, const std::vector<String> & names);

/// Добавляет в селект запрос секцию select column_name as value
/// Например select _port as 9000.
void rewriteEntityInAst(ASTPtr ast, const String & column_name, const Field & value);

/// Оставить в блоке только строки, подходящие под секции WHERE и PREWHERE запроса.
/// Рассматриваются только элементы внешней конъюнкции, зависящие только от столбцов, присутствующих в блоке.
/// Возвращает true, если хоть одна строка выброшена.
bool filterBlockWithQuery(ASTPtr query, Block & block, const Context & context);

/// Извлечь из входного потока множество значений столбца name
template<typename T1>
std::multiset<T1> extractSingleValueFromBlock(const Block & block, const String & name)
{
	std::multiset<T1> res;
	const ColumnWithNameAndType & data = block.getByName(name);
	size_t rows = block.rows();
	for (size_t i = 0; i < rows; ++i)
		res.insert((*data.column)[i].get<T1>());
	return res;
}

}

}
