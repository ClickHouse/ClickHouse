#pragma once

#include <DB/Interpreters/Context.h>
#include <DB/DataStreams/AddingConstColumnBlockInputStream.h>
#include <DB/DataStreams/OneBlockInputStream.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/ASTExpressionList.h>
#include <DB/Parsers/ASTLiteral.h>
#include <DB/Parsers/ASTSelectQuery.h>
#include <DB/Columns/ColumnString.h>

namespace DB
{

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

/// Получить поток блоков содержащий интересующие нас значения виртуальных столбцов
/// На вход подается исходный запрос, блок с значениями виртуальных столбцов и контекст
BlockInputStreamPtr getVirtualColumnsBlocks(ASTPtr query, const Block & input, const Context & context);

/// Извлечь из входного потока множество значений столбца name
template<typename T1>
std::multiset<T1> extractSingleValueFromBlocks(BlockInputStreamPtr input, const String & name)
{
	std::multiset<T1> res;
	input->readPrefix();
	while(1)
	{
		Block block = input->read();
		if (!block) break;
		const ColumnWithNameAndType & data = block.getByName(name);
		for (size_t i = 0; i < block.rows(); ++i)
			res.insert((*data.column)[i].get<T1>());
	}
	return res;
}

}

}
