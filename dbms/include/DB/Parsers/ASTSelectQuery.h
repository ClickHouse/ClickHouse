#pragma once

#include <DB/Parsers/IAST.h>
#include <DB/Parsers/ASTQueryWithOutput.h>
#include <DB/Parsers/ASTExpressionList.h>
#include <DB/Parsers/ASTFunction.h>
#include <DB/Parsers/ASTAsterisk.h>

namespace DB
{

/** SELECT запрос
  */
class ASTSelectQuery : public ASTQueryWithOutput
{
public:
	ASTSelectQuery() = default;
	ASTSelectQuery(const StringRange range_);

	/** Получить текст, который идентифицирует этот элемент. */
	String getID() const override { return "SelectQuery"; };

	/// Проверить наличие функции arrayJoin. (Не большого ARRAY JOIN.)
	static bool hasArrayJoin(const ASTPtr & ast);

	/// Содержит ли запрос астериск?
	bool hasAsterisk() const;

	/// Переименовать столбцы запроса в такие же имена, как в исходном запросе.
	void renameColumns(const ASTSelectQuery & source);

	/// Переписывает select_expression_list, чтобы вернуть только необходимые столбцы в правильном порядке.
	void rewriteSelectExpressionList(const Names & column_names);

	bool isUnionAllHead() const { return prev_union_all.isNull() && !next_union_all.isNull(); }

	ASTPtr clone() const override;

	/// Возвращает указатель на формат из последнего SELECT'а цепочки UNION ALL.
	const IAST * getFormat() const override;

public:
	bool distinct = false;
	ASTPtr select_expression_list;
	ASTPtr database;
	ASTPtr table;	/// Идентификатор, табличная функция или подзапрос (рекурсивно ASTSelectQuery)
	ASTPtr array_join_expression_list;	/// ARRAY JOIN
	ASTPtr join;						/// Обычный (не ARRAY) JOIN.
	bool final = false;
	ASTPtr sample_size;
	ASTPtr prewhere_expression;
	ASTPtr where_expression;
	ASTPtr group_expression_list;
	bool group_by_with_totals = false;
	ASTPtr having_expression;
	ASTPtr order_expression_list;
	ASTPtr limit_offset;
	ASTPtr limit_length;
	ASTPtr settings;
	ASTPtr prev_union_all;
	ASTPtr next_union_all; /// Следующий запрос SELECT в цепочке UNION ALL, если такой есть
};

}
