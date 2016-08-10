#pragma once

#include <DB/Parsers/IAST.h>
#include <DB/Parsers/ASTQueryWithOutput.h>
#include <DB/Parsers/ASTExpressionList.h>
#include <DB/Parsers/ASTFunction.h>
#include <DB/Parsers/ASTAsterisk.h>

namespace DB
{

struct ASTTablesInSelectQueryElement;


/** SELECT query
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

	bool isUnionAllHead() const { return (prev_union_all == nullptr) && next_union_all != nullptr; }

	ASTPtr clone() const override;

	/// Получить глубокую копию дерева первого запроса SELECT.
	ASTPtr cloneFirstSelect() const;

	/// Возвращает указатель на формат из последнего SELECT'а цепочки UNION ALL.
	const IAST * getFormat() const override;

private:
	ASTPtr cloneImpl(bool traverse_union_all) const;

public:
	bool distinct = false;
	ASTPtr select_expression_list;
	ASTPtr tables;
	ASTPtr prewhere_expression;
	ASTPtr where_expression;
	ASTPtr group_expression_list;
	bool group_by_with_totals = false;
	ASTPtr having_expression;
	ASTPtr order_expression_list;
	ASTPtr limit_offset;
	ASTPtr limit_length;
	ASTPtr settings;

	/// Compatibility with old parser of tables list. TODO remove
	ASTPtr database() const;
	ASTPtr table() const;
	ASTPtr sample_size() const;
	ASTPtr sample_offset() const;
	ASTPtr array_join_expression_list() const;
	const ASTTablesInSelectQueryElement * join() const;
	bool array_join_is_left() const;
	bool final() const;
	void setDatabaseIfNeeded(const String & database_name);
	void replaceDatabaseAndTable(const String & database_name, const String & table_name);

	/// Двусвязный список запросов SELECT внутри запроса UNION ALL.

	/// Следующий запрос SELECT в цепочке UNION ALL, если такой есть
	ASTPtr next_union_all;
	/// Предыдущий запрос SELECT в цепочке UNION ALL (не вставляется в children и не клонируется)
	/// Указатель голый по следующим причинам:
	/// 1. чтобы предотвратить появление циклических зависимостей и, значит, утечки памяти;
	IAST * prev_union_all = nullptr;

protected:
	void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
