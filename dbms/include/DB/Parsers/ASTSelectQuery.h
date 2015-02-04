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
	ASTPtr next_union_all; /// Следующий запрос SELECT в цепочке UNION ALL, если такой есть

	ASTSelectQuery() = default;
	ASTSelectQuery(const StringRange range_) : ASTQueryWithOutput(range_) {}

	/** Получить текст, который идентифицирует этот элемент. */
	String getID() const override { return "SelectQuery"; };

	/// Проверить наличие функции arrayJoin. (Не большого ARRAY JOIN.)
	static bool hasArrayJoin(const ASTPtr & ast)
	{
		if (const ASTFunction * function = typeid_cast<const ASTFunction *>(&*ast))
			if (function->kind == ASTFunction::ARRAY_JOIN)
				return true;

		for (const auto & child : ast->children)
			if (hasArrayJoin(child))
				return true;

		return false;
	}

	/// Содержит ли запрос астериск?
	bool hasAsterisk() const
	{
		for (const auto & ast : select_expression_list->children)
			if (typeid_cast<const ASTAsterisk *>(&*ast) != nullptr)
				return true;

		return false;
	}

	/// Переименовать столбцы запроса в такие же имена, как в исходном запросе.
	void renameColumns(const ASTSelectQuery & source)
	{
		const ASTs & from = source.select_expression_list->children;
		ASTs & to = select_expression_list->children;

		if (from.size() != to.size())
			throw Exception("Size mismatch in UNION ALL chain",
							DB::ErrorCodes::UNION_ALL_RESULT_STRUCTURES_MISMATCH);

		for (size_t i = 0; i < from.size(); ++i)
		{
			/// Если столбец имеет алиас, то он должен совпадать с названием исходного столбца.
			/// В противном случае мы ему присваиваем алиас, если требуется.
			if (!to[i]->tryGetAlias().empty())
			{
				if (to[i]->tryGetAlias() != from[i]->getAliasOrColumnName())
					throw Exception("Column alias mismatch in UNION ALL chain",
									DB::ErrorCodes::UNION_ALL_COLUMN_ALIAS_MISMATCH);
			}
			else if (to[i]->getColumnName() != from[i]->getAliasOrColumnName())
				to[i]->setAlias(from[i]->getAliasOrColumnName());
		}
	}

	/// Переписывает select_expression_list, чтобы вернуть только необходимые столбцы в правильном порядке.
	void rewriteSelectExpressionList(const Names & column_names)
	{
		ASTPtr result = new ASTExpressionList;
		ASTs asts = select_expression_list->children;

		/// Не будем выбрасывать выражения, содержащие функцию arrayJoin.
		std::set<ASTPtr> unremovable_asts;
		for (size_t j = 0; j < asts.size(); ++j)
		{
			if (hasArrayJoin(asts[j]))
			{
				result->children.push_back(asts[j]->clone());
				unremovable_asts.insert(asts[j]);
			}
		}

		for (const auto & column_name : column_names)
		{
			bool done = false;
			for (size_t j = 0; j < asts.size(); ++j)
			{
				if (asts[j]->getAliasOrColumnName() == column_name)
				{
					if (!unremovable_asts.count(asts[j]))
						result->children.push_back(asts[j]->clone());
					done = true;
				}
			}
			if (!done)
				throw Exception("Error while rewriting expression list for select query."
					" Could not find alias: " + column_name,
					DB::ErrorCodes::UNKNOWN_IDENTIFIER);
		}

		for (auto & child : children)
		{
			if (child == select_expression_list)
			{
				child = result;
				break;
			}
		}
		select_expression_list = result;

		/** NOTE: Может показаться, что мы могли испортить запрос, выбросив выражение с алиасом, который используется где-то еще.
		  *       Такого произойти не может, потому что этот метод вызывается всегда для запроса, на котором хоть раз создавали
		  *       ExpressionAnalyzer, что гарантирует, что в нем все алиасы уже подставлены. Не совсем очевидная логика :)
		  */
	}

	ASTPtr clone() const override
	{
		ASTSelectQuery * res = new ASTSelectQuery(*this);
		ASTPtr ptr{res};

		res->children.clear();

#define CLONE(member) if (member) { res->member = member->clone(); res->children.push_back(res->member); }

		/** NOTE Члены должны клонироваться точно в таком же порядке,
		  *  в каком они были вставлены в children в ParserSelectQuery.
		  * Это важно, потому что из имён children-ов составляется идентификатор (getTreeID),
		  *  который может быть использован для идентификаторов столбцов в случае подзапросов в операторе IN.
		  * При распределённой обработке запроса, в случае, если один из серверов localhost, а другой - нет,
		  *  запрос на localhost выполняется в рамках процесса и при этом клонируется,
		  *  а на удалённый сервер запрос отправляется в текстовом виде по TCP.
		  * И если порядок при клонировании не совпадает с порядком при парсинге,
		  *  то на разных серверах получатся разные идентификаторы.
		  */
		CLONE(select_expression_list)
		CLONE(database)
		CLONE(table)
		CLONE(array_join_expression_list)
		CLONE(join)
		CLONE(sample_size)
		CLONE(prewhere_expression)
		CLONE(where_expression)
		CLONE(group_expression_list)
		CLONE(having_expression)
		CLONE(order_expression_list)
		CLONE(limit_offset)
		CLONE(limit_length)
		CLONE(format)
		CLONE(next_union_all)

#undef CLONE

		return ptr;
	}
};

}
