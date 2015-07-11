#include <DB/Parsers/ASTSelectQuery.h>

namespace DB
{

ASTSelectQuery::ASTSelectQuery(const StringRange range_) : ASTQueryWithOutput(range_)
{
}

bool ASTSelectQuery::hasArrayJoin(const ASTPtr & ast)
{
	if (const ASTFunction * function = typeid_cast<const ASTFunction *>(&*ast))
		if (function->kind == ASTFunction::ARRAY_JOIN)
			return true;

	for (const auto & child : ast->children)
		if (hasArrayJoin(child))
			return true;

	return false;
}

bool ASTSelectQuery::hasAsterisk() const
{
	for (const auto & ast : select_expression_list->children)
		if (typeid_cast<const ASTAsterisk *>(&*ast) != nullptr)
			return true;

	return false;
}

void ASTSelectQuery::renameColumns(const ASTSelectQuery & source)
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

void ASTSelectQuery::rewriteSelectExpressionList(const Names & column_names)
{
	ASTPtr result = new ASTExpressionList;
	ASTs asts = select_expression_list->children;

	/// Создать отображение.

	/// Элемент отображения.
	struct Arrow
	{
		Arrow() = default;
		Arrow(size_t to_position_) :
			to_position(to_position_), is_selected(true)
		{
		}
		size_t to_position = 0;
		bool is_selected = false;
	};

	/// Отображение одного SELECT выражения в другое.
	using Mapping = std::vector<Arrow>;

	Mapping mapping(asts.size());
	std::vector<size_t> from(column_names.size());

	/// Не будем выбрасывать выражения, содержащие функцию arrayJoin.
	for (size_t i = 0; i < asts.size(); ++i)
	{
		if (hasArrayJoin(asts[i]))
			mapping[i] = Arrow(i);
	}

	for (size_t i = 0; i < column_names.size(); ++i)
	{
		bool done = false;
		for (size_t j = 0; j < asts.size(); ++j)
		{
			if (asts[j]->getAliasOrColumnName() == column_names[i])
			{
				from[i] = j;
				done = true;
				break;
			}
		}
		if (!done)
			throw Exception("Error while rewriting expression list for select query."
				" Could not find alias: " + column_names[i],
				DB::ErrorCodes::UNKNOWN_IDENTIFIER);
	}

	auto to = from;
	std::sort(from.begin(), from.end());

	for (size_t i = 0; i < column_names.size(); ++i)
		mapping[from[i]] = Arrow(to[i]);

	/// Составить новое выражение.
	for (const auto & arrow : mapping)
	{
		if (arrow.is_selected)
			result->children.push_back(asts[arrow.to_position]->clone());
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

ASTPtr ASTSelectQuery::clone() const
{
	ASTPtr ptr = cloneImpl(true);

	/// Установить указатели на предыдущие запросы SELECT.
	ASTPtr current = ptr;
	ASTPtr next = static_cast<ASTSelectQuery *>(&*current)->next_union_all;
	while (!next.isNull())
	{
		ASTSelectQuery * next_select_query = static_cast<ASTSelectQuery *>(&*next);
		next_select_query->prev_union_all = current;
		current = next;
		next = next_select_query->next_union_all;
	}

	return ptr;
}

ASTPtr ASTSelectQuery::cloneFirstSelect() const
{
	ASTPtr res = cloneImpl(false);
	static_cast<ASTSelectQuery *>(&*res)->prev_union_all = nullptr;
	return res;
}

ASTPtr ASTSelectQuery::cloneImpl(bool traverse_union_all) const
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
	CLONE(settings)
	CLONE(format)

#undef CLONE

	if (traverse_union_all)
	{
		if (next_union_all)
		{
			res->next_union_all = static_cast<const ASTSelectQuery *>(&*next_union_all)->cloneImpl(true);
			res->children.push_back(res->next_union_all);
		}
	}
	else
		res->next_union_all = nullptr;

	return ptr;
}

const IAST * ASTSelectQuery::getFormat() const
{
	const ASTSelectQuery * query = this;
	while (!query->next_union_all.isNull())
		query = static_cast<const ASTSelectQuery *>(query->next_union_all.get());
	return query->format.get();
}

};

