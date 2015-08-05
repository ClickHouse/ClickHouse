#include <DB/Parsers/ASTSetQuery.h>
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
	static_cast<ASTSelectQuery *>(&*current)->prev_union_all = nullptr;
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


void ASTSelectQuery::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override
{
	frame.need_parens = false;
	std::string indent_str = settings.one_line ? "" : std::string(4 * frame.indent, ' ');

	settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << "SELECT " << (distinct ? "DISTINCT " : "") << (settings.hilite ? hilite_none : "");

	settings.one_line
		? select_expression_list->formatImpl(settings, state, frame)
		: typeid_cast<const ASTExpressionList &>(*select_expression_list).formatImplMultiline(settings, state, frame);

	if (table)
	{
		settings.ostr << (settings.hilite ? hilite_keyword : "") << settings.nl_or_ws << settings.ostr << indent_str << "FROM " << (settings.hilite ? hilite_none : "");
		if (database)
		{
			database->formatImpl(settings, state, frame);
			settings.ostr << ".";
		}

		if (typeid_cast<const ASTSelectQuery *>(&*table))
		{
			if (settings.one_line)
				settings.ostr << " (";
			else
				settings.ostr << "\n" << indent_str << "(\n";

			table->formatImpl(settings, state, frame);

			if (settings.one_line)
				settings.ostr << ")";
			else
				settings.ostr << "\n" << indent_str << ")";
		}
		else
			table->formatImpl(settings, state, frame);
	}

	if (final)
	{
		settings.ostr << (settings.hilite ? hilite_keyword : "") << settings.nl_or_ws << settings.ostr << indent_str << "FINAL" << (settings.hilite ? hilite_none : "");
	}

	if (sample_size)
	{
		settings.ostr << (settings.hilite ? hilite_keyword : "") << settings.nl_or_ws << settings.ostr << indent_str << "SAMPLE " << (settings.hilite ? hilite_none : "");
		sample_size->formatImpl(settings, state, frame);
	}

	if (array_join_expression_list)
	{
		settings.ostr << (settings.hilite ? hilite_keyword : "") << settings.nl_or_ws << settings.ostr << indent_str
		<< (array_join_is_left ? "LEFT " : "") << "ARRAY JOIN " << (settings.hilite ? hilite_none : "");

		settings.one_line
			? array_join_expression_list->formatImpl(settings, state, frame)
			: typeid_cast<const ASTExpressionList &>(*array_join_expression_list).formatImplMultiline(settings, state, frame);
	}

	if (join)
	{
		settings.ostr << " ";
		join->formatImpl(settings, state, frame);
	}

	if (prewhere_expression)
	{
		settings.ostr << (settings.hilite ? hilite_keyword : "") << settings.nl_or_ws << settings.ostr << indent_str << "PREWHERE " << (settings.hilite ? hilite_none : "");
		prewhere_expression->formatImpl(settings, state, frame);
	}

	if (where_expression)
	{
		settings.ostr << (settings.hilite ? hilite_keyword : "") << settings.nl_or_ws << settings.ostr << indent_str << "WHERE " << (settings.hilite ? hilite_none : "");
		where_expression, s, indent, hilite, settings.one_line);
	}

	if (group_expression_list)
	{
		settings.ostr << (settings.hilite ? hilite_keyword : "") << settings.nl_or_ws << settings.ostr << indent_str << "GROUP BY " << (settings.hilite ? hilite_none : "");
		settings.one_line
			? group_expression_list->formatImpl(settings, state, frame)
			: typeid_cast<const ASTExpressionList &>(*group_expression_list).formatImplMultiline(settings, state, frame);
	}

	if (group_by_with_totals)
		settings.ostr << (settings.hilite ? hilite_keyword : "") << settings.nl_or_ws << settings.ostr << indent_str << (settings.one_line ? "" : "    ") << "WITH TOTALS" << (settings.hilite ? hilite_none : "");

	if (having_expression)
	{
		settings.ostr << (settings.hilite ? hilite_keyword : "") << settings.nl_or_ws << settings.ostr << indent_str << "HAVING " << (settings.hilite ? hilite_none : "");
		having_expression->formatImpl(settings, state, frame);
	}

	if (order_expression_list)
	{
		settings.ostr << (settings.hilite ? hilite_keyword : "") << settings.nl_or_ws << settings.ostr << indent_str << "ORDER BY " << (settings.hilite ? hilite_none : "");
		settings.one_line
			? order_expression_list->formatImpl(settings, state, frame)
			: typeid_cast<const ASTExpressionList &>(*order_expression_list).formatImplMultiline(settings, state, frame);
	}

	if (limit_length)
	{
		settings.ostr << (settings.hilite ? hilite_keyword : "") << settings.nl_or_ws << settings.ostr << indent_str << "LIMIT " << (settings.hilite ? hilite_none : "");
		if (limit_offset)
		{
			limit_offset->formatImpl(settings, state, frame);
			settings.ostr << ", ";
		}
		limit_length->formatImpl(settings, state, frame);
	}

	if (settings)
	{
		settings.ostr << (settings.hilite ? hilite_keyword : "") << settings.nl_or_ws << settings.ostr << indent_str << "SETTINGS " << (settings.hilite ? hilite_none : "");

		const ASTSetQuery & ast_set = typeid_cast<const ASTSetQuery &>(*settings);
		for (ASTSetQuery::Changes::const_iterator it = ast_set.changes.begin(); it != ast_set.changes.end(); ++it)
		{
			if (it != ast_set.changes.begin())
				settings.ostr << ", ";

			settings.ostr << it->name << " = " << apply_visitor(FieldVisitorToString(), it->value);
		}
	}

	if (format)
	{
		settings.ostr << (settings.hilite ? hilite_keyword : "") << settings.nl_or_ws << settings.ostr << indent_str << "FORMAT " << (settings.hilite ? hilite_none : "");
		format->formatImpl(settings, state, frame);
	}

	if (next_union_all)
	{
		settings.ostr << (settings.hilite ? hilite_keyword : "") << settings.nl_or_ws << settings.ostr << indent_str << "UNION ALL " << settings.nl_or_ws << settings.ostr << (settings.hilite ? hilite_none : "");

		// NOTE Мы можем безопасно применить static_cast вместо typeid_cast, потому что знаем, что в цепочке UNION ALL
		// имеются только деревья типа SELECT.
		const ASTSelectQuery & next_ast = static_cast<const ASTSelectQuery &>(*next_union_all);

		next_ast->formatImpl(settings, state, frame);
	}
}

};

