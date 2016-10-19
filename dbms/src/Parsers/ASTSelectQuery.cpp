#include <DB/Core/FieldVisitors.h>
#include <DB/Parsers/ASTSetQuery.h>
#include <DB/Parsers/ASTSelectQuery.h>
#include <DB/Parsers/ASTTablesInSelectQuery.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int UNION_ALL_COLUMN_ALIAS_MISMATCH;
	extern const int UNION_ALL_RESULT_STRUCTURES_MISMATCH;
	extern const int UNKNOWN_IDENTIFIER;
	extern const int LOGICAL_ERROR;
}


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

void ASTSelectQuery::rewriteSelectExpressionList(const Names & required_column_names)
{
	ASTPtr result = std::make_shared<ASTExpressionList>();
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

	/// На какой позиции в SELECT-выражении находится соответствующий столбец из column_names.
	std::vector<size_t> positions_of_required_columns(required_column_names.size());

	/// Не будем выбрасывать выражения, содержащие функцию arrayJoin.
	for (size_t i = 0; i < asts.size(); ++i)
	{
		if (hasArrayJoin(asts[i]))
			mapping[i] = Arrow(i);
	}

	for (size_t i = 0; i < required_column_names.size(); ++i)
	{
		size_t j = 0;
		for (; j < asts.size(); ++j)
		{
			if (asts[j]->getAliasOrColumnName() == required_column_names[i])
			{
				positions_of_required_columns[i] = j;
				break;
			}
		}
		if (j == asts.size())
			throw Exception("Error while rewriting expression list for select query."
				" Could not find alias: " + required_column_names[i],
				DB::ErrorCodes::UNKNOWN_IDENTIFIER);
	}

	std::vector<size_t> positions_of_required_columns_in_subquery_order = positions_of_required_columns;
	std::sort(positions_of_required_columns_in_subquery_order.begin(), positions_of_required_columns_in_subquery_order.end());

	for (size_t i = 0; i < required_column_names.size(); ++i)
		mapping[positions_of_required_columns_in_subquery_order[i]] = Arrow(positions_of_required_columns[i]);


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
		*       ExpressionAnalyzer, что гарантирует, что в нем все алиасы уже подставлены. Не совсем очевидная логика.
		*/
}

ASTPtr ASTSelectQuery::clone() const
{
	ASTPtr ptr = cloneImpl(true);

	/// Установить указатели на предыдущие запросы SELECT.
	ASTPtr current = ptr;
	static_cast<ASTSelectQuery *>(&*current)->prev_union_all = nullptr;
	ASTPtr next = static_cast<ASTSelectQuery *>(&*current)->next_union_all;
	while (next != nullptr)
	{
		ASTSelectQuery * next_select_query = static_cast<ASTSelectQuery *>(&*next);
		next_select_query->prev_union_all = current.get();
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
	auto res = std::make_shared<ASTSelectQuery>(*this);
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
	CLONE(tables)
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

	return res;
}

const IAST * ASTSelectQuery::getFormat() const
{
	const ASTSelectQuery * query = this;
	while (query->next_union_all != nullptr)
		query = static_cast<const ASTSelectQuery *>(query->next_union_all.get());
	return query->format.get();
}


void ASTSelectQuery::formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const
{
	frame.current_select = this;
	frame.need_parens = false;
	std::string indent_str = s.one_line ? "" : std::string(4 * frame.indent, ' ');

	s.ostr << (s.hilite ? hilite_keyword : "") << indent_str << "SELECT " << (distinct ? "DISTINCT " : "") << (s.hilite ? hilite_none : "");

	s.one_line
		? select_expression_list->formatImpl(s, state, frame)
		: typeid_cast<const ASTExpressionList &>(*select_expression_list).formatImplMultiline(s, state, frame);

	if (tables)
	{
		s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << indent_str << "FROM " << (s.hilite ? hilite_none : "");
		tables->formatImpl(s, state, frame);
	}

	if (prewhere_expression)
	{
		s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << indent_str << "PREWHERE " << (s.hilite ? hilite_none : "");
		prewhere_expression->formatImpl(s, state, frame);
	}

	if (where_expression)
	{
		s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << indent_str << "WHERE " << (s.hilite ? hilite_none : "");
		where_expression->formatImpl(s, state, frame);
	}

	if (group_expression_list)
	{
		s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << indent_str << "GROUP BY " << (s.hilite ? hilite_none : "");
		s.one_line
			? group_expression_list->formatImpl(s, state, frame)
			: typeid_cast<const ASTExpressionList &>(*group_expression_list).formatImplMultiline(s, state, frame);
	}

	if (group_by_with_totals)
		s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << indent_str << (s.one_line ? "" : "    ") << "WITH TOTALS" << (s.hilite ? hilite_none : "");

	if (having_expression)
	{
		s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << indent_str << "HAVING " << (s.hilite ? hilite_none : "");
		having_expression->formatImpl(s, state, frame);
	}

	if (order_expression_list)
	{
		s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << indent_str << "ORDER BY " << (s.hilite ? hilite_none : "");
		s.one_line
			? order_expression_list->formatImpl(s, state, frame)
			: typeid_cast<const ASTExpressionList &>(*order_expression_list).formatImplMultiline(s, state, frame);
	}

	if (limit_length)
	{
		s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << indent_str << "LIMIT " << (s.hilite ? hilite_none : "");
		if (limit_offset)
		{
			limit_offset->formatImpl(s, state, frame);
			s.ostr << ", ";
		}
		limit_length->formatImpl(s, state, frame);
	}

	if (settings)
	{
		s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << indent_str << "SETTINGS " << (s.hilite ? hilite_none : "");

		const ASTSetQuery & ast_set = typeid_cast<const ASTSetQuery &>(*settings);
		for (ASTSetQuery::Changes::const_iterator it = ast_set.changes.begin(); it != ast_set.changes.end(); ++it)
		{
			if (it != ast_set.changes.begin())
				s.ostr << ", ";

			s.ostr << it->name << " = " << apply_visitor(FieldVisitorToString(), it->value);
		}
	}

	if (format)
	{
		s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << indent_str << "FORMAT " << (s.hilite ? hilite_none : "");
		format->formatImpl(s, state, frame);
	}

	if (next_union_all)
	{
		s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << indent_str << "UNION ALL " << s.nl_or_ws << (s.hilite ? hilite_none : "");

		// NOTE Мы можем безопасно применить static_cast вместо typeid_cast, потому что знаем, что в цепочке UNION ALL
		// имеются только деревья типа SELECT.
		const ASTSelectQuery & next_ast = static_cast<const ASTSelectQuery &>(*next_union_all);

		next_ast.formatImpl(s, state, frame);
	}
}


/// Compatibility functions. TODO Remove.


static const ASTTableExpression * getFirstTableExpression(const ASTSelectQuery & select)
{
	if (!select.tables)
		return {};

	const ASTTablesInSelectQuery & tables_in_select_query = static_cast<const ASTTablesInSelectQuery &>(*select.tables);
	if (tables_in_select_query.children.empty())
		return {};

	const ASTTablesInSelectQueryElement & tables_element = static_cast<const ASTTablesInSelectQueryElement &>(*tables_in_select_query.children[0]);
	if (!tables_element.table_expression)
		return {};

	return static_cast<const ASTTableExpression *>(tables_element.table_expression.get());
}

static ASTTableExpression * getFirstTableExpression(ASTSelectQuery & select)
{
	if (!select.tables)
		return {};

	ASTTablesInSelectQuery & tables_in_select_query = static_cast<ASTTablesInSelectQuery &>(*select.tables);
	if (tables_in_select_query.children.empty())
		return {};

	ASTTablesInSelectQueryElement & tables_element = static_cast<ASTTablesInSelectQueryElement &>(*tables_in_select_query.children[0]);
	if (!tables_element.table_expression)
		return {};

	return static_cast<ASTTableExpression *>(tables_element.table_expression.get());
}

static const ASTArrayJoin * getFirstArrayJoin(const ASTSelectQuery & select)
{
	if (!select.tables)
		return {};

	const ASTTablesInSelectQuery & tables_in_select_query = static_cast<const ASTTablesInSelectQuery &>(*select.tables);
	if (tables_in_select_query.children.empty())
		return {};

	const ASTArrayJoin * array_join = nullptr;
	for (const auto & child : tables_in_select_query.children)
	{
		const ASTTablesInSelectQueryElement & tables_element = static_cast<const ASTTablesInSelectQueryElement &>(*child);
		if (tables_element.array_join)
		{
			if (!array_join)
				array_join = static_cast<const ASTArrayJoin *>(tables_element.array_join.get());
			else
				throw Exception("Support for more than one ARRAY JOIN in query is not implemented", ErrorCodes::NOT_IMPLEMENTED);
		}
	}

	return array_join;
}

static const ASTTablesInSelectQueryElement * getFirstTableJoin(const ASTSelectQuery & select)
{
	if (!select.tables)
		return {};

	const ASTTablesInSelectQuery & tables_in_select_query = static_cast<const ASTTablesInSelectQuery &>(*select.tables);
	if (tables_in_select_query.children.empty())
		return {};

	const ASTTablesInSelectQueryElement * joined_table = nullptr;
	for (const auto & child : tables_in_select_query.children)
	{
		const ASTTablesInSelectQueryElement & tables_element = static_cast<const ASTTablesInSelectQueryElement &>(*child);
		if (tables_element.table_join)
		{
			if (!joined_table)
				joined_table = &tables_element;
			else
				throw Exception("Support for more than one JOIN in query is not implemented", ErrorCodes::NOT_IMPLEMENTED);
		}
	}

	return joined_table;
}


ASTPtr ASTSelectQuery::database() const
{
	const ASTTableExpression * table_expression = getFirstTableExpression(*this);
	if (!table_expression || !table_expression->database_and_table_name || table_expression->database_and_table_name->children.empty())
		return {};

	if (table_expression->database_and_table_name->children.size() != 2)
		throw Exception("Logical error: more than two components in table expression", ErrorCodes::LOGICAL_ERROR);

	return table_expression->database_and_table_name->children[0];
}


ASTPtr ASTSelectQuery::table() const
{
	const ASTTableExpression * table_expression = getFirstTableExpression(*this);
	if (!table_expression)
		return {};

	if (table_expression->database_and_table_name)
	{
		if (table_expression->database_and_table_name->children.empty())
			return table_expression->database_and_table_name;

		if (table_expression->database_and_table_name->children.size() != 2)
			throw Exception("Logical error: more than two components in table expression", ErrorCodes::LOGICAL_ERROR);

		return table_expression->database_and_table_name->children[1];
	}

	if (table_expression->table_function)
		return table_expression->table_function;

	if (table_expression->subquery)
		return static_cast<const ASTSubquery *>(table_expression->subquery.get())->children.at(0);

	throw Exception("Logical error: incorrect table expression", ErrorCodes::LOGICAL_ERROR);
}


ASTPtr ASTSelectQuery::sample_size() const
{
	const ASTTableExpression * table_expression = getFirstTableExpression(*this);
	if (!table_expression)
		return {};

	return table_expression->sample_size;
}


ASTPtr ASTSelectQuery::sample_offset() const
{
	const ASTTableExpression * table_expression = getFirstTableExpression(*this);
	if (!table_expression)
		return {};

	return table_expression->sample_offset;
}


bool ASTSelectQuery::final() const
{
	const ASTTableExpression * table_expression = getFirstTableExpression(*this);
	if (!table_expression)
		return {};

	return table_expression->final;
}


ASTPtr ASTSelectQuery::array_join_expression_list() const
{
	const ASTArrayJoin * array_join = getFirstArrayJoin(*this);
	if (!array_join)
		return {};

	return array_join->expression_list;
}


bool ASTSelectQuery::array_join_is_left() const
{
	const ASTArrayJoin * array_join = getFirstArrayJoin(*this);
	if (!array_join)
		return {};

	return array_join->kind == ASTArrayJoin::Kind::Left;
}


const ASTTablesInSelectQueryElement * ASTSelectQuery::join() const
{
	return getFirstTableJoin(*this);
}


void ASTSelectQuery::setDatabaseIfNeeded(const String & database_name)
{
	ASTTableExpression * table_expression = getFirstTableExpression(*this);
	if (!table_expression)
		return;

	if (!table_expression->database_and_table_name)
		return;

	if (table_expression->database_and_table_name->children.empty())
	{
		ASTPtr database = std::make_shared<ASTIdentifier>(StringRange(), database_name, ASTIdentifier::Database);
		ASTPtr table = table_expression->database_and_table_name;

		const String & old_name = static_cast<ASTIdentifier &>(*table_expression->database_and_table_name).name;
		table_expression->database_and_table_name = std::make_shared<ASTIdentifier>(StringRange(), database_name + "." + old_name, ASTIdentifier::Table);
		table_expression->database_and_table_name->children = {database, table};
	}
	else if (table_expression->database_and_table_name->children.size() != 2)
	{
		throw Exception("Logical error: more than two components in table expression", ErrorCodes::LOGICAL_ERROR);
	}
}


void ASTSelectQuery::replaceDatabaseAndTable(const String & database_name, const String & table_name)
{
	ASTTableExpression * table_expression = getFirstTableExpression(*this);

	if (!table_expression)
	{
		auto tables_list = std::make_shared<ASTTablesInSelectQuery>();
		auto element = std::make_shared<ASTTablesInSelectQueryElement>();
		auto table_expr = std::make_shared<ASTTableExpression>();
		element->table_expression = table_expr;
		element->children.emplace_back(table_expr);
		tables_list->children.emplace_back(element);
		tables = tables_list;
		children.emplace_back(tables_list);
		table_expression = table_expr.get();
	}

	ASTPtr table = std::make_shared<ASTIdentifier>(StringRange(), table_name, ASTIdentifier::Table);

	if (!database_name.empty())
	{
		ASTPtr database = std::make_shared<ASTIdentifier>(StringRange(), database_name, ASTIdentifier::Database);

		table_expression->database_and_table_name = std::make_shared<ASTIdentifier>(
			StringRange(), database_name + "." + table_name, ASTIdentifier::Table);
		table_expression->database_and_table_name->children = {database, table};
	}
	else
	{
		table_expression->database_and_table_name = std::make_shared<ASTIdentifier>(
			StringRange(), table_name, ASTIdentifier::Table);
	}
}

};

