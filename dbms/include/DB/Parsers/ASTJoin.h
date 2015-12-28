#pragma once

#include <DB/Parsers/IAST.h>

namespace DB
{


/** [GLOBAL] ANY|ALL INNER|LEFT JOIN (subquery) USING (tuple)
  * Замечание: ARRAY JOIN к этому не относится.
  */
class ASTJoin : public IAST
{
public:
	/// Алгоритм работы при распределённой обработке запроса.
	enum Locality
	{
		Local,	/// Выполнить JOIN, используя соответствующие данные в пределах каждого удалённого сервера.
		Global	/// Собрать соединяемые данные со всех удалённых серверов, объединить их, затем отправить на каждый удалённый сервер.
	};

	/// Оптимизация JOIN-а для большинства простых случаев.
	enum Strictness
	{
		Any,	/// Если в "правой" таблице несколько подходящих строк - выбрать из них одну, любую.
		All		/// Если в "правой" таблице несколько подходящих строк - размножить строки по ним (обычная семантика JOIN-а).
	};

	/// Способ соединения.
	enum Kind
	{
		Inner,	/// Оставить только записи, для которых в "правой" таблице есть соответствующая.
		Left,	/// Если в "правой" таблице нет соответствующих записей, заполнить столбцы значениями "по-умолчанию".
		Right,
		Full,
		Cross	/// Прямое произведение. strictness и using_expr_list не используются.
	};

	Locality locality = Local;
	Strictness strictness = Any;
	Kind kind = Inner;

	ASTPtr table;				/// "Правая" таблица для соединения - подзапрос или имя таблицы.
	ASTPtr using_expr_list;		/// По каким столбцам выполнять соединение.
	ASTPtr on_expr;				/// Условие соединения. Поддерживается либо USING либо ON, но не одновременно.

	ASTJoin() = default;
	ASTJoin(const StringRange range_) : IAST(range_) {}

	/** Получить текст, который идентифицирует этот элемент. */
	String getID() const override
	{
		String res;
		{
			WriteBufferFromString wb(res);

			if (locality == Global)
				writeString("Global", wb);

			writeString(strictness == Any ? "Any" : "All", wb);

			writeString(
				kind == Inner ? "Inner"
				: (kind == Left ? "Left"
				: (kind == Right ? "Right"
				: (kind == Full ? "Full"
				: "Cross"))), wb);

			writeString("Join", wb);
		}

		return res;
	};

	ASTPtr clone() const override
	{
		ASTJoin * res = new ASTJoin(*this);
		ASTPtr ptr{res};

		res->children.clear();

		if (table) 				{ res->table 			= table->clone(); 			res->children.push_back(res->table); }
		if (using_expr_list) 	{ res->using_expr_list 	= using_expr_list->clone(); res->children.push_back(res->using_expr_list); }

		return ptr;
	}

protected:
	void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override
	{
		frame.need_parens = false;

		settings.ostr << (settings.hilite ? hilite_keyword : "");

		if (locality == ASTJoin::Global)
			settings.ostr << "GLOBAL ";

		if (kind != ASTJoin::Cross)
			settings.ostr << (strictness == ASTJoin::Any ? "ANY " : "ALL ");

		settings.ostr << (kind == ASTJoin::Inner ? "INNER "
			: (kind == ASTJoin::Left ? "LEFT "
			: (kind == ASTJoin::Right ? "RIGHT "
			: (kind == ASTJoin::Cross ? "CROSS "
			: "FULL OUTER "))));

		settings.ostr << "JOIN "
			<< (settings.hilite ? hilite_none : "");

		table->formatImpl(settings, state, frame);

		if (using_expr_list)
		{
			settings.ostr << (settings.hilite ? hilite_keyword : "") << " USING " << (settings.hilite ? hilite_none : "");
			using_expr_list->formatImpl(settings, state, frame);
		}
		else if (on_expr)
		{
			settings.ostr << (settings.hilite ? hilite_keyword : "") << " ON " << (settings.hilite ? hilite_none : "");
			on_expr->formatImpl(settings, state, frame);
		}
	}
};

}
