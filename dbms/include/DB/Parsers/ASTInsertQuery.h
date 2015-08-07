#pragma once

#include <DB/Parsers/ASTExpressionList.h>
#include <DB/Parsers/ASTFunction.h>


namespace DB
{


/** INSERT запрос
  */
class ASTInsertQuery : public IAST
{
public:
	String database;
	String table;
	ASTPtr columns;
	String format;
	ASTPtr select;
	/// Идентификатор запроса INSERT. Используется при репликации.
	String insert_id;
	/// Данные для вставки
	const char * data = nullptr;
	const char * end = nullptr;

	ASTInsertQuery() = default;
	ASTInsertQuery(const StringRange range_) : IAST(range_) {}
	
	/** Получить текст, который идентифицирует этот элемент. */
	String getID() const override { return "InsertQuery_" + database + "_" + table; };

	ASTPtr clone() const override
	{
		ASTInsertQuery * res = new ASTInsertQuery(*this);
		ASTPtr ptr{res};

		res->children.clear();

		if (columns) 	{ res->columns = columns->clone(); 	res->children.push_back(res->columns); }
		if (select) 	{ res->select = select->clone(); 	res->children.push_back(res->select); }

		return ptr;
	}

protected:
	void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override
	{
		frame.need_parens = false;

		settings.ostr << (settings.hilite ? hilite_keyword : "") << "INSERT INTO " << (settings.hilite ? hilite_none : "")
		<< (!database.empty() ? backQuoteIfNeed(database) + "." : "") << backQuoteIfNeed(table);

		if (!insert_id.empty())
			settings.ostr << (settings.hilite ? hilite_keyword : "") << " ID = " << (settings.hilite ? hilite_none : "")
			<< mysqlxx::quote << insert_id;

		if (columns)
		{
			settings.ostr << " (";
			columns->formatImpl(settings, state, frame);
			settings.ostr << ")";
		}

		if (select)
		{
			settings.ostr << " ";
			select->formatImpl(settings, state, frame);
		}
		else
		{
			if (!format.empty())
			{
				settings.ostr << (settings.hilite ? hilite_keyword : "") << " FORMAT " << (settings.hilite ? hilite_none : "") << format;
			}
			else
			{
				settings.ostr << (settings.hilite ? hilite_keyword : "") << " VALUES" << (settings.hilite ? hilite_none : "");
			}
		}
	}
};

}
