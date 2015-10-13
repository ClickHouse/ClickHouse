#pragma once

#include <DB/Core/Field.h>
#include <DB/Core/FieldVisitors.h>
#include <DB/Parsers/IAST.h>


namespace DB
{


/** SET запрос
  */
class ASTSetQuery : public IAST
{
public:
	struct Change
	{
		String name;
		Field value;
	};

	typedef std::vector<Change> Changes;
	Changes changes;

	bool global;	/// Если запрос SET GLOBAL.

	ASTSetQuery() = default;
	ASTSetQuery(const StringRange range_) : IAST(range_) {}

	/** Получить текст, который идентифицирует этот элемент. */
	String getID() const override { return "Set"; };

	ASTPtr clone() const override { return new ASTSetQuery(*this); }

protected:
	void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override
	{
		settings.ostr << (settings.hilite ? hilite_keyword : "") << "SET " << (global ? "GLOBAL " : "") << (settings.hilite ? hilite_none : "");

		for (ASTSetQuery::Changes::const_iterator it = changes.begin(); it != changes.end(); ++it)
		{
			if (it != changes.begin())
				settings.ostr << ", ";

			settings.ostr << it->name << " = " << apply_visitor(FieldVisitorToString(), it->value);
		}
	}
};

}
