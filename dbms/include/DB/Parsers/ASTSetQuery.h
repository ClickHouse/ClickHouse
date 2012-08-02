#pragma once

#include <DB/Core/Field.h>
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

	ASTSetQuery() {}
	ASTSetQuery(StringRange range_) : IAST(range_) {}
	
	/** Получить текст, который идентифицирует этот элемент. */
	String getID() { return "Set"; };

	ASTPtr clone() const { return new ASTSetQuery(*this); }
};

}
