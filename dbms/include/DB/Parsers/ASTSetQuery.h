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

	ASTSetQuery() = default;
	ASTSetQuery(const StringRange range_) : IAST(range_) {}
	
	/** Получить текст, который идентифицирует этот элемент. */
	String getID() const override { return "Set"; };

	ASTPtr clone() const override { return new ASTSetQuery(*this); }
};

}
