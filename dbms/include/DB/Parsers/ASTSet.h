#pragma once

#include <DB/Interpreters/Set.h>
#include <DB/Parsers/IAST.h>


namespace DB
{

/** Множество. В процессе вычисления, на множество заменяется выражение в секции IN
  *  - подзапрос или явное перечисление значений.
  */
class ASTSet : public IAST
{
public:
	SetPtr set;
	String column_name;
	
	ASTSet(const String & column_name_) : column_name(column_name_) {}
	ASTSet(StringRange range_, const String & column_name_) : IAST(range_), column_name(column_name_) {}
	String getID() { return "Set"; }
	ASTPtr clone() const { return new ASTSet(*this); }
	String getColumnName() { return column_name; }
};

}
