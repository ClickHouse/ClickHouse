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
	
	ASTSet() {}
	ASTSet(StringRange range_) : IAST(range_) {}
	String getID() { return "Set"; }
	ASTPtr clone() const { return new ASTSet(*this); }
};

}
