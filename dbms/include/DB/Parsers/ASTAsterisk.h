#pragma once

#include <DB/Parsers/IAST.h>


namespace DB
{

/** Звёздочка.
  */
class ASTAsterisk : public IAST
{
public:
	ASTAsterisk() {}
	ASTAsterisk(StringRange range_) : IAST(range_) {}
	String getID() { return "Asterisk"; }
};

}
