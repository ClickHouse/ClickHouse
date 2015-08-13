#pragma once

#include <DB/Parsers/IAST.h>


namespace DB
{

/** Звёздочка.
  */
class ASTAsterisk : public IAST
{
public:
	ASTAsterisk() = default;
	ASTAsterisk(StringRange range_) : IAST(range_) {}
	String getID() const override { return "Asterisk"; }
	ASTPtr clone() const override { return new ASTAsterisk(*this); }
	String getColumnName() const override { return "*"; }

protected:
	void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override
	{
		settings.ostr << "*";
	}
};

}
