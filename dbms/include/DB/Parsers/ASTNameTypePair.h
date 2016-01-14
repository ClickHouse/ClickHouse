#pragma once

#include <DB/Parsers/IAST.h>
#include <DB/Parsers/ASTFunction.h>


namespace DB
{

/** Пара из имени и типа. Например, browser FixedString(2).
  */
class ASTNameTypePair : public IAST
{
public:
	/// имя
	String name;
	/// тип
	ASTPtr type;

    ASTNameTypePair() = default;
    ASTNameTypePair(const StringRange range_) : IAST(range_) {}

	/** Получить текст, который идентифицирует этот элемент. */
	String getID() const override { return "NameTypePair_" + name; }

	ASTPtr clone() const override
	{
		ASTNameTypePair * res = new ASTNameTypePair(*this);
		ASTPtr ptr{res};

		res->children.clear();

		if (type) 	{ res->type = type->clone(); 	res->children.push_back(res->type); }

		return ptr;
	}

protected:
	void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override
	{
		std::string indent_str = settings.one_line ? "" : std::string(4 * frame.indent, ' ');

		settings.ostr << settings.nl_or_ws << indent_str << backQuoteIfNeed(name) << " ";
		type->formatImpl(settings, state, frame);
	}
};


}

