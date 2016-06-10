#pragma once

#include <DB/Parsers/IAST.h>


namespace DB
{

/** Запрос с секцией FORMAT.
  */
class ASTQueryWithOutput : public IAST
{
public:
	ASTPtr format;

	ASTQueryWithOutput() = default;
	ASTQueryWithOutput(const StringRange range_) : IAST(range_) {}

	/** Возвращает указатель на формат. Если типом объекта является ASTSelectQuery,
	  * то эта функция возвращает указатель на формат из последнего SELECT'а цепочки UNION ALL.
	  */
	virtual const IAST * getFormat() const { return format.get(); }
};


/// Объявляет класс-наследник ASTQueryWithOutput с реализованными методами getID и clone.
#define DEFINE_AST_QUERY_WITH_OUTPUT(Name, ID, Query) \
class Name : public ASTQueryWithOutput \
{ \
public: \
	Name() {} \
	Name(StringRange range_) : ASTQueryWithOutput(range_) {} \
	String getID() const override { return ID; }; \
	\
	ASTPtr clone() const override \
	{ \
		std::shared_ptr<Name> res = std::make_shared<Name>(*this); \
		res->children.clear(); \
		if (format) \
		{ \
			res->format = format->clone(); \
			res->children.push_back(res->format); \
		} \
		return res; \
	} \
\
protected: \
	void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override \
	{ \
		settings.ostr << (settings.hilite ? hilite_keyword : "") << Query << (settings.hilite ? hilite_none : ""); \
	} \
};

}
