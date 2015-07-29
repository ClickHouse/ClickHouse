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
};


/// Объявляет класс-наследник ASTQueryWithOutput с реализованными методами getID и clone.
#define DEFINE_AST_QUERY_WITH_OUTPUT(Name, ID) \
class Name : public ASTQueryWithOutput \
{ \
public: \
	Name() {} \
	Name(StringRange range_) : ASTQueryWithOutput(range_) {} \
	String getID() const override { return ID; }; \
	\
	ASTPtr clone() const override \
	{ \
		Name * res = new Name(*this); \
		ASTPtr ptr{res}; \
		res->children.clear(); \
		if (format) \
		{ \
			res->format = format->clone(); \
			res->children.push_back(res->format); \
		} \
		return ptr; \
	} \
};

}
