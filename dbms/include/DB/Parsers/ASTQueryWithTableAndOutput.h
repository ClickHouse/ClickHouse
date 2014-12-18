#pragma once

#include <DB/Parsers/IAST.h>
#include <DB/Parsers/ASTQueryWithOutput.h>


namespace DB
{
	
	
	/** Запрос с указанием названия таблицы и, возможно, БД и секцией FORMAT.
	 */
	class ASTQueryWithTableAndOutput : public ASTQueryWithOutput
	{
	public:
		String database;
		String table;
		
		ASTQueryWithTableAndOutput() = default;
		ASTQueryWithTableAndOutput(const StringRange range_) : ASTQueryWithOutput(range_) {}
	};
	
	
/// Объявляет класс-наследник ASTQueryWithTableAndOutput с реализованными методами getID и clone.
#define DEFINE_AST_QUERY_WITH_TABLE_AND_OUTPUT(Name, ID) \
	class Name : public ASTQueryWithTableAndOutput \
	{ \
public: \
		Name() = default;												\
		Name(const StringRange range_) : ASTQueryWithTableAndOutput(range_) {} \
		String getID() const override { return ID"_" + database + "_" + table; }; \
	\
		ASTPtr clone() const override \
		{ \
			Name * res = new Name(*this); \
			ASTPtr ptr{res};			  \
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
