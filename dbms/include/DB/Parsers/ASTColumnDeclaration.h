#pragma once

#include <DB/Parsers/IAST.h>

namespace DB
{

/** Name, type, default-specifier, default-expression.
 *  The type is optional if default-expression is specified.
 */
class ASTColumnDeclaration : public IAST
{
public:
	String name;
	ASTPtr type;
	String default_specifier;
	ASTPtr default_expression;

    ASTColumnDeclaration() = default;
    ASTColumnDeclaration(const StringRange range) : IAST{range} {}

	String getID() const override { return "ColumnDeclaration_" + name; }

	void updateHashWith(SipHash & hash) const override
	{
		hash.update("ColumnDeclaration", strlen("ColumnDeclaration") + 1);
		hash.update(name.data(), name.size() + 1);
	}

	ASTPtr clone() const override
	{
		const auto res = new ASTColumnDeclaration{*this};
		ASTPtr ptr{res};

		res->children.clear();

		if (type) {
			res->type = type->clone();
			res->children.push_back(res->type);
		}

		if (default_expression) {
			res->default_expression = default_expression->clone();
			res->children.push_back(res->default_expression);
		}

		return ptr;
	}
};

}
