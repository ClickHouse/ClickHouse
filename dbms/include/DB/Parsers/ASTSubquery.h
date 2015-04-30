#pragma once

#include <DB/DataTypes/IDataType.h>

#include <DB/Parsers/IAST.h>


namespace DB
{


/** Подзарос SELECT
  */
class ASTSubquery : public IAST
{
public:
	ASTSubquery() = default;
	ASTSubquery(const StringRange range_) : IAST(range_) {}

	/** Получить текст, который идентифицирует этот элемент. */
	String getID() const override { return "Subquery"; }

	void updateHashWith(SipHash & hash) const override
	{
		hash.update("Subquery", strlen("Subquery") + 1);
	}

	ASTPtr clone() const override
	{
		const auto res = new ASTSubquery{*this};
		ASTPtr ptr{res};

		res->children.clear();

		for (const auto & child : children)
			res->children.emplace_back(child->clone());

		return ptr;
	}

	String getColumnName() const override
	{
		auto id = getTreeID();
		return toString(id.first) + "_" + toString(id.second);
	}
};

}
