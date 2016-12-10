#pragma once

#include <DB/Parsers/IAST.h>


namespace DB
{

/** Элемент выражения, после которого стоит ASC или DESC
  */
class ASTOrderByElement : public IAST
{
public:
	int direction;	/// 1, если ASC, -1, если DESC

	/** Collation for locale-specific string comparison. If empty, then sorting done by bytes. */
	ASTPtr collation;

	ASTOrderByElement() = default;
	ASTOrderByElement(const StringRange range_, const int direction_, ASTPtr & collation_)
		: IAST(range_), direction(direction_), collation(collation_) {}

	/** Получить текст, который идентифицирует этот элемент. */
	String getID() const override { return "OrderByElement"; }

	ASTPtr clone() const override { return std::make_shared<ASTOrderByElement>(*this); }

protected:
	void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
