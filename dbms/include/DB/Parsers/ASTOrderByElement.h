#pragma once

#include <DB/Parsers/IAST.h>
#include <DB/Common/Collator.h>

namespace DB
{

using Poco::SharedPtr;


/** Элемент выражения, после которого стоит ASC или DESC
  */
class ASTOrderByElement : public IAST
{
public:
	int direction;	/// 1, если ASC, -1, если DESC
	
	/** Collator для locale-specific сортировки строк.
	 * Если NULL, то производится сортировка по байтам.
	 */
	Poco::SharedPtr<Collator> collator;
	
	ASTOrderByElement() {}
	ASTOrderByElement(StringRange range_, int direction_, const Poco::SharedPtr<Collator> & collator_ = nullptr)
		: IAST(range_), direction(direction_), collator(collator_) {}
	
	/** Получить текст, который идентифицирует этот элемент. */
	String getID() const { return "OrderByElement"; }

	ASTPtr clone() const { return new ASTOrderByElement(*this); }
};

}
