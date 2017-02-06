#include <DB/Common/Collator.h>
#include <DB/Parsers/ASTOrderByElement.h>


namespace DB
{

void ASTOrderByElement::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
	children.front()->formatImpl(settings, state, frame);
	settings.ostr << (settings.hilite ? hilite_keyword : "")
		<< (direction == -1 ? " DESC" : " ASC")
		<< (settings.hilite ? hilite_none : "");

	if (collation)
	{
		settings.ostr << (settings.hilite ? hilite_keyword : "") << " COLLATE " << (settings.hilite ? hilite_none : "");
		collation->formatImpl(settings, state, frame);
	}
}

}
