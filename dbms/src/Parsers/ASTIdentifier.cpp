#include <DB/Parsers/ASTIdentifier.h>


namespace DB
{

void ASTIdentifier::formatImplWithoutAlias(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
	auto format_element = [&](const String & name)
	{
		settings.ostr << (settings.hilite ? hilite_identifier : "");

		WriteBufferFromOStream wb(settings.ostr, 32);
		writeProbablyBackQuotedString(name, wb);
		wb.next();

		settings.ostr << (settings.hilite ? hilite_none : "");
	};

	/// Простой или составной идентификатор?

	if (children.size() > 1)
	{
		for (size_t i = 0, size = children.size(); i < size; ++i)
		{
			if (i != 0)
				settings.ostr << '.';

			format_element(static_cast<const ASTIdentifier &>(*children[i].get()).name);
		}
	}
	else
	{
		format_element(name);
	}
}

}
