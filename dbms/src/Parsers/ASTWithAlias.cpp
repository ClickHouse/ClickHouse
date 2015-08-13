#include <DB/Parsers/ASTWithAlias.h>

namespace DB
{

void ASTWithAlias::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
	if (!alias.empty())
	{
		/// Если мы уже ранее вывели этот узел в другом месте запроса, то теперь достаточно вывести лишь алиас.
		if (!state.printed_asts_with_alias.emplace(frame.current_select, alias).second)
		{
			WriteBufferFromOStream wb(settings.ostr, 32);
			writeProbablyBackQuotedString(alias, wb);
			return;
		}
	}

	/// Если есть алиас, то требуются скобки вокруг всего выражения, включая алиас. Потому что запись вида 0 AS x + 0 синтаксически некорректна.
	if (frame.need_parens && !alias.empty())
		settings.ostr <<'(';

	formatImplWithoutAlias(settings, state, frame);

	if (!alias.empty())
	{
		writeAlias(alias, settings.ostr, settings.hilite);
		if (frame.need_parens)
			settings.ostr <<')';
	}
}

}
