#pragma once

#include <DB/IO/WriteBufferFromOStream.h>
#include <DB/Parsers/IAST.h>


namespace DB
{

/** Базовый класс для AST, которые могут содержать алиас (идентификаторы, литералы, функции).
  */
class ASTWithAlias : public IAST
{
public:
	/// Алиас, если есть, или пустая строка.
	String alias;

	using IAST::IAST;

	String getAliasOrColumnName() const override 	{ return alias.empty() ? getColumnName() : alias; }
	String tryGetAlias() const override 			{ return alias; }
	void setAlias(const String & to) override 		{ alias = to; }

	void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override final
	{
		if (!alias.empty())
		{
			/// Если мы уже ранее вывели этот узел в другом месте запроса, то теперь достаточно вывести лишь алиас.
			if (!state.printed_asts_with_alias.insert(this).second)
			{
				WriteBufferFromOStream wb(settings.ostr, 32);
				writeProbablyBackQuotedString(alias, wb);
				return;
			}
		}

		/// Если есть алиас, то требуются скобки вокруг всего выражения, включая алиас. Потому что запись вида 0 AS x + 0 синтаксически некорректна.
		if (frame.need_parens && !alias.empty())
			settings.ostr <<'(';

		formatImplWithAlias(settings, state, frame);

		if (!alias.empty())
		{
			writeAlias(alias, settings.ostr, settings.hilite);
			if (frame.need_parens)
				settings.ostr <<')';
		}
	}

	virtual void formatImplWithAlias(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const = 0;
};

/// helper for setting aliases and chaining result to other functions
inline ASTPtr setAlias(ASTPtr ast, const String & alias) {
	ast->setAlias(alias);
	return ast;
};


}
