#pragma once

#include <DB/DataTypes/IDataType.h>

#include <DB/Parsers/ASTWithAlias.h>


namespace DB
{


/** Подзарос SELECT
  */
class ASTSubquery : public ASTWithAlias
{
public:
	ASTSubquery() = default;
	ASTSubquery(const StringRange range_) : ASTWithAlias(range_) {}

	/** Получить текст, который идентифицирует этот элемент. */
	String getID() const override { return "Subquery"; }

	ASTPtr clone() const override
	{
		const auto res = new ASTSubquery{*this};
		ASTPtr ptr{res};

		res->children.clear();

		for (const auto & child : children)
			res->children.emplace_back(child->clone());

		return ptr;
	}

	String getColumnName() const override { return getTreeID(); }

protected:
	void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override
	{
		/// Если есть алиас, то требуются скобки вокруг всего выражения, включая алиас. Потому что запись вида 0 AS x + 0 синтаксически некорректна.
		if (frame.need_parens && !alias.empty())
			settings.ostr << '(';

		std::string indent_str = settings.one_line ? "" : std::string(4 * frame.indent, ' ');
		std::string nl_or_nothing = settings.one_line ? "" : "\n";

		settings.ostr << nl_or_nothing << indent_str << "(" << nl_or_nothing;
		FormatStateStacked frame_dont_need_parens = frame;
		frame_dont_need_parens.need_parens = false;
		children[0]->formatImpl(settings, state, frame_dont_need_parens);
		settings.ostr << nl_or_nothing << indent_str << ")";

		if (!alias.empty())
		{
			writeAlias(alias, settings.ostr, settings.hilite);
			if (frame.need_parens)
				settings.ostr << ')';
		}
	}
};

}
