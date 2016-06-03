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

	/// Вызывает formatImplWithoutAlias, а также выводит алиас. Если надо - заключает всё выражение в скобки.
	void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override final;

	virtual void formatImplWithoutAlias(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const = 0;
};

/// helper for setting aliases and chaining result to other functions
inline ASTPtr setAlias(ASTPtr ast, const String & alias)
{
	ast->setAlias(alias);
	return ast;
};


}
