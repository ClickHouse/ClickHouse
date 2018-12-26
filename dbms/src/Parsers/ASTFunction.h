#pragma once

#include <Parsers/ASTWithAlias.h>
#include <Parsers/ASTExpressionList.h>


namespace DB
{

/** AST for function application or operator.
  */
class ASTFunction : public ASTWithAlias
{
public:
    String name;
    ASTPtr arguments;
    /// parameters - for parametric aggregate function. Example: quantile(0.9)(x) - what in first parens are 'parameters'.
    ASTPtr parameters;

public:
    /** Get text identifying the AST node. */
    String getID(char delim) const override;

    ASTPtr clone() const override;

protected:
    void formatImplWithoutAlias(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
    void appendColumnNameImpl(WriteBuffer & ostr) const override;
};


template <typename... Args>
std::shared_ptr<ASTFunction> makeASTFunction(const String & name, Args &&... args)
{
    const auto function = std::make_shared<ASTFunction>();

    function->name = name;
    function->arguments = std::make_shared<ASTExpressionList>();
    function->children.push_back(function->arguments);

    function->arguments->children = { std::forward<Args>(args)... };

    return function;
}

/// key-value just a pair "key value" separated by space, where key just a word (USER, PASSWORD, HOST etc) and value is just a literal.
/// KeyValueFunction is a function which arguments consist of either key-value pairs or another KeyValueFunction
/// For example: SOURCE(USER 'clickhouse' PASSWORD 'qwerty123' PORT 9000 REPLICA(HOST '127.0.0.1' PRIORITY 1) TABLE 'some_table')
class ASTKeyValueFunction : public IAST
{
public:
    String name;
    ASTPtr elements;

public:

    String getID(char delim) const override;

    ASTPtr clone() const override;
};

}
