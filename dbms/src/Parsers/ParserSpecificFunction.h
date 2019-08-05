#pragma once

#include <functional>
#include <unordered_map>
#include <ext/singleton.h>
#include <Parsers/IParserBase.h>

namespace DB
{

/** A function, for example, f(x, y + 1, g(z)).
  * Or an aggregate function: sum(x + f(y)), corr(x, y). The syntax is the same as the usual function.
  * Or a parametric aggregate function: quantile(0.9)(x + y).
  *  Syntax - two pairs of parentheses instead of one. The first is for parameters, the second for arguments.
  * For functions, the DISTINCT modifier can be specified, for example, count(DISTINCT x, y).
  */
class ParserFunction : public IParserBase
{
protected:
    const char * getName() const { return "function"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected);
};

/** Use it to define specific functions that are different from ASTFunction(ASTExpressionList(ASTLiteral, ASTIdentifier))
 *  for example, MySQL('host:port', 'database_name', 'table_name', 'user_name', 'password'), password is masked when parsing.
 */
class ParserSpecificFunction : public IParserBase
{
protected:
    const char * getName() const override { return "Specific function"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

    struct SpecificParserFunctionFactory : public ext::singleton<SpecificParserFunctionFactory>
    {
    public:
        using ParseFunction = std::function<bool(Pos &, ASTPtr &, Expected &)>;

        const ParseFunction * tryGet(const String & function_name);

    private:
        std::unordered_map<String, ParseFunction> parser_function_dictionary;

        SpecificParserFunctionFactory();

        friend class ext::singleton<SpecificParserFunctionFactory>;
    };
};

}
