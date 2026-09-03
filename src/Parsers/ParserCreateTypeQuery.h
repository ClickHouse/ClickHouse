#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{

/** Parser for CREATE TYPE query.
  * Example:
  * CREATE TYPE type_name AS base_type
  *     INPUT input_expression
  *     OUTPUT output_expression
  *     DEFAULT default_expression;
  */
class ParserCreateTypeQuery : public IParserBase
{
protected:
    const char * getName() const override { return "CREATE TYPE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
