#pragma once

#include "IParserBase.h"

namespace DB
{
/// CREATE TYPE type1 AS UInt32
/// Or: CREATE TYPE type1 AS Tuple(Float64, Float64)
class ParserCreateDataTypeQuery : public IParserBase
{
protected:
    const char * getName() const override { return "CREATE TYPE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

class ParserFunctionDefinitionQuery : public IParserBase
{
public:
    explicit ParserFunctionDefinitionQuery(const String & function_type_name_)
        : function_type_name(function_type_name_)
    {}

protected:
    const char * getName() const override { return "function definition query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

private:
    String function_type_name;
};

}
