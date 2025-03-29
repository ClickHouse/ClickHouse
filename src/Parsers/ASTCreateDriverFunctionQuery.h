#pragma once

#include <Parsers/IAST.h>


namespace DB
{

class ASTCreateDriverFunctionQuery : public IAST
{
public:
    ASTPtr function_name;
    ASTPtr function_params;
    ASTPtr function_return_type;
    ASTPtr engine_name;
    ASTPtr function_body;

    bool or_replace = false;
    bool if_not_exists = false;

    String getID(char delim) const override { return "CreateDriverFunctionQuery" + (delim + getFunctionName()); }

    ASTPtr clone() const override;

    String getFunctionName() const;

    String getEngineName() const;

    QueryKind getQueryKind() const override { return QueryKind::Create; }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};

}
