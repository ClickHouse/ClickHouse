#pragma once

#include <Parsers/IAST.h>


namespace DB
{

class ASTDropModelQuery : public IAST
{
public:
    ASTPtr model_name;
    bool if_exists = false;

    String getID(char) const override { return "DropModelQuery"; }

    ASTPtr clone() const override;

    QueryKind getQueryKind() const override { return QueryKind::Drop; }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};

}
