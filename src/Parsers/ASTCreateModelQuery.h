#pragma once

#include <Parsers/IAST.h>

namespace DB
{

class ASTCreateModelQuery : public IAST
{
public:
    ASTPtr model_name;
    ASTPtr algorithm;
    ASTPtr options;
    ASTPtr target;
    ASTPtr table_name;

    String getID(char) const override;

    ASTPtr clone() const override;

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & format_settings, FormatState & state, FormatStateStacked frame) const override;
};

}
