#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTIdentifier.h> // Для имени типа

namespace DB
{

class ASTDropTypeQuery : public IAST
{
public:
    String type_name;
    bool if_exists{false};

    String getID(char delim) const override;
    ASTPtr clone() const override;
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
