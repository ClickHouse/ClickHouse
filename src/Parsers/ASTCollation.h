#pragma once

#include <Parsers/IAST.h>

namespace DB
{

class ASTCollation : public IAST
{
public:
    ASTPtr collation = nullptr;

    String getID(char) const override { return "Collation"; }

    ASTPtr clone() const override;

    void formatImpl(const FormattingBuffer & out) const override;
};

}
