#pragma once

#include <Parsers/IAST.h>


namespace DB
{

/// Stub for MySQL compatibility
class ASTTransactionQuery : public IAST
{
public:
    ASTPtr clone() const override
    {
        return std::make_shared<ASTTransactionQuery>(*this);
    }

    String getID(char) const override
    {
        return {};
    }

    void formatImpl(const FormatSettings &, FormatState &, FormatStateStacked) const override
    {
    }
};

}

