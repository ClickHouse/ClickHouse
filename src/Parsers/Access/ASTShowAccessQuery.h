#pragma once

#include <Parsers/ASTQueryWithOutput.h>


namespace DB
{

/** SHOW ACCESS [WITH VERSION]
  */
class ASTShowAccessQuery : public ASTQueryWithOutput
{
public:
    bool show_rbac_version = false;

    String getID(char) const override;
    ASTPtr clone() const override;

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};

}
