#pragma once

#include <Parsers/ASTQueryWithOutput.h>


namespace DB
{
/** SHOW QUOTAS
  * SHOW QUOTA USAGE [CURRENT | ALL]
  */
class ASTShowQuotasQuery : public ASTQueryWithOutput
{
public:
    bool usage = false;
    bool current = false;

    String getID(char) const override;
    ASTPtr clone() const override;

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};

}
