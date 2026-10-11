#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOutput.h>

namespace DB
{

/** SHOW TYPES query
  */
class ASTShowTypesQuery : public ASTQueryWithOutput
{
public:
    String getID(char) const override { return "ShowTypesQuery"; }

    ASTPtr clone() const override;

protected:
    void formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};

}
