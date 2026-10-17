#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOutput.h>

namespace DB
{

/** SHOW TYPE type_name query
  */
class ASTShowTypeQuery : public ASTQueryWithOutput
{
public:
    String type_name;

    String getID(char) const override { return "ShowTypeQuery"; }

    ASTPtr clone() const override;

protected:
    void formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};

}
