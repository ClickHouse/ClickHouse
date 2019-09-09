#pragma once

#include <iomanip>
#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOutput.h>


namespace DB
{


/** Query SHOW TABLES or SHOW DATABASES
  */
class ASTShowTablesQuery : public ASTQueryWithOutput
{
public:
    bool databases{false};
    bool temporary{false};
    String from;
    String like;
    bool not_like{false};

    /** Get the text that identifies this element. */
    String getID(char) const override { return "ShowTables"; }

    ASTPtr clone() const override;

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};

}
