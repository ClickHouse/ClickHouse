#pragma once

#include <iomanip>
#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/ASTNamedChildrenHelper.h>


namespace DB
{

/** Query SHOW TABLES or SHOW DATABASES
  */
class DECLARE_SELF_AND_CHILDREN(ASTShowTablesQuery, FROM, LIKE, LIMIT_LENGTH)
    : public ASTWithNamedChildren<ASTQueryWithOutput, ASTShowTablesQuery, ASTShowTablesQueryChildren>
{
public:
    bool databases{false};
    bool dictionaries{false};
    bool temporary{false};
    bool not_like{false};

    /** Get the text that identifies this element. */
    String getID(char) const override { return "ShowTables"; }

    ASTPtr clone() const override;

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};

}
