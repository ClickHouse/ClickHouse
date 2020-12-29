#pragma once

#include <iomanip>
#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOutput.h>


namespace DB
{


/** Query SHOW TABLES or SHOW DATABASES or SHOW CLUSTERS
  */
class ASTShowTablesQuery : public ASTQueryWithOutput
{
public:
    bool databases{false};
    bool clusters{false};
    bool cluster{false};
    bool dictionaries{false};
    bool temporary{false};

    String cluster_str;
    String from;
    String like;

    bool not_like{false};
    bool case_insensitive_like{false};

    ASTPtr where_expression;
    ASTPtr limit_length;

    /** Get the text that identifies this element. */
    String getID(char) const override { return "ShowTables"; }

    ASTPtr clone() const override;

protected:
    void formatLike(const FormatSettings & settings) const;
    void formatLimit(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const;
    void formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};

}
