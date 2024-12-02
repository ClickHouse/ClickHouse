#pragma once

#include <iomanip>
#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOutput.h>


namespace DB
{


/** Query SHOW TABLES or SHOW DATABASES or SHOW CLUSTERS or SHOW CACHES or SHOW MERGES
  */
class ASTShowTablesQuery : public ASTQueryWithOutput
{
public:
    bool databases = false;
    bool clusters = false;
    bool cluster = false;
    bool dictionaries = false;
    bool m_settings = false;
    bool merges = false;
    bool changed = false;
    bool temporary = false;
    bool caches = false;
    bool full = false;

    IAST * from;

    String cluster_str;
    String like;

    bool not_like = false;
    bool case_insensitive_like = false;

    ASTPtr where_expression;
    ASTPtr limit_length;

    String getID(char) const override { return "ShowTables"; }
    ASTPtr clone() const override;
    QueryKind getQueryKind() const override { return QueryKind::Show; }

    String getFrom() const;

protected:
    void formatLike(WriteBuffer & ostr, const FormatSettings & settings) const;
    void formatLimit(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const;
    void formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};

}
