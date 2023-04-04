#pragma once

#include <iomanip>
#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOutput.h>


namespace DB
{


/** Query SHOW TABLES or SHOW DATABASES or SHOW CLUSTERS or SHOW CACHES
  */
class ASTShowTablesQuery : public ASTQueryWithOutput
{
public:
    bool databases = false;
    bool clusters = false;
    bool cluster = false;
    bool dictionaries = false;
    bool m_settings = false;
    bool changed = false;
    bool temporary = false;
    bool caches = false;
    bool full = false;

    String cluster_str;
    String from;
    String like;

    bool not_like = false;
    bool case_insensitive_like = false;

    ASTPtr where_expression;
    ASTPtr limit_length;

    String getID(char) const override { return "ShowTables"; }
    ASTPtr clone() const override;
    QueryKind getQueryKind() const override { return QueryKind::Show; }

protected:
    void formatLike(FormattingBuffer out) const;
    void formatLimit(FormattingBuffer out) const;
    void formatQueryImpl(FormattingBuffer out) const override;
};

}
