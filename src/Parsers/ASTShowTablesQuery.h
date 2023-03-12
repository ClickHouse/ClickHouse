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
    bool databases{false};
    bool clusters{false};
    bool cluster{false};
    bool dictionaries{false};
    bool m_settings{false};
    bool changed{false};
    bool temporary{false};
    bool caches{false};
    bool full{false};

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
    void formatLike(const FormattingBuffer & out) const;
    void formatLimit(const FormattingBuffer & out) const;
    void formatQueryImpl(const FormattingBuffer & out) const override;
};

}
