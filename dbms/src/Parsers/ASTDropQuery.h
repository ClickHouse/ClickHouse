#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/ASTQueryWithOnCluster.h>

namespace DB
{


/** DROP query
  */
class ASTDropQuery : public ASTQueryWithOutput, public ASTQueryWithOnCluster
{
public:
    bool detach{false};    /// DETACH query, not DROP.
    bool if_exists{false};
    bool temporary{false};
    String database;
    String table;

    /** Get the text that identifies this element. */
    String getID() const override { return (detach ? "DetachQuery_" : "DropQuery_") + database + "_" + table; };

    ASTPtr clone() const override
    {
        auto res = std::make_shared<ASTDropQuery>(*this);
        cloneOutputOptions(*res);
        return res;
    }

    ASTPtr getRewrittenASTWithoutOnCluster(const std::string & new_database) const override
    {
        auto query_ptr = clone();
        auto & query = static_cast<ASTDropQuery &>(*query_ptr);

        query.cluster.clear();
        if (query.database.empty())
            query.database = new_database;

        return query_ptr;
    }

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override
    {
        if (table.empty() && !database.empty())
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "")
                << (detach ? "DETACH DATABASE " : "DROP DATABASE ")
                << (if_exists ? "IF EXISTS " : "")
                << (settings.hilite ? hilite_none : "")
                << backQuoteIfNeed(database);
            formatOnCluster(settings);
        }
        else
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "")
                << (detach ? "DETACH TABLE " : "DROP TABLE ")
                << (if_exists ? "IF EXISTS " : "") << (settings.hilite ? hilite_none : "")
                << (!database.empty() ? backQuoteIfNeed(database) + "." : "") << backQuoteIfNeed(table);
            formatOnCluster(settings);
        }
    }
};

}
