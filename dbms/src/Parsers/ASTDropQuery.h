#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/ASTQueryWithOnCluster.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}

/** DROP query
  */
class ASTDropQuery : public ASTQueryWithOutput, public ASTQueryWithOnCluster
{
public:
    enum Kind
    {
        Drop,
        Detach,
        Truncate,
    };

    Kind kind;
    bool if_exists{false};
    bool temporary{false};
    String database;
    String table;

    /** Get the text that identifies this element. */
    String getID() const override
    {
        if (kind == ASTDropQuery::Kind::Drop)
            return "DropQuery_" + database + "_" + table;
        else if (kind == ASTDropQuery::Kind::Detach)
            return "DetachQuery_" + database + "_" + table;
        else if (kind == ASTDropQuery::Kind::Truncate)
            return "TruncateQuery_" + database + "_" + table;
        else
            throw Exception("Not supported kind of drop query.", ErrorCodes::SYNTAX_ERROR);
    }

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
            settings.ostr << (settings.hilite ? hilite_keyword : "");
            if (kind == ASTDropQuery::Kind::Drop)
                settings.ostr << "DROP DATABASE ";
            else if (kind == ASTDropQuery::Kind::Detach)
                settings.ostr << "DETACH DATABASE ";
            else if (kind == ASTDropQuery::Kind::Truncate)
                settings.ostr << "TRUNCATE DATABASE ";
            else
                throw Exception("Not supported kind of drop query.", ErrorCodes::SYNTAX_ERROR);

            settings.ostr << (if_exists ? "IF EXISTS " : "") << (settings.hilite ? hilite_none : "") << backQuoteIfNeed(database);
            formatOnCluster(settings);
        }
        else
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "");
            if (kind == ASTDropQuery::Kind::Drop)
                settings.ostr << "DROP TABLE ";
            else if (kind == ASTDropQuery::Kind::Detach)
                settings.ostr << "DETACH TABLE ";
            else if (kind == ASTDropQuery::Kind::Truncate)
                settings.ostr << "TRUNCATE TABLE ";
            else
                throw Exception("Not supported kind of drop query.", ErrorCodes::SYNTAX_ERROR);

            settings.ostr << (if_exists ? "IF EXISTS " : "") << (settings.hilite ? hilite_none : "")
                          << (!database.empty() ? backQuoteIfNeed(database) + "." : "") << backQuoteIfNeed(table);
            formatOnCluster(settings);
        }
    }
};

}
