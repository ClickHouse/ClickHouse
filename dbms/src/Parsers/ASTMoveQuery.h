#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/ASTQueryWithOnCluster.h>

namespace DB
{


/** MOVE query
  */
class ASTMoveQuery : public ASTQueryWithOutput, public ASTQueryWithOnCluster
{
public:
    struct Table
    {
        String database;
        String table;
    };

    Table table;
    String part_name;
    String destination_disk_name;

    /** Get the text that identifies this element. */
    String getID(char) const override { return "Move"; }

    ASTPtr clone() const override
    {
        auto res = std::make_shared<ASTMoveQuery>(*this);
        cloneOutputOptions(*res);
        return res;
    }

    ASTPtr getRewrittenASTWithoutOnCluster(const std::string & /*new_database*/) const override
    {
        ///@TODO_IGR ASK
        auto query_ptr = clone();
        auto & query = query_ptr->as<ASTMoveQuery &>();

        query.cluster.clear();

        query.table = table;
        query.part_name = part_name;
        query.destination_disk_name = destination_disk_name;

        return query_ptr;
    }

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "TABLE"
                << (settings.hilite ? hilite_keyword : "")
                << (table.database.empty() ? backQuoteIfNeed(table.database) + "." : "") << backQuoteIfNeed(table.table)
                << (settings.hilite ? hilite_keyword : "") << "MOVE PART"
                << (settings.hilite ? hilite_keyword : "") << part_name
                << (settings.hilite ? hilite_keyword : "") << "TO"
                << (settings.hilite ? hilite_none : "") << destination_disk_name;
    }
};

}
