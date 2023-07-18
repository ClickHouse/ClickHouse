#include <Parsers/ASTDropQuery.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}


String ASTDropQuery::getID(char delim) const
{
    if (kind == ASTDropQuery::Kind::Drop)
        return "DropQuery" + (delim + getDatabase()) + delim + getTable();
    else if (kind == ASTDropQuery::Kind::Detach)
        return "DetachQuery" + (delim + getDatabase()) + delim + getTable();
    else if (kind == ASTDropQuery::Kind::Truncate)
        return "TruncateQuery" + (delim + getDatabase()) + delim + getTable();
    else
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Not supported kind of drop query.");
}

ASTPtr ASTDropQuery::clone() const
{
    auto res = std::make_shared<ASTDropQuery>(*this);
    cloneOutputOptions(*res);
    cloneTableOptions(*res);
    return res;
}

void ASTDropQuery::formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "");
    if (kind == ASTDropQuery::Kind::Drop)
        settings.ostr << "DROP ";
    else if (kind == ASTDropQuery::Kind::Detach)
        settings.ostr << "DETACH ";
    else if (kind == ASTDropQuery::Kind::Truncate)
        settings.ostr << "TRUNCATE ";
    else
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Not supported kind of drop query.");

    if (temporary)
        settings.ostr << "TEMPORARY ";


    if (!table && database)
        settings.ostr << "DATABASE ";
    else if (is_dictionary)
        settings.ostr << "DICTIONARY ";
    else if (is_view)
        settings.ostr << "VIEW ";
    else
        settings.ostr << "TABLE ";

    if (if_exists)
        settings.ostr << "IF EXISTS ";

    settings.ostr << (settings.hilite ? hilite_none : "");

    for (auto it=databases_tables.begin();it !=databases_tables.end();++it)
    {
        ASTPtr elem_database = std::get<0>(*it);
        ASTPtr elem_table = std::get<1>(*it);
      if (it!=databases_tables.begin())
          settings.ostr << ",";
      if (!elem_table && elem_database)
      {
        String database_name;
        tryGetIdentifierNameInfo(elem_database,database_name);
        settings.ostr << backQuoteIfNeed(database_name);
      }
      else
      {
        String database_name;
        tryGetIdentifierNameInfo(elem_database,database_name);
        database_name = (elem_database ? backQuoteIfNeed(database_name)+".":"");
        String table_name;
        tryGetIdentifierNameInfo(elem_table,table_name);
        settings.ostr << database_name << backQuoteIfNeed(table_name);
      }
    }

    formatOnCluster(settings);

    if (permanently)
        settings.ostr << " PERMANENTLY";

    if (sync)
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " SYNC" << (settings.hilite ? hilite_none : "");
}

}
