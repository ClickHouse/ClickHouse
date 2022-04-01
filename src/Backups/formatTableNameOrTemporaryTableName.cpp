#include <Backups/formatTableNameOrTemporaryTableName.h>
#include <Common/quoteString.h>
#include <Interpreters/DatabaseCatalog.h>


namespace DB
{

String formatTableNameOrTemporaryTableName(const DatabaseAndTableName & table_name)
{
    if (table_name.first == DatabaseCatalog::TEMPORARY_DATABASE)
        return "temporary table " + backQuoteIfNeed(table_name.second);
    else
        return "table " + backQuoteIfNeed(table_name.first) + "." + backQuoteIfNeed(table_name.second);
}

}
