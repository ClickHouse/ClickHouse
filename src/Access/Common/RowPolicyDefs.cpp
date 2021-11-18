#include <Access/Common/RowPolicyDefs.h>
#include <Common/quoteString.h>


namespace DB
{

String RowPolicyName::toString() const
{
    String name;
    name.reserve(database.length() + table_name.length() + short_name.length() + 6);
    name += backQuoteIfNeed(short_name);
    name += " ON ";
    if (!database.empty())
    {
        name += backQuoteIfNeed(database);
        name += '.';
    }
    name += backQuoteIfNeed(table_name);
    return name;
}

}
