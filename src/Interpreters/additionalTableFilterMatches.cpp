#include <Interpreters/additionalTableFilterMatches.h>

#include <Interpreters/Context.h>

namespace DB
{

bool additionalTableFilterMatches(
    const String & filter_key, const String & table_expression_alias, const StorageID & storage_id, const Context & context)
{
    if (!table_expression_alias.empty() && filter_key == table_expression_alias)
        return true;
    if (filter_key == storage_id.getFullNameNotQuoted())
        return true;

    const auto database_info = context.getCurrentDatabaseInfo();
    if (database_info.database != storage_id.database_name)
        return false;

    const auto & table_name = storage_id.table_name;
    if (filter_key == table_name)
        return true;

    /// under USE db.ns the name relative to the namespace addresses the same table
    const auto & prefix = database_info.table_prefix;
    return !prefix.empty()
        && table_name.size() > prefix.size() + 1
        && table_name.compare(0, prefix.size(), prefix) == 0
        && table_name[prefix.size()] == '.'
        && filter_key == table_name.substr(prefix.size() + 1);
}

}
