#include <Access/ImplicitGrants.h>
#include <Interpreters/DatabaseCatalog.h>
#include <boost/algorithm/string/case_conv.hpp>


namespace DB
{

AccessRights addImplicitGrants(const AccessRights & access)
{
    auto modifier = [&](const AccessFlags & flags, const AccessFlags & min_flags_with_children, const AccessFlags & max_flags_with_children, const std::string_view & database, const std::string_view & table, const std::string_view & column) -> AccessFlags
    {
        size_t level = !database.empty() + !table.empty() + !column.empty();
        AccessFlags res = flags;

        /// CREATE_TABLE => CREATE_VIEW, DROP_TABLE => DROP_VIEW, ALTER_TABLE => ALTER_VIEW
        static const AccessFlags create_table = AccessType::CREATE_TABLE;
        static const AccessFlags create_view = AccessType::CREATE_VIEW;
        static const AccessFlags drop_table = AccessType::DROP_TABLE;
        static const AccessFlags drop_view = AccessType::DROP_VIEW;
        static const AccessFlags alter_table = AccessType::ALTER_TABLE;
        static const AccessFlags alter_view = AccessType::ALTER_VIEW;

        if (res & create_table)
            res |= create_view;

        if (res & drop_table)
            res |= drop_view;

        if (res & alter_table)
            res |= alter_view;

        /// CREATE TABLE (on any database/table) => CREATE_TEMPORARY_TABLE (global)
        static const AccessFlags create_temporary_table = AccessType::CREATE_TEMPORARY_TABLE;
        if ((level == 0) && (max_flags_with_children & create_table))
            res |= create_temporary_table;

        /// ALTER_TTL => ALTER_MATERIALIZE_TTL
        static const AccessFlags alter_ttl = AccessType::ALTER_TTL;
        static const AccessFlags alter_materialize_ttl = AccessType::ALTER_MATERIALIZE_TTL;
        if (res & alter_ttl)
            res |= alter_materialize_ttl;

        /// RELOAD_DICTIONARY (global) => RELOAD_EMBEDDED_DICTIONARIES (global)
        static const AccessFlags reload_dictionary = AccessType::SYSTEM_RELOAD_DICTIONARY;
        static const AccessFlags reload_embedded_dictionaries = AccessType::SYSTEM_RELOAD_EMBEDDED_DICTIONARIES;
        if ((level == 0) && (min_flags_with_children & reload_dictionary))
            res |= reload_embedded_dictionaries;

        /// any column flag => SHOW_COLUMNS => SHOW_TABLES => SHOW_DATABASES
        ///                  any table flag => SHOW_TABLES => SHOW_DATABASES
        ///       any dictionary flag => SHOW_DICTIONARIES => SHOW_DATABASES
        ///                              any database flag => SHOW_DATABASES
        static const AccessFlags show_columns = AccessType::SHOW_COLUMNS;
        static const AccessFlags show_tables = AccessType::SHOW_TABLES;
        static const AccessFlags show_dictionaries = AccessType::SHOW_DICTIONARIES;
        static const AccessFlags show_tables_or_dictionaries = show_tables | show_dictionaries;
        static const AccessFlags show_databases = AccessType::SHOW_DATABASES;

        if (res & AccessFlags::allColumnFlags())
            res |= show_columns;

        if ((res & AccessFlags::allTableFlags())
            || (level <= 2 && (res & show_columns))
            || (level == 2 && (max_flags_with_children & show_columns)))
        {
            res |= show_tables;
        }

        if (res & AccessFlags::allDictionaryFlags())
            res |= show_dictionaries;

        if ((res & AccessFlags::allDatabaseFlags())
            || (level <= 1 && (res & show_tables_or_dictionaries))
            || (level == 1 && (max_flags_with_children & show_tables_or_dictionaries)))
        {
            res |= show_databases;
        }

        return res;
    };

    AccessRights res = access;
    res.modifyFlags(modifier);

    res.grant(getConstImplicitGrants());
    return res;
}

const AccessRightsElements & getConstImplicitGrants()
{
    static const AccessRightsElements elements = []
    {
        AccessRightsElements res;

        /// System tables any user can SELECT from.
        AccessFlags select_flags = AccessType::SELECT;
        constexpr std::array tables_in_system_db = {
            "one",
            "numbers",
            "numbers_mt",
            "zeros",
            "zeros_mt",
            "functions",
            "settings",
            "formats",
            "table_functions",
            "aggregate_function_combinators",
            "data_type_families",
            "collations",
            "table_engines",
            "contributors",
            "privileges",
            "licenses",
            "time_zones",
        };
        for (const auto * table_name : tables_in_system_db)
            res.emplace_back(select_flags, DatabaseCatalog::SYSTEM_DATABASE, table_name);

        /// We implicitly grant access to tables in the `information_schema` database because those tables are implemented
        /// via requests to some tables in the `system` database (system.databases -> information_schema.schemata,
        /// system.tables -> information_schema.tables, system.tables -> information_schema.views,
        /// system.columns -> information_schema.columns).
        /// So for example to perform SELECT from `information_schema.schemata` it's necessary to have an ability to SELECT from
        /// system.databases.
        constexpr std::array tables_in_information_schema_db = {"schemata", "tables", "views", "columns"};
        for (const auto * table_name : tables_in_information_schema_db)
        {
            res.emplace_back(select_flags, DatabaseCatalog::INFORMATION_SCHEMA, table_name);
            res.emplace_back(select_flags, DatabaseCatalog::INFORMATION_SCHEMA_UPPERCASE, boost::to_upper_copy(String{table_name}));
         }

        return res;
    }();

    return elements;
}

}
