#include "Utils.h"

#if USE_LIBPQXX

#include <IO/Operators.h>

namespace postgres
{

ConnectionInfo formatConnectionString(String dbname, String host, UInt16 port, String user, String password)
{
    DB::WriteBufferFromOwnString out;
    out << "dbname=" << DB::quote << dbname
        << " host=" << DB::quote << host
        << " port=" << port
        << " user=" << DB::quote << user
        << " password=" << DB::quote << password
        << " connect_timeout=10";
    return std::make_pair(out.str(), host + ':' + DB::toString(port));
}

String formatNameForLogs(const String & postgres_database_name, const String & postgres_table_name)
{
    if (postgres_database_name.empty())
        return postgres_table_name;
    if (postgres_table_name.empty())
        return postgres_database_name;
    return fmt::format("{}.{}", postgres_database_name, postgres_table_name);
}

}

#endif
