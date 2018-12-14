#include <Interpreters/SystemLog.h>
#include <Interpreters/QueryLog.h>
#include <Interpreters/QueryThreadLog.h>
#include <Interpreters/PartLog.h>

#include <Poco/Util/AbstractConfiguration.h>


namespace DB
{

SystemLogs::SystemLogs(Context & global_context, const Poco::Util::AbstractConfiguration & config)
{
    query_log = createDefaultSystemLog<QueryLog>(global_context, "system", "query_log", config, "query_log");
    query_thread_log = createDefaultSystemLog<QueryThreadLog>(global_context, "system", "query_thread_log", config, "query_thread_log");
    part_log = createDefaultSystemLog<PartLog>(global_context, "system", "part_log", config, "part_log");
}


SystemLogs::~SystemLogs() = default;

}
