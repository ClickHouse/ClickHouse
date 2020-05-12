#include <Databases/MySQL/MasterStatusInfo.h>

namespace DB
{
MasterStatusInfo::MasterStatusInfo(
    String binlog_file_, UInt64 binlog_position_, String binlog_do_db_, String binlog_ignore_db_, String executed_gtid_set_)
    : binlog_file(binlog_file_), binlog_position(binlog_position_), binlog_do_db(binlog_do_db_), binlog_ignore_db(binlog_ignore_db_),
    executed_gtid_set(executed_gtid_set_)
{
}

}
