#include <Common/StringUtils.h>
#include "config_tools.h"


int mainEntryClickHouseKeeper(int argc, char ** argv);

#if ENABLE_CLICKHOUSE_KEEPER_CLIENT
int mainEntryClickHouseKeeperClient(int argc, char ** argv);
#endif

int main(int argc_, char ** argv_)
{
#if ENABLE_CLICKHOUSE_KEEPER_CLIENT

    if (argc_ >= 2)
    {
        /// 'clickhouse-keeper --client ...' and 'clickhouse-keeper client ...' are OK
        if (strcmp(argv_[1], "--client") == 0 || strcmp(argv_[1], "client") == 0)
        {
            argv_[1] = argv_[0];
            return mainEntryClickHouseKeeperClient(--argc_, argv_ + 1);
        }
    }

    if (argc_ > 0 && (strcmp(argv_[0], "clickhouse-keeper-client") == 0 || endsWith(argv_[0], "/clickhouse-keeper-client")))
        return mainEntryClickHouseKeeperClient(argc_, argv_);
#endif

    return mainEntryClickHouseKeeper(argc_, argv_);
}
