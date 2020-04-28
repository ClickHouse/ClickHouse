#pragma once

#include <set>
#include <mutex>

struct QueryTimestamps
{
    UInt64 read_timestamp;
    UInt64 write_timestamp;
};

// Singleton that manage timestamps gor queries
// It is needed to work with transactions in MergeTree* engines
class QueryTimestampsManager
{
public:
    static QueryTimestamps registerQuery()
    {
        return getSingleton().registerQueryImpl();
    }

    static void endQuery(QueryTimestamps timestamps)
    {
        return getSingleton().endQueryImpl(timestamps);
    }

    static UInt64 getMinReadTimestamp()
    {
        return getSingleton().getMinReadTimestampImpl();
    }

private:
    QueryTimestampsManager() = default;
    ~QueryTimestampsManager() = default;

    static QueryTimestampsManager& getSingleton()
    {
        static QueryTimestampsManager manager;
        return manager;
    }

    QueryTimestamps registerQueryImpl()
    {
        std::unique_lock<std::mutex> guard(mutex);
        QueryTimestamps ts;
        ts.write_timestamp = curr_write_timestamp;
        ++curr_write_timestamp;
        running_write_timestamps.insert(ts.write_timestamp);
        // We can read all data with timestamp strictly lower than any running write timestamp
        ts.read_timestamp = *running_write_timestamps.begin();
        running_read_timestamps.insert(ts.read_timestamp);
        return ts;
    }

    void endQueryImpl(QueryTimestamps timestamps)
    {
        std::unique_lock<std::mutex> guard(mutex);
        running_write_timestamps.erase(timestamps.write_timestamp);
        running_read_timestamps.erase(running_read_timestamps.find(timestamps.read_timestamp));
    }

    // Needed to check whether we can delete MergeTreeDataPart or not
    UInt64 getMinReadTimestampImpl()
    {
        std::unique_lock<std::mutex> guard(mutex);
        if (running_read_timestamps.empty())
        {
            // We have no queries, so return read_timestamp of next query
            return curr_write_timestamp;
        }
        return *running_read_timestamps.begin();
    }


    UInt64 curr_write_timestamp = 0;
    std::set<UInt64> running_write_timestamps;
    std::multiset<UInt64> running_read_timestamps;
    std::mutex mutex;
};
