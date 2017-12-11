#include <Common/config.h>
#if USE_MYSQL

#include <Dictionaries/Embedded/TechDataHierarchy.h>

#include <common/logger_useful.h>
#include <mysqlxx/PoolWithFailover.h>


static constexpr auto config_key = "mysql_metrica";


void TechDataHierarchy::reload()
{
    Logger * log = &Logger::get("TechDataHierarchy");
    LOG_DEBUG(log, "Loading tech data hierarchy.");

    mysqlxx::PoolWithFailover pool(config_key);
    mysqlxx::Pool::Entry conn = pool.Get();

    {
        mysqlxx::Query q = conn->query("SELECT Id, COALESCE(Parent_Id, 0) FROM OS2");
        LOG_TRACE(log, q.str());
        mysqlxx::UseQueryResult res = q.use();
        while (mysqlxx::Row row = res.fetch())
        {
            UInt64 child = row[0].getUInt();
            UInt64 parent = row[1].getUInt();

            if (child > 255 || parent > 255)
                throw Poco::Exception("Too large OS id (> 255).");

            os_parent[child] = parent;
        }
    }

    {
        mysqlxx::Query q = conn->query("SELECT Id, COALESCE(ParentId, 0) FROM SearchEngines");
        LOG_TRACE(log, q.str());
        mysqlxx::UseQueryResult res = q.use();
        while (mysqlxx::Row row = res.fetch())
        {
            UInt64 child = row[0].getUInt();
            UInt64 parent = row[1].getUInt();

            if (child > 255 || parent > 255)
                throw Poco::Exception("Too large search engine id (> 255).");

            se_parent[child] = parent;
        }
    }
}


bool TechDataHierarchy::isConfigured(const Poco::Util::AbstractConfiguration & config)
{
    return config.has(config_key);
}

#endif
