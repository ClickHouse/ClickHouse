#include "PostgreSQLReplicaConsumer.h"

#include <Formats/FormatFactory.h>
#include <Formats/FormatSettings.h>

namespace DB
{

PostgreSQLReplicaConsumer::PostgreSQLReplicaConsumer(
    const std::string & table_name_,
    const std::string & conn_str,
    const std::string & replication_slot_name_,
    const std::string & publication_name_,
    const LSNPosition & start_lsn)
    : log(&Poco::Logger::get("PostgreSQLReaplicaConsumer"))
    , replication_slot_name(replication_slot_name_)
    , publication_name(publication_name_)
    , table_name(table_name_)
    , connection(std::make_shared<PostgreSQLConnection>(conn_str))
    , current_lsn(start_lsn)
{
    replication_connection = std::make_shared<PostgreSQLConnection>(fmt::format("{} replication=database", conn_str));
}


void PostgreSQLReplicaConsumer::run()
{
    auto options = fmt::format(" (\"proto_version\" '1', \"publication_names\" '{}')", publication_name);
    startReplication(replication_slot_name, current_lsn.lsn, -1, options);
}


void PostgreSQLReplicaConsumer::startReplication(
        const std::string & slot_name, const std::string start_lsn, const int64_t /* timeline */, const std::string & plugin_args)
{
    std::string query_str = fmt::format("START_REPLICATION SLOT {} LOGICAL {}", slot_name, start_lsn);

    if (!plugin_args.empty())
        query_str += plugin_args;

    auto tx = std::make_unique<pqxx::nontransaction>(*replication_connection->conn());
    tx->exec(query_str);

    //pqxx::stream_from stream(*tx, pqxx::from_query, std::string_view(query_str));
    //pqxx::result result{tx->exec(query_str)};
    //pqxx::row row{result[0]};
    //for (auto res : row)
    //{
    //    if (std::size(res))
    //        LOG_TRACE(log, "GOT {}", res.as<std::string>());
    //    else
    //        LOG_TRACE(log, "GOT NULL");
    //}

   // while (true)
   // {
   //     const std::vector<pqxx::zview> * row{stream.read_row()};

   //     if (!row)
   //     {
   //         LOG_TRACE(log, "STREAM REPLICATION END");
   //         stream.complete();
   //         tx->commit();
   //         break;
   //     }
   //     LOG_TRACE(log, "STARTED REPLICATION. GOT ROW SIZE", row->size());

   //     for (const auto idx : ext::range(0, row->size()))
   //     {
   //         auto current = (*row)[idx];
   //         LOG_TRACE(log, "Started replication. GOT: {}", current);
   //     }

    //}
}

}
