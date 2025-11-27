#include <Interpreters/Context.h>
#include <Interpreters/ZooKeeperConnectionLog.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeNullable.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/KeeperFeatureFlags.h>
#include <Storages/System/StorageSystemZooKeeperInfo.h>
#include <Coordination/FourLetterCommand.h>
#include <Coordination/CoordinationSettings.h>
#include <IO/ReadBuffer.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ZooKeeper/Common.h>
#include <Common/ZooKeeper/ZooKeeperIO.h>
#include <Poco/NumberParser.h>
#include <Common/logger_useful.h>
#include <Poco/Logger.h>
#include <boost/algorithm/string/split.hpp>
#include <IO/S3/Credentials.h>
#include <Server/CloudPlacementInfo.h>
#include <base/find_symbols.h>
#include <base/getFQDNOrHostName.h>
#include <Poco/String.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/StringUtils.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/isLocalAddress.h>
#include <Common/thread_local_rng.h>
#include <Common/Config/ConfigProcessor.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <IO/ReadBufferFromPocoSocket.h>

namespace DB
{

ColumnsDescription StorageSystemZooKeeperInfo::getColumnsDescription()
{
    DataTypeEnum16::Values feature_flags_enum_values;
    feature_flags_enum_values.reserve(magic_enum::enum_count<KeeperFeatureFlag>());
    for (const auto & [feature_flag, feature_flag_string] : magic_enum::enum_entries<KeeperFeatureFlag>())
        feature_flags_enum_values.push_back(std::pair{std::string{feature_flag_string}, static_cast<Int16>(feature_flag)});

    auto feature_flags_enum = std::make_shared<DataTypeEnum16>(std::move(feature_flags_enum_values));

    return ColumnsDescription
    {
        /* 0 */ {"zookeeper_cluster_name", std::make_shared<DataTypeString>(), "ZooKeeper cluster's name."},
        /* 1 */ {"host", std::make_shared<DataTypeString>(), "The hostname/IP of the ZooKeeper node that ClickHouse connected to."},
        /* 2 */ {"port", std::make_shared<DataTypeUInt16>(), "The port of the ZooKeeper node that ClickHouse connected to."},
        /* 3 */ {"index", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>()), "The index of the ZooKeeper node that ClickHouse connected to. The index is from ZooKeeper config. If not connected, this column is NULL."},
        /* 4 */ {"is_connected", std::make_shared<DataTypeUInt8>(), "Is the zookeeper connected."},

        /// isro command
        /* 5 */ {"is_readonly", std::make_shared<DataTypeUInt8>(), "Is readonly."},

        /// mntr command
        /* 6 */ {"version", std::make_shared<DataTypeString>(), "The ZooKeeper version."},
        /* 7 */ {"avg_latency", std::make_shared<DataTypeUInt64>(), "The average latency."},
        /* 8 */ {"max_latency", std::make_shared<DataTypeUInt64>(), "The max latency."},
        /* 9 */ {"min_latency", std::make_shared<DataTypeUInt64>(), "The min latency."},
        /* 10 */ {"packets_received", std::make_shared<DataTypeUInt64>(), "The number of packets received."},
        /* 11 */ {"packets_sent", std::make_shared<DataTypeUInt64>(), "The number of packets sent."},
        /* 12 */ {"outstanding_requests", std::make_shared<DataTypeUInt64>(), "The number of outstanding requests."},
        /* 13 */ {"server_state", std::make_shared<DataTypeString>(), "Server state."},
        /* 14 */ {"is_leader", std::make_shared<DataTypeUInt8>(), "Is this zookeeper leader."},
        /* 15 */ {"znode_count", std::make_shared<DataTypeUInt64>(), "The znode count."},
        /* 16 */ {"watch_count", std::make_shared<DataTypeUInt64>(), "The watch count."},
        /* 17 */ {"ephemerals_count", std::make_shared<DataTypeUInt64>(), "The ephemerals count."},
        /* 18 */ {"approximate_data_size", std::make_shared<DataTypeUInt64>(), "The approximate data size."},
        /* 19 */ {"followers", std::make_shared<DataTypeUInt64>(), "The followers of the leader. This field is only exposed by the leader."},
        /* 20 */ {"synced_followers", std::make_shared<DataTypeUInt64>(), "The synced followers of the leader. This field is only exposed by the leader."},
        /* 21 */ {"pending_syncs", std::make_shared<DataTypeUInt64>(), "The pending syncs of the leader. This field is only exposed by the leader."},
        /* 22 */ {"open_file_descriptor_count", std::make_shared<DataTypeUInt64>(), "The open file descriptor count. Only available on Unix platforms."},
        /* 23 */ {"max_file_descriptor_count", std::make_shared<DataTypeUInt64>(), "The max file descriptor count. Only available on Unix platforms."},


        ///srvr command
        /* 24 */ {"connections", std::make_shared<DataTypeUInt64>(), "The ZooKeeper connections."},
        /* 25 */ {"outstanding", std::make_shared<DataTypeUInt64>(), "The ZooKeeper outstanding."},
        /* 26 */ {"zxid", std::make_shared<DataTypeInt64>(), "The ZooKeeper zxid."},
        /* 27 */ {"node_count", std::make_shared<DataTypeUInt64>(), "The ZooKeeper node count."},


        /// dirs command
        /* 28 */ {"snapshot_dir_size", std::make_shared<DataTypeUInt64>(), "The ZooKeeper snapshot directory size."},
        /* 29 */ {"log_dir_size", std::make_shared<DataTypeUInt64>(), "The ZooKeeper log directory size."},


        /// lgif command
        /* 30 */ {"first_log_idx", std::make_shared<DataTypeUInt64>(), "The ZooKeeper first log index."},
        /* 31 */ {"first_log_term", std::make_shared<DataTypeUInt64>(), "The ZooKeeper first log term."},
        /* 32 */ {"last_log_idx", std::make_shared<DataTypeUInt64>(), "The ZooKeeper last log index."},
        /* 33 */ {"last_log_term", std::make_shared<DataTypeUInt64>(), "The ZooKeeper last log term."},
        /* 34 */ {"last_committed_idx", std::make_shared<DataTypeUInt64>(), "The ZooKeeper last committed index."},
        /* 35 */ {"leader_committed_log_idx", std::make_shared<DataTypeUInt64>(), "The ZooKeeper leader committed log index."},
        /* 36 */ {"target_committed_log_idx", std::make_shared<DataTypeUInt64>(), "The ZooKeeper target committed log index."},
        /* 37 */ {"last_snapshot_idx", std::make_shared<DataTypeUInt64>(), "The ZooKeeper last snapshot index."},
    };
}

void StorageSystemZooKeeperInfo::fillData(MutableColumns & res_columns, ContextPtr context,
                                                    const ActionsDAG::Node *, std::vector<UInt8>) const
{
    LOG_DEBUG(getLogger("StorageSystemZooKeeperInfo"), "StorageSystemZooKeeperInfo enter");

    /// Read all Keeper from config
    ConfigProcessor config_processor(context->getConfigRef().getString("config-file", "config.xml"));

    /// This will handle a situation when clickhouse is running on the embedded config, but config.d folder is also present.
    ConfigProcessor::registerEmbeddedConfig("config.xml", "<clickhouse/>");
    auto clickhouse_config = config_processor.loadConfig();

    Poco::Util::AbstractConfiguration::Keys keys;
    clickhouse_config.configuration->keys("zookeeper", keys);

    if (!context->getConfigRef().has("host") && !context->getConfigRef().has("port") && !keys.empty())
    {
        LOG_INFO(getLogger("KeeperClient"), "Found keeper node in the config.xml, will use it for connection");

        for (const auto & key : keys)
        {
            if (key != "node")
                continue;

            String prefix = "zookeeper." + key;
            String host = clickhouse_config.configuration->getString(prefix + ".host");
            String port = clickhouse_config.configuration->getString(prefix + ".port");

            if (clickhouse_config.configuration->has(prefix + ".secure"))
                host = "secure://" + host;

            LOG_DEBUG(getLogger("StorageSystemZooKeeperInfo"), "StorageSystemZooKeeperInfo host = {} port = {}", host, port);

            res_columns[0]->insert("default");
            res_columns[1]->insert(host);
            res_columns[2]->insert(stoi(port));

            /// to-do set index
            res_columns[3]->insert(0);
            res_columns[4]->insert(false);


            /// isro command
            /// The server will respond with "ro" if in read-only mode or "rw" if not in read-only mode.
            String isro_output = sendFourLetterCommand(host, port, "isro");
            if (isro_output == "ro")
                res_columns[5]->insert(true);
            else
                res_columns[5]->insert(false);

            /// mntr command
            String mntr_output = sendFourLetterCommand(host, port, "mntr");
            std::vector<String> mntr_result_split;
            boost::split(mntr_result_split, mntr_output, [](char c) { return c == '\n'; });

            std::map<String,String> mntr_responses_map;
            for (auto & line : mntr_result_split)
            {
                if (line.empty())
                    break;
                LOG_DEBUG(getLogger("StorageSystemZooKeeperInfo"), "Result of line =  {}", line);

                std::vector <String> line_result;

                boost::split(line_result, line, [](char c) { return c == '\t'; });

                assert(line_result.size() == 2);
                LOG_DEBUG(getLogger("StorageSystemZooKeeperInfo"), "Result of line split  name = {} value={}. ", line_result[0], line_result[1]);
                mntr_responses_map[line_result[0]] = line_result[1];
            }

            /// /* 6 */{"version", std::make_shared<DataTypeString>(), "The ZooKeeper version."},
            res_columns[6]->insert(mntr_responses_map["zk_version"]);

            // /* 7 */ {"avg_latency", std::make_shared<DataTypeUInt64>(), "The average latency."},
            res_columns[7]->insert(stoi(mntr_responses_map["zk_avg_latency"]));

            // /* 8 */ {"max_latency", std::make_shared<DataTypeUInt64>(), "The max latency."},
            res_columns[8]->insert(stoi(mntr_responses_map["zk_max_latency"]));

            //  /* 9 */ {"min_latency", std::make_shared<DataTypeUInt64>(), "The min latency."},
            res_columns[9]->insert(stoi(mntr_responses_map["zk_min_latency"]));

            // /* 10 */ {"packets_received", std::make_shared<DataTypeUInt64>(), "The number of packets received."},
            res_columns[10]->insert(stoi(mntr_responses_map["zk_packets_received"]));

            // /* 11 */ {"packets_sent", std::make_shared<DataTypeUInt64>(), "The number of packets sent."},
            res_columns[11]->insert(stoi(mntr_responses_map["zk_packets_sent"]));

            // /* 12 */ {"outstanding_requests", std::make_shared<DataTypeUInt64>(), "The number of outstanding requests."},
            res_columns[12]->insert(stoi(mntr_responses_map["zk_outstanding_requests"]));

            // /* 13 */ {"server_state", std::make_shared<DataTypeString>(), "Server state."},
            res_columns[13]->insert(mntr_responses_map["zk_server_state"]);

            int followers = stoi(mntr_responses_map["zk_followers"]);
            ///* 14 */ {"is_leader", std::make_shared<DataTypeUInt8>(), "Is this zookeeper leader."},
            if (followers)
                res_columns[14]->insert(true);
            else
                res_columns[14]->insert(false);

            ///* 15 */ {"znode_count", std::make_shared<DataTypeUInt64>(), "The znode count."},
            res_columns[15]->insert(stoi(mntr_responses_map["zk_znode_count"]));

            // /* 16 */ {"watch_count", std::make_shared<DataTypeUInt64>(), "The watch count."},
            res_columns[16]->insert(stoi(mntr_responses_map["zk_watch_count"]));

            // /* 17 */ {"ephemerals_count", std::make_shared<DataTypeUInt64>(), "The ephemerals count."},
            res_columns[17]->insert(stoi(mntr_responses_map["zk_ephemerals_count"]));

            // /* 18 */ {"approximate_data_size", std::make_shared<DataTypeUInt64>(), "The approximate data size."},
            res_columns[18]->insert(stoi(mntr_responses_map["zk_approximate_data_size"]));

            // /* 19 */ {"followers", std::make_shared<DataTypeUInt64>(), "The followers of the leader. This field is only exposed by the leader."},
            res_columns[19]->insert(stoi(mntr_responses_map["zk_followers"]));

            // /* 20 */ {"synced_followers", std::make_shared<DataTypeUInt64>(), "The synced followers of the leader. This field is only exposed by the leader."},
            int synced_followers = stoi(mntr_responses_map["zk_synced_followers"]);
            res_columns[20]->insert(synced_followers);

            // /* 21 */ {"pending_syncs", std::make_shared<DataTypeUInt64>(), "The pending syncs of the leader. This field is only exposed by the leader."},
            res_columns[21]->insert(followers - synced_followers);

            // /* 22 */ {"open_file_descriptor_count", std::make_shared<DataTypeUInt64>(), "The open file descriptor count. Only available on Unix platforms."},
            res_columns[22]->insert(stoi(mntr_responses_map["zk_open_file_descriptor_count"]));

            // /* 23 */ {"max_file_descriptor_count", std::make_shared<DataTypeUInt64>(), "The max file descriptor count. Only available on Unix platforms."},
            res_columns[23]->insert(stoi(mntr_responses_map["zk_max_file_descriptor_count"]));


            /// srvr command
            String srvr_output = sendFourLetterCommand(host, port, "srvr");
            std::vector<String> srvr_result_split;
            boost::split(srvr_result_split, srvr_output, [](char c) { return c == '\n'; });

            std::map<String,String> srvr_responses_map;
            for (auto & line : srvr_result_split)
            {
                if (line.empty())
                    break;
                LOG_DEBUG(getLogger("StorageSystemZooKeeperInfo"), "Result of line =  {}", line);

                std::vector <String> line_result;

                boost::split(line_result, line, [](char c) { return c == ':'; });

                assert(line_result.size() == 2);
                LOG_DEBUG(getLogger("StorageSystemZooKeeperInfo"), "Result of srvr line split  name = {} value={}. ", line_result[0], line_result[1]);
                srvr_responses_map[line_result[0]] = line_result[1];
            }

            ///* 24 */ {"connections", std::make_shared<DataTypeUInt64>(), "The ZooKeeper connections."},
            res_columns[24]->insert(stoi(srvr_responses_map["Connections"]));

            ///* 25 */ {"outstanding", std::make_shared<DataTypeUInt64>(), "The ZooKeeper outstanding."},
            res_columns[25]->insert(stoi(srvr_responses_map["Outstanding"]));

            //* 26 */ {"zxid", std::make_shared<DataTypeInt64>(), "The ZooKeeper zxid."},
            res_columns[26]->insert(stoi(srvr_responses_map["Zxid"]));

            //* 27 */ {"node_count", std::make_shared<DataTypeUInt64>(), "The ZooKeeper node count."},
            res_columns[27]->insert(stoi(srvr_responses_map["Node count"]));


            /// dirs command
            String dirs_output = sendFourLetterCommand(host, port, "dirs");
            std::vector<String> dirs_result_split;
            boost::split(dirs_result_split, dirs_output, [](char c) { return c == '\n'; });

            std::map<String,String> dirs_responses_map;
            for (auto & line : dirs_result_split)
            {
                if (line.empty())
                    break;
                LOG_DEBUG(getLogger("StorageSystemZooKeeperInfo"), "Result of line =  {}", line);

                std::vector <String> line_result;

                boost::split(line_result, line, [](char c) { return c == ':'; });

                assert(line_result.size() == 2);
                LOG_DEBUG(getLogger("StorageSystemZooKeeperInfo"), "Result of dirs line split  name = {} value={}. ", line_result[0], line_result[1]);
                dirs_responses_map[line_result[0]] = line_result[1];
            }


            //* 28 */ {"snapshot_dir_size", std::make_shared<DataTypeUInt64>(), "The ZooKeeper snapshot directory size."},
            res_columns[28]->insert(stoi(dirs_responses_map["snapshot_dir_size"]));

            //* 29 */ {"log_dir_size", std::make_shared<DataTypeUInt64>(), "The ZooKeeper log directory size."},
            res_columns[29]->insert(stoi(dirs_responses_map["log_dir_size"]));


            //        /// lgif command
            String lgif_output = sendFourLetterCommand(host, port, "lgif");
            std::vector<String> lgif_result_split;
            boost::split(lgif_result_split, lgif_output, [](char c) { return c == '\n'; });

            std::map<String,String> lgif_responses_map;
            for (auto & line :lgif_result_split)
            {
                if (line.empty())
                    break;
                LOG_DEBUG(getLogger("StorageSystemZooKeeperInfo"), "Result of line lgif =  {}", line);

                std::vector <String> line_result;

                boost::split(line_result, line, [](char c) { return c == '\t'; });

                assert(line_result.size() == 2);
                LOG_DEBUG(getLogger("StorageSystemZooKeeperInfo"), "Result of lgif line split  name = {} value={}. ", line_result[0], line_result[1]);
                lgif_responses_map[line_result[0]] = line_result[1];
            }


            // /* 30 */ {"first_log_idx", std::make_shared<DataTypeUInt64>(), "The ZooKeeper first log index."},
            res_columns[30]->insert(stoi(lgif_responses_map["first_log_idx"]));

            // /* 31 */ {"first_log_term", std::make_shared<DataTypeUInt64>(), "The ZooKeeper first log term."},
            res_columns[31]->insert(stoi(lgif_responses_map["first_log_term"]));

            // /* 32 */ {"last_log_idx", std::make_shared<DataTypeUInt64>(), "The ZooKeeper last log index."},
            res_columns[32]->insert(stoi(lgif_responses_map["last_log_idx"]));

            // /* 33 */ {"last_log_term", std::make_shared<DataTypeUInt64>(), "The ZooKeeper last log term."},
            res_columns[33]->insert(stoi(lgif_responses_map["last_log_term"]));

            // /* 34 */ {"last_committed_idx", std::make_shared<DataTypeUInt64>(), "The ZooKeeper last committed index."},
            res_columns[34]->insert(stoi(lgif_responses_map["last_committed_log_idx"]));

            // /* 35 */ {"leader_committed_log_idx", std::make_shared<DataTypeUInt64>(), "The ZooKeeper leader committed log index."},
            res_columns[35]->insert(stoi(lgif_responses_map["leader_committed_log_idx"]));

            // /* 36 */ {"target_committed_log_idx", std::make_shared<DataTypeUInt64>(), "The ZooKeeper target committed log index."},
            res_columns[36]->insert(stoi(lgif_responses_map["target_committed_log_idx"]));

            // /* 37 */ {"last_snapshot_idx", std::make_shared<DataTypeUInt64>(), "The ZooKeeper last snapshot index."},
            res_columns[37]->insert(stoi(lgif_responses_map["last_snapshot_idx"]));

        }
    }
}


String StorageSystemZooKeeperInfo::sendFourLetterCommand(String host, String port, String command) const
{
    Poco::Net::SocketAddress address(host, port);
    LOG_INFO(getLogger("StorageSystemZooKeeperInfo"), "sendFourLetterCommand socket_address : {}", address.toString());

    Poco::Net::StreamSocket socket;

    String response;
        try
        {
            socket = Poco::Net::StreamSocket();
            socket.connect(address);

            socket.setReceiveTimeout(10000 * 3000);
            socket.setSendTimeout(10000 * 1000);
            socket.setNoDelay(true);

            auto in = std::make_shared<ReadBufferFromPocoSocket>(socket);
            auto out = std::make_shared<AutoCanceledWriteBuffer<WriteBufferFromPocoSocket>>(socket);

            int32_t res = *reinterpret_cast<const int32_t *>(command.data());
            /// keep consistent with Coordination::read method by changing big endian to little endian.
            int32_t cmd_int = std::byteswap(res);

            Coordination::write(cmd_int,*out);
            out->next();
            Coordination::read(response, *in);
            LOG_INFO(getLogger("StorageSystemZooKeeperInfo"), "Response to four letter command : {}   response : {}", command, response);
        }
        catch (...)
        {
            LOG_INFO(getLogger("StorageSystemZooKeeperInfo"), "Exception  {} ", getCurrentExceptionMessage(true));
        }

    return response;
}


}
