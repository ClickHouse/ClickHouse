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
#include <Poco/Net/StreamSocket.h>
#include <Poco/DOM/Document.h>
#include <Poco/DOM/DOMParser.h>
#include <Poco/DOM/DOMWriter.h>
#include <Poco/DOM/NodeList.h>
#include <Poco/DOM/NamedNodeMap.h>
#include <Poco/AutoPtr.h>
#include <Poco/DirectoryIterator.h>
#include <Poco/ConsoleChannel.h>
#include <Common/XMLUtils.h>

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
        /* 5 */ {"is_readonly", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>()), "Is readonly."},

        /// mntr command
        /* 6 */ {"version", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "The ZooKeeper version."},
        /* 7 */ {"avg_latency", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()), "The average latency."},
        /* 8 */ {"max_latency", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()), "The max latency."},
        /* 9 */ {"min_latency", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()), "The min latency."},
        /* 10 */ {"packets_received", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()), "The number of packets received."},
        /* 11 */ {"packets_sent", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()), "The number of packets sent."},
        /* 12 */ {"outstanding_requests", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()), "The number of outstanding requests."},
        /* 13 */ {"server_state", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "Server state."},
        /* 14 */ {"is_leader", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>()), "Is this zookeeper leader."},
        /* 15 */ {"znode_count", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()), "The znode count."},
        /* 16 */ {"watch_count", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()), "The watch count."},
        /* 17 */ {"ephemerals_count", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()), "The ephemerals count."},
        /* 18 */ {"approximate_data_size", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()), "The approximate data size."},
        /* 19 */ {"followers", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()), "The followers of the leader. This field is only exposed by the leader."},
        /* 20 */ {"synced_followers", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()), "The synced followers of the leader. This field is only exposed by the leader."},
        /* 21 */ {"pending_syncs", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()), "The pending syncs of the leader. This field is only exposed by the leader."},
        /* 22 */ {"open_file_descriptor_count", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()), "The open file descriptor count. Only available on Unix platforms."},
        /* 23 */ {"max_file_descriptor_count", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()), "The max file descriptor count. Only available on Unix platforms."},


        ///srvr command
        /* 24 */ {"connections", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()), "The ZooKeeper connections."},
        /* 25 */ {"outstanding", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()), "The ZooKeeper outstanding."},
        /* 26 */ {"zxid", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>()), "The ZooKeeper zxid."},
        /* 27 */ {"node_count", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()), "The ZooKeeper node count."},


        /// dirs command
        /* 28 */ {"snapshot_dir_size", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()), "The ZooKeeper snapshot directory size."},
        /* 29 */ {"log_dir_size", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()), "The ZooKeeper log directory size."},


        /// lgif command
        /* 30 */ {"first_log_idx", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()), "The ZooKeeper first log index."},
        /* 31 */ {"first_log_term", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()), "The ZooKeeper first log term."},
        /* 32 */ {"last_log_idx", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()), "The ZooKeeper last log index."},
        /* 33 */ {"last_log_term", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()), "The ZooKeeper last log term."},
        /* 34 */ {"last_committed_idx", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()), "The ZooKeeper last committed index."},
        /* 35 */ {"leader_committed_log_idx", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()), "The ZooKeeper leader committed log index."},
        /* 36 */ {"target_committed_log_idx", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()), "The ZooKeeper target committed log index."},
        /* 37 */ {"last_snapshot_idx", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()), "The ZooKeeper last snapshot index."},
    };
}

static std::map<String,String> getTokens(String response, char separator)
{
    std::vector<String> result_split;
    splitInto<'\n'>(result_split, response);

    std::map<String,String> responses_map;
    for (auto & line : result_split)
    {
        auto pos = line.rfind(separator);
        if (pos != std::string::npos)
        {
            String key = line.substr(0, pos - 1);
            String value = line.substr(pos + 1);
            responses_map[key] = value;
        }
    }
    return responses_map;
}

void StorageSystemZooKeeperInfo::fillData(MutableColumns & res_columns, ContextPtr context,
                                                    const ActionsDAG::Node *, std::vector<UInt8>) const
{
    auto zk = context->getZooKeeper();
    auto zookeepers = context->getAuxiliaryZooKeepers();
    
    zookeepers["default"] = zk;

    LOG_INFO(getLogger("StorageSystemZooKeeperInfo"), "fillData size zk  {} ", zookeepers.size());


    for (const auto & elem : zookeepers)
    {
        auto zookeeper = elem.second;

        auto index = zookeeper->getConnectedHostIdx();
        String host_port = zookeeper->getConnectedHostPort();

        zkutil::ZooKeeperArgs zk_args = zookeeper->getArgs();

        if (index != -1 && !host_port.empty())
        {
            size_t offset = host_port.find_last_of(':');
            String host = host_port.substr(0, offset);
            String port_string = host_port.substr(offset + 1);
            UInt16 port = static_cast<UInt16>(Poco::NumberParser::parseUnsigned(port_string));

            LOG_INFO(getLogger("StorageSystemZooKeeperInfo"), "zookeeper host {}  port {} ", host,port);

            res_columns[0]->insert(elem.first);
            res_columns[1]->insert(host);
            res_columns[2]->insert(port);

            res_columns[3]->insert(index.value());

            /// ruok command
            auto ruok_output = sendFourLetterCommand(host, port_string, "ruok");
            if (ruok_output.has_value())
            {
                if (ruok_output.value() == "imok")
                    res_columns[4]->insert(true);
                else
                    res_columns[4]->insert(false);
            }


            /// isro command
            /// The server will respond with "ro" if in read-only mode or "rw" if not in read-only mode.
            auto isro_output = sendFourLetterCommand(host, port_string, "isro");
            if (isro_output.has_value())
            {
                if (isro_output.value() == "ro")
                    res_columns[5]->insert(true);
                else
                    res_columns[5]->insert(false);
            }


            /// mntr command
            auto mntr_output_expected = sendFourLetterCommand(host, port_string, "mntr");
            if (mntr_output_expected.has_value())
            {
                const String & mntr_output = mntr_output_expected.value();
                std::map <String, String> mntr_responses_map = getTokens(mntr_output, '\t');

                /// /* 6 */{"version", std::make_shared<DataTypeString>(), "The ZooKeeper version."},
                if (const auto & it = mntr_responses_map.find("zk_version"); it != mntr_responses_map.end())
                    res_columns[6]->insert(parse<int>(it->second));

                // /* 7 */ {"avg_latency", std::make_shared<DataTypeUInt64>(), "The average latency."},
                if (const auto & it = mntr_responses_map.find("zk_avg_latency"); it != mntr_responses_map.end())
                    res_columns[7]->insert(parse<int>(it->second));

                // /* 8 */ {"max_latency", std::make_shared<DataTypeUInt64>(), "The max latency."},
                if (const auto & it = mntr_responses_map.find("zk_max_latency"); it != mntr_responses_map.end())
                    res_columns[8]->insert(parse<int>(it->second));

                //  /* 9 */ {"min_latency", std::make_shared<DataTypeUInt64>(), "The min latency."},
                if (const auto & it = mntr_responses_map.find("zk_min_latency"); it != mntr_responses_map.end())
                    res_columns[9]->insert(parse<int>(it->second));

                // /* 10 */ {"packets_received", std::make_shared<DataTypeUInt64>(), "The number of packets received."},
                if (const auto & it = mntr_responses_map.find("zk_packets_received"); it != mntr_responses_map.end())
                    res_columns[10]->insert(parse<int>(it->second));

                // /* 11 */ {"packets_sent", std::make_shared<DataTypeUInt64>(), "The number of packets sent."},
                if (const auto & it = mntr_responses_map.find("zk_packets_sent"); it != mntr_responses_map.end())
                    res_columns[11]->insert(parse<int>(it->second));

                // /* 12 */ {"outstanding_requests", std::make_shared<DataTypeUInt64>(), "The number of outstanding requests."},
                if (const auto & it = mntr_responses_map.find("zk_outstanding_requests"); it != mntr_responses_map.end())
                    res_columns[12]->insert(parse<int>(it->second));

                // /* 13 */ {"server_state", std::make_shared<DataTypeString>(), "Server state."},
                if (const auto & it = mntr_responses_map.find("zk_server_state"); it != mntr_responses_map.end())
                    res_columns[13]->insert(it->second);

                ///* 15 */ {"znode_count", std::make_shared<DataTypeUInt64>(), "The znode count."},
                int followers = 0;
                if (const auto & it = mntr_responses_map.find("zk_followers"); it != mntr_responses_map.end())
                {
                    auto followers_in_string = mntr_responses_map["zk_followers"];
                    if (!followers_in_string.empty())
                    {
                        followers = parse<int>(followers_in_string);
                    }

                    ///* 14 */ {"is_leader", std::make_shared<DataTypeUInt8>(), "Is this zookeeper leader."},
                    if (followers)
                        res_columns[14]->insert(true);
                    else
                        res_columns[14]->insert(false);
                }

                if (const auto & it = mntr_responses_map.find("zk_znode_count"); it != mntr_responses_map.end())
                    res_columns[15]->insert(parse<int>(it->second));

                // /* 16 */ {"watch_count", std::make_shared<DataTypeUInt64>(), "The watch count."},
                if (const auto & it = mntr_responses_map.find("zk_watch_count"); it != mntr_responses_map.end())
                    res_columns[16]->insert(parse<int>(it->second));

                // /* 17 */ {"ephemerals_count", std::make_shared<DataTypeUInt64>(), "The ephemerals count."},
                if (const auto & it = mntr_responses_map.find("zk_ephemerals_count"); it != mntr_responses_map.end())
                    res_columns[17]->insert(parse<int>(it->second));

                // /* 18 */ {"approximate_data_size", std::make_shared<DataTypeUInt64>(), "The approximate data size."},
                if (const auto & it = mntr_responses_map.find("zk_approximate_data_size"); it != mntr_responses_map.end())
                    res_columns[18]->insert(parse<int>(it->second));

                // /* 19 */ {"followers", std::make_shared<DataTypeUInt64>(), "The followers of the leader. This field is only exposed by the leader."},
                if (const auto & it = mntr_responses_map.find("zk_followers"); it != mntr_responses_map.end())
                    res_columns[19]->insert(parse<int>(it->second));
                else
                    res_columns[19]->insert(0);

                // /* 20 */ {"synced_followers", std::make_shared<DataTypeUInt64>(), "The synced followers of the leader. This field is only exposed by the leader."},
                // /* 21 */ {"pending_syncs", std::make_shared<DataTypeUInt64>(), "The pending syncs of the leader. This field is only exposed by the leader."},
                if (const auto & it = mntr_responses_map.find("zk_synced_followers"); it != mntr_responses_map.end())
                {
                    int synced_followers = parse<int>(it->second);
                    res_columns[20]->insert(synced_followers);
                    res_columns[21]->insert(followers - synced_followers);
                }

                // /* 22 */ {"open_file_descriptor_count", std::make_shared<DataTypeUInt64>(), "The open file descriptor count. Only available on Unix platforms."},
                if (const auto & it = mntr_responses_map.find("zk_open_file_descriptor_count"); it != mntr_responses_map.end())
                    res_columns[22]->insert(parse<int>(it->second));

                // /* 23 */ {"max_file_descriptor_count", std::make_shared<DataTypeUInt64>(), "The max file descriptor count. Only available on Unix platforms."},
                if (const auto & it = mntr_responses_map.find("zk_max_file_descriptor_count"); it != mntr_responses_map.end())
                    res_columns[23]->insert(parse<int>(it->second));
            }

            /// srvr command
            auto srvr_output_expected = sendFourLetterCommand(host, port_string, "srvr");
            if (srvr_output_expected.has_value())
            {
                const String & srvr_output = srvr_output_expected.value();
                std::map <String, String> srvr_responses_map = getTokens(srvr_output, ':');

                ///* 24 */ {"connections", std::make_shared<DataTypeUInt64>(), "The ZooKeeper connections."},
                if (const auto & it = srvr_responses_map.find("Connections"); it != srvr_responses_map.end())
                    res_columns[24]->insert(parse<int>(it->second));

                ///* 25 */ {"outstanding", std::make_shared<DataTypeUInt64>(), "The ZooKeeper outstanding."},
                if (const auto & it = srvr_responses_map.find("Outstanding"); it != srvr_responses_map.end())
                    res_columns[25]->insert(parse<int>(it->second));

                //* 26 */ {"zxid", std::make_shared<DataTypeInt64>(), "The ZooKeeper zxid."},
                if (const auto & it = srvr_responses_map.find("Zxid"); it != srvr_responses_map.end())
                    res_columns[26]->insert(parse<int>(it->second));

                //* 27 */ {"node_count", std::make_shared<DataTypeUInt64>(), "The ZooKeeper node count."},
                if (const auto & it = srvr_responses_map.find("Node count"); it != srvr_responses_map.end())
                    res_columns[27]->insert(parse<int>(it->second));
            }


            /// dirs command
            auto dirs_output_expected = sendFourLetterCommand(host, port_string, "dirs");
            if (dirs_output_expected.has_value())
            {
                const String & dirs_output = dirs_output_expected.value();
                std::map <String, String> dirs_responses_map = getTokens(dirs_output, ':');

                //* 28 */ {"snapshot_dir_size", std::make_shared<DataTypeUInt64>(), "The ZooKeeper snapshot directory size."},
                if (const auto & it = dirs_responses_map.find("snapshot_dir_size"); it != dirs_responses_map.end())
                    res_columns[28]->insert(parse<int>(it->second));

                //* 29 */ {"log_dir_size", std::make_shared<DataTypeUInt64>(), "The ZooKeeper log directory size."},
                if (const auto & it = dirs_responses_map.find("log_dir_size"); it != dirs_responses_map.end())
                    res_columns[29]->insert(parse<int>(it->second));
            }

            /// lgif command
            auto lgif_output_expected = sendFourLetterCommand(host, port_string, "lgif");
            if (lgif_output_expected.has_value())
            {
                const String & lgif_output = lgif_output_expected.value();
                std::map <String, String> lgif_responses_map = getTokens(lgif_output, '\t');

                // /* 30 */ {"first_log_idx", std::make_shared<DataTypeUInt64>(), "The ZooKeeper first log index."},
                if (const auto & it = lgif_responses_map.find("snapshot_dir_size"); it != lgif_responses_map.end())
                    res_columns[30]->insert(parse<int>(it->second));

                // /* 31 */ {"first_log_term", std::make_shared<DataTypeUInt64>(), "The ZooKeeper first log term."},
                if (const auto & it = lgif_responses_map.find("first_log_term"); it != lgif_responses_map.end())
                    res_columns[31]->insert(parse<int>(it->second));

                // /* 32 */ {"last_log_idx", std::make_shared<DataTypeUInt64>(), "The ZooKeeper last log index."},
                if (const auto & it = lgif_responses_map.find("last_log_idx"); it != lgif_responses_map.end())
                    res_columns[32]->insert(parse<int>(it->second));

                // /* 33 */ {"last_log_term", std::make_shared<DataTypeUInt64>(), "The ZooKeeper last log term."},
                if (const auto & it = lgif_responses_map.find("last_log_term"); it != lgif_responses_map.end())
                    res_columns[33]->insert(parse<int>(it->second));

                // /* 34 */ {"last_committed_idx", std::make_shared<DataTypeUInt64>(), "The ZooKeeper last committed index."},
                if (const auto & it = lgif_responses_map.find("last_committed_log_idx"); it != lgif_responses_map.end())
                    res_columns[34]->insert(parse<int>(it->second));

                // /* 35 */ {"leader_committed_log_idx", std::make_shared<DataTypeUInt64>(), "The ZooKeeper leader committed log index."},
                if (const auto & it = lgif_responses_map.find("leader_committed_log_idx"); it != lgif_responses_map.end())
                    res_columns[35]->insert(parse<int>(it->second));

                // /* 36 */ {"target_committed_log_idx", std::make_shared<DataTypeUInt64>(), "The ZooKeeper target committed log index."},
                if (const auto & it = lgif_responses_map.find("target_committed_log_idx"); it != lgif_responses_map.end())
                    res_columns[36]->insert(parse<int>(it->second));

                // /* 37 */ {"last_snapshot_idx", std::make_shared<DataTypeUInt64>(), "The ZooKeeper last snapshot index."},
                if (const auto & it = lgif_responses_map.find("last_snapshot_idx"); it != lgif_responses_map.end())
                    res_columns[37]->insert(parse<int>(it->second));
            }

        }


        LOG_INFO(getLogger("StorageSystemZooKeeperInfo"), "zookeeper cluster name {}  host size {} ", elem.first, zk_args.hosts.size());
    }
}


std::expected<String,String> StorageSystemZooKeeperInfo::sendFourLetterCommand(String host, String port, String command) const
{
    Poco::Net::SocketAddress address(host, port);
    Poco::Net::StreamSocket socket;

    String response;
    try
    {
        socket = Poco::Net::StreamSocket();
        socket.connect(address);
        socket.setNoDelay(true);

        auto in = std::make_shared<ReadBufferFromPocoSocket>(socket);
        auto out = std::make_shared<AutoCanceledWriteBuffer<WriteBufferFromPocoSocket>>(socket);

        int32_t res = *reinterpret_cast<const int32_t *>(command.data());
        /// keep consistent with Coordination::read method by changing big endian to little endian.
        int32_t cmd_int = std::byteswap(res);

        Coordination::write(cmd_int,*out);
        out->next();

        readBinary(response,*in);
    }
    catch (...)
    {
        LOG_INFO(getLogger("StorageSystemZooKeeperInfo"), "Exception  {} ", getCurrentExceptionMessage(true));
        return getCurrentExceptionMessage(true);
    }

    return response;
}


}
