#include <string>

#include <Core/MySQL/Authentication.h>
#include <Core/MySQL/MySQLClient.h>
#include <Core/MySQL/PacketsConnection.h>
#include <Core/MySQL/PacketsGeneric.h>
#include <Core/MySQL/PacketsProtocolText.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteBufferFromOStream.h>

#include <boost/program_options.hpp>

int main(int argc, char ** argv)
{
    using namespace DB;
    using namespace MySQLProtocol;
    using namespace MySQLProtocol::Generic;
    using namespace MySQLProtocol::Authentication;
    using namespace MySQLProtocol::ConnectionPhase;
    using namespace MySQLProtocol::ProtocolText;


    uint8_t server_sequence_id = 1;
    uint8_t client_sequence_id = 1;
    String user = "default";
    String password = "123";
    String database;

    UInt8 charset_utf8 = 33;
    UInt32 max_packet_size = MAX_PACKET_LENGTH;
    String mysql_native_password = "mysql_native_password";

    UInt32 server_capability_flags = CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH
        | CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA | CLIENT_CONNECT_WITH_DB | CLIENT_DEPRECATE_EOF;

    UInt32 client_capability_flags = CLIENT_PROTOCOL_41 | CLIENT_PLUGIN_AUTH | CLIENT_SECURE_CONNECTION;

    /// Handshake packet
    {
        /// 1. Greeting:
        /// 1.1 Server writes greeting to client
        std::string s0;
        WriteBufferFromString out0(s0);

        Handshake server_handshake(
            server_capability_flags, -1, "ClickHouse", "mysql_native_password", "aaaaaaaaaaaaaaaaaaaaa", CharacterSet::utf8_general_ci);
        server_handshake.writePayload(out0, server_sequence_id);

        /// 1.2 Client reads the greeting
        ReadBufferFromString in0(s0);
        Handshake client_handshake;
        client_handshake.readPayload(in0, client_sequence_id);

        /// Check packet
        ASSERT(server_handshake.capability_flags == client_handshake.capability_flags)
        ASSERT(server_handshake.status_flags == client_handshake.status_flags)
        ASSERT(server_handshake.server_version == client_handshake.server_version)
        ASSERT(server_handshake.protocol_version == client_handshake.protocol_version)
        ASSERT(server_handshake.auth_plugin_data.substr(0, 20) == client_handshake.auth_plugin_data)
        ASSERT(server_handshake.auth_plugin_name == client_handshake.auth_plugin_name)

        /// 2. Greeting Response:
        std::string s1;
        WriteBufferFromString out1(s1);

        /// 2.1 Client writes to server
        Native41 native41(password, client_handshake.auth_plugin_data);
        String auth_plugin_data = native41.getAuthPluginData();
        HandshakeResponse client_handshake_response(
            client_capability_flags, max_packet_size, charset_utf8, user, database, auth_plugin_data, mysql_native_password);
        client_handshake_response.writePayload(out1, client_sequence_id);

        /// 2.2 Server reads the response
        ReadBufferFromString in1(s1);
        HandshakeResponse server_handshake_response;
        server_handshake_response.readPayload(in1, server_sequence_id);

        /// Check
        ASSERT(server_handshake_response.capability_flags == client_handshake_response.capability_flags)
        ASSERT(server_handshake_response.character_set == client_handshake_response.character_set)
        ASSERT(server_handshake_response.username == client_handshake_response.username)
        ASSERT(server_handshake_response.database == client_handshake_response.database)
        ASSERT(server_handshake_response.auth_response == client_handshake_response.auth_response)
        ASSERT(server_handshake_response.auth_plugin_name == client_handshake_response.auth_plugin_name)
    }

    /// OK Packet
    {
        // 1. Server writes packet
        std::string s0;
        WriteBufferFromString out0(s0);
        OKPacket server(0x00, server_capability_flags, 0, 0, 0, "", "");
        server.writePayload(out0, server_sequence_id);

        // 2. Client reads packet
        ReadBufferFromString in0(s0);
        ResponsePacket client(server_capability_flags);
        client.readPayload(in0, client_sequence_id);

        // Check
        ASSERT(client.getType() == PACKET_OK)
        ASSERT(client.ok.header == server.header)
        ASSERT(client.ok.status_flags == server.status_flags)
        ASSERT(client.ok.capabilities == server.capabilities)
    }

    /// ERR Packet
    {
        // 1. Server writes packet
        std::string s0;
        WriteBufferFromString out0(s0);
        ERRPacket server(123, "12345", "This is the error message");
        server.writePayload(out0, server_sequence_id);

        // 2. Client reads packet
        ReadBufferFromString in0(s0);
        ResponsePacket client(server_capability_flags);
        client.readPayload(in0, client_sequence_id);

        // Check
        ASSERT(client.getType() == PACKET_ERR)
        ASSERT(client.err.header == server.header)
        ASSERT(client.err.error_code == server.error_code)
        ASSERT(client.err.sql_state == server.sql_state)
        ASSERT(client.err.error_message == server.error_message)
    }

    /// EOF Packet
    {
        // 1. Server writes packet
        std::string s0;
        WriteBufferFromString out0(s0);
        EOFPacket server(1, 1);
        server.writePayload(out0, server_sequence_id);

        // 2. Client reads packet
        ReadBufferFromString in0(s0);
        ResponsePacket client(server_capability_flags);
        client.readPayload(in0, client_sequence_id);

        // Check
        ASSERT(client.getType() == PACKET_EOF)
        ASSERT(client.eof.header == server.header)
        ASSERT(client.eof.warnings == server.warnings)
        ASSERT(client.eof.status_flags == server.status_flags)
    }

    /// ColumnDefinition Packet
    {
        // 1. Server writes packet
        std::string s0;
        WriteBufferFromString out0(s0);
        ColumnDefinition server("schema", "tbl", "org_tbl", "name", "org_name", 33, 0x00, MYSQL_TYPE_STRING, 0x00, 0x00);
        server.writePayload(out0, server_sequence_id);

        // 2. Client reads packet
        ReadBufferFromString in0(s0);
        ColumnDefinition client;
        client.readPayload(in0, client_sequence_id);

        // Check
        ASSERT(client.column_type == server.column_type)
        ASSERT(client.column_length == server.column_length)
        ASSERT(client.next_length == server.next_length)
        ASSERT(client.character_set == server.character_set)
        ASSERT(client.decimals == server.decimals)
        ASSERT(client.name == server.name)
        ASSERT(client.org_name == server.org_name)
        ASSERT(client.table == server.table)
        ASSERT(client.org_table == server.org_table)
        ASSERT(client.schema == server.schema)
    }

    /// GTID sets tests.
   {
        struct Testcase
        {
            String name;
            String sets;
            String want;
        };

        Testcase cases[] = {
            {"gtid-sets-without-whitespace",
             "2c5adab4-d64a-11e5-82df-ac162d72dac0:1-247743812,9f58c169-d121-11e7-835b-ac162db9c048:1-56060985:56060987-56061175:56061177-"
             "56061224:56061226-75201528:75201530-75201755:75201757-75201983:75201985-75407550:75407552-75407604:75407606-75407661:"
             "75407663-87889848:87889850-87889935:87889937-87890042:87890044-88391955:88391957-88392125:88392127-88392245:88392247-"
             "88755771:88755773-88755826:88755828-88755921:88755923-100279047:100279049-100279126:100279128-100279247:100279249-121672430:"
             "121672432-121672503:121672505-121672524:121672526-122946019:122946021-122946291:122946293-122946469:122946471-134313284:"
             "134313286-134313415:134313417-134313648:134313650-136492728:136492730-136492784:136492786-136492904:136492906-145582402:"
             "145582404-145582439:145582441-145582463:145582465-147455222:147455224-147455262:147455264-147455277:147455279-149319049:"
             "149319051-149319261:149319263-150635915,a6d83ff6-bfcf-11e7-8c93-246e96158550:1-126618302",
             "2c5adab4-d64a-11e5-82df-ac162d72dac0:1-247743812,9f58c169-d121-11e7-835b-ac162db9c048:1-56060985:56060987-56061175:56061177-"
             "56061224:56061226-75201528:75201530-75201755:75201757-75201983:75201985-75407550:75407552-75407604:75407606-75407661:"
             "75407663-87889848:87889850-87889935:87889937-87890042:87890044-88391955:88391957-88392125:88392127-88392245:88392247-"
             "88755771:88755773-88755826:88755828-88755921:88755923-100279047:100279049-100279126:100279128-100279247:100279249-121672430:"
             "121672432-121672503:121672505-121672524:121672526-122946019:122946021-122946291:122946293-122946469:122946471-134313284:"
             "134313286-134313415:134313417-134313648:134313650-136492728:136492730-136492784:136492786-136492904:136492906-145582402:"
             "145582404-145582439:145582441-145582463:145582465-147455222:147455224-147455262:147455264-147455277:147455279-149319049:"
             "149319051-149319261:149319263-150635915,a6d83ff6-bfcf-11e7-8c93-246e96158550:1-126618302"},

            {"gtid-sets-with-whitespace",
             "2c5adab4-d64a-11e5-82df-ac162d72dac0:1-247743812, 9f58c169-d121-11e7-835b-ac162db9c048:1-56060985:56060987-56061175:56061177",
             "2c5adab4-d64a-11e5-82df-ac162d72dac0:1-247743812,9f58c169-d121-11e7-835b-ac162db9c048:1-56060985:56060987-56061175:56061177"},

            {"gtid-sets-single", "2c5adab4-d64a-11e5-82df-ac162d72dac0:1-247743812", "2c5adab4-d64a-11e5-82df-ac162d72dac0:1-247743812"}};

        for (auto & tc : cases)
        {
            GTIDSets gtid_sets;
            gtid_sets.parse(tc.sets);

            String want = tc.want;
            String got = gtid_sets.toString();
            ASSERT(want == got)
        }
    }

    {
        struct Testcase
        {
            String name;
            String gtid_sets;
            String gtid_str;
            String want;
        };

        Testcase cases[] = {
            {"merge",
             "10662d71-9d91-11ea-bbc2-0242ac110003:1-2:4-7",
             "10662d71-9d91-11ea-bbc2-0242ac110003:3",
             "10662d71-9d91-11ea-bbc2-0242ac110003:1-7"},

            {"merge-front",
             "10662d71-9d91-11ea-bbc2-0242ac110003:1-2:5-7",
             "10662d71-9d91-11ea-bbc2-0242ac110003:3",
             "10662d71-9d91-11ea-bbc2-0242ac110003:1-3:5-7"},

            {"extend-interval",
             "10662d71-9d91-11ea-bbc2-0242ac110003:1-2:6-7",
             "10662d71-9d91-11ea-bbc2-0242ac110003:4",
             "10662d71-9d91-11ea-bbc2-0242ac110003:1-2:4:6-7"},

            {"extend-interval",
             "10662d71-9d91-11ea-bbc2-0242ac110003:1-2:4:7-9",
             "10662d71-9d91-11ea-bbc2-0242ac110003:5",
             "10662d71-9d91-11ea-bbc2-0242ac110003:1-2:4-5:7-9"},

            {"extend-interval",
             "10662d71-9d91-11ea-bbc2-0242ac110003:6-7",
             "10662d71-9d91-11ea-bbc2-0242ac110003:4",
             "10662d71-9d91-11ea-bbc2-0242ac110003:4:6-7"},

            {"extend-interval",
             "10662d71-9d91-11ea-bbc2-0242ac110003:6-7",
             "10662d71-9d91-11ea-bbc2-0242ac110003:9",
             "10662d71-9d91-11ea-bbc2-0242ac110003:6-7:9"},

            {"extend-interval",
             "10662d71-9d91-11ea-bbc2-0242ac110003:6-7",
             "20662d71-9d91-11ea-bbc2-0242ac110003:9",
             "10662d71-9d91-11ea-bbc2-0242ac110003:6-7,20662d71-9d91-11ea-bbc2-0242ac110003:9"},

            {"shirnk-sequence",
              "10662d71-9d91-11ea-bbc2-0242ac110003:1-3:4-5:7",
             "10662d71-9d91-11ea-bbc2-0242ac110003:6",
             "10662d71-9d91-11ea-bbc2-0242ac110003:1-7"},

            {"shirnk-sequence",
             "10662d71-9d91-11ea-bbc2-0242ac110003:1-3:4-5:10",
             "10662d71-9d91-11ea-bbc2-0242ac110003:8",
             "10662d71-9d91-11ea-bbc2-0242ac110003:1-5:8:10"
            }
        };

        for (auto & tc : cases)
        {
            GTIDSets gtid_sets;
            gtid_sets.parse(tc.gtid_sets);
            ASSERT(tc.gtid_sets == gtid_sets.toString())

            GTIDSets gtid_sets1;
            gtid_sets1.parse(tc.gtid_str);

            GTID gtid;
            gtid.uuid = gtid_sets1.sets[0].uuid;
            gtid.seq_no = gtid_sets1.sets[0].intervals[0].start;
            gtid_sets.update(gtid);

            String want = tc.want;
            String got = gtid_sets.toString();
            ASSERT(want == got)
        }
    }

    {
        /// mysql_protocol --host=172.17.0.3 --user=root --password=123 --db=sbtest
        try
        {
            boost::program_options::options_description desc("Allowed options");
            desc.add_options()("host", boost::program_options::value<std::string>()->required(), "master host")(
                "port", boost::program_options::value<std::int32_t>()->default_value(3306), "master port")(
                "user", boost::program_options::value<std::string>()->default_value("root"), "master user")(
                "password", boost::program_options::value<std::string>()->required(), "master password")(
                "gtid", boost::program_options::value<std::string>()->default_value(""), "executed GTID sets")(
                "db", boost::program_options::value<std::string>()->required(), "replicate do db")(
                "binlog_checksum", boost::program_options::value<std::string>()->default_value("CRC32"), "master binlog_checksum");

            boost::program_options::variables_map options;
            boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), options);
            if (argc == 0)
            {
                return 1;
            }

            auto host = options.at("host").as<DB::String>();
            auto port = options.at("port").as<DB::Int32>();
            auto master_user = options.at("user").as<DB::String>();
            auto master_password = options.at("password").as<DB::String>();
            auto gtid_sets = options.at("gtid").as<DB::String>();
            auto replicate_db = options.at("db").as<DB::String>();
            auto binlog_checksum = options.at("binlog_checksum").as<String>();

            std::cerr << "Master Host: " << host << ", Port: " << port << ", User: " << master_user << ", Password: " << master_password
                      << ", Replicate DB: " << replicate_db << ", GTID: " << gtid_sets << std::endl;

            UInt32 slave_id = 9004;
            MySQLClient slave(host, port, master_user, master_password);

            /// Connect to the master.
            slave.connect();
            slave.startBinlogDumpGTID(slave_id, replicate_db, gtid_sets, binlog_checksum);

            WriteBufferFromOStream cerr(std::cerr);

            /// Read one binlog event on by one.
            while (true)
            {
                auto event = slave.readOneBinlogEvent();
                switch (event->type())
                {
                    case MYSQL_QUERY_EVENT: {
                        auto binlog_event = std::static_pointer_cast<QueryEvent>(event);
                        binlog_event->dump(cerr);

                        Position pos = slave.getPosition();
                        pos.dump(cerr);
                        break;
                    }
                    case MYSQL_WRITE_ROWS_EVENT: {
                        auto binlog_event = std::static_pointer_cast<WriteRowsEvent>(event);
                        binlog_event->dump(cerr);

                        Position pos = slave.getPosition();
                        pos.dump(cerr);
                        break;
                    }
                    case MYSQL_UPDATE_ROWS_EVENT: {
                        auto binlog_event = std::static_pointer_cast<UpdateRowsEvent>(event);
                        binlog_event->dump(cerr);

                        Position pos = slave.getPosition();
                        pos.dump(cerr);
                        break;
                    }
                    case MYSQL_DELETE_ROWS_EVENT: {
                        auto binlog_event = std::static_pointer_cast<DeleteRowsEvent>(event);
                        binlog_event->dump(cerr);

                        Position pos = slave.getPosition();
                        pos.dump(cerr);
                        break;
                    }
                    default:
                        if (event->header.type != MySQLReplication::EventType::HEARTBEAT_EVENT)
                        {
                            event->dump(cerr);
                        }
                        break;
                }
            }
        }
        catch (const Exception & ex)
        {
            std::cerr << "Error: " << ex.message() << std::endl;
            return 1;
        }
    }
}
