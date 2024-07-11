#pragma once

#include <base/types.h>
#include <Core/ProtocolDefines.h>


namespace DB
{


/// Client-server protocol.
///
/// Client opens a connection and sends Hello packet.
/// If client version is incompatible, the server can terminate the connection.
/// Server responds with Hello packet.
/// If server version is incompatible, the client can terminate the connection.
///
/// The main loop follows:
///
/// 1. The client sends Query packet.
///
/// Starting from version 50263 immediately after sending the Query packet the client starts
/// transfer of external (temporary) table (external storages) - one or several Data packets.
/// End of transmission is marked by an empty block.
/// At present, non-empty tables can be sent only along with SELECT query.
///
/// If the query is an INSERT (and thus requires data transfer from client), then the server transmits
/// Data packet containing empty block that describes the table structure.
/// Then the client sends one or several Data packets - data for insertion.
/// End of data is marked by the transmission of empty block.
/// Then the server sends EndOfStream packet.
///
/// If the query is a SELECT or a query of other type, then the server transmits packets of
/// one of the following types:
/// - Data - data corresponding to one block of query results.
/// - Progress - query execution progress.
/// - Exception - error description.
/// - EndOfStream - the end of data transmission.
///
/// The client should read packets until EndOfStream or Exception.
///
/// The client can also send Cancel packet - a request to cancel the query.
/// In this case the server can stop executing the query and return incomplete data,
/// but the client must still read until EndOfStream packet.
///
/// Also if there is profiling info and the client revision is recent enough, the server can
/// send one of the following packets before EndOfStream:
/// - Totals - a block with total values
/// - ProfileInfo - serialized BlockStreamProfileInfo structure.
///
/// If a query returns data, the server sends an empty header block containing
/// the description of resulting columns before executing the query.
/// Using this block the client can initialize the output formatter and display the prefix of resulting table
/// beforehand.

namespace EncodedUserInfo
{

/// Marker for the inter-server secret (passed as the user name)
/// (anyway user cannot be started with a whitespace)
const char USER_INTERSERVER_MARKER[] = " INTERSERVER SECRET ";

/// Marker for SSH-keys-based authentication (passed as the user name)
const char SSH_KEY_AUTHENTICAION_MARKER[] = " SSH KEY AUTHENTICATION ";

/// Marker for JSON Web Token authentication
const char JWT_AUTHENTICAION_MARKER[] = " JWT AUTHENTICATION ";

};

namespace Protocol
{
    /// Packet types that server transmits.
    namespace Server
    {
        enum Enum
        {
            Hello = 0,                      /// Name, version, revision.
            Data = 1,                       /// A block of data (compressed or not).
            Exception = 2,                  /// The exception during query execution.
            Progress = 3,                   /// Query execution progress: rows read, bytes read.
            Pong = 4,                       /// Ping response
            EndOfStream = 5,                /// All packets were transmitted
            ProfileInfo = 6,                /// Packet with profiling info.
            Totals = 7,                     /// A block with totals (compressed or not).
            Extremes = 8,                   /// A block with minimums and maximums (compressed or not).
            TablesStatusResponse = 9,       /// A response to TablesStatus request.
            Log = 10,                       /// System logs of the query execution
            TableColumns = 11,              /// Columns' description for default values calculation
            PartUUIDs = 12,                 /// List of unique parts ids.
            ReadTaskRequest = 13,           /// String (UUID) describes a request for which next task is needed
                                            /// This is such an inverted logic, where server sends requests
                                            /// And client returns back response
            ProfileEvents = 14,             /// Packet with profile events from server.
            MergeTreeAllRangesAnnouncement = 15,
            MergeTreeReadTaskRequest = 16,  /// Request from a MergeTree replica to a coordinator
            TimezoneUpdate = 17,            /// Receive server's (session-wide) default timezone
            SSHChallenge = 18,              /// Return challenge for SSH signature signing
            MAX = SSHChallenge,

        };

        /// NOTE: If the type of packet argument would be Enum, the comparison packet >= 0 && packet < 10
        /// would always be true because of compiler optimization. That would lead to out-of-bounds error
        /// if the packet is invalid.
        /// See https://www.securecoding.cert.org/confluence/display/cplusplus/INT36-CPP.+Do+not+use+out-of-range+enumeration+values
        inline const char * toString(UInt64 packet)
        {
            static const char * data[] = {
                "Hello",
                "Data",
                "Exception",
                "Progress",
                "Pong",
                "EndOfStream",
                "ProfileInfo",
                "Totals",
                "Extremes",
                "TablesStatusResponse",
                "Log",
                "TableColumns",
                "PartUUIDs",
                "ReadTaskRequest",
                "ProfileEvents",
                "MergeTreeAllRangesAnnouncement",
                "MergeTreeReadTaskRequest",
                "TimezoneUpdate",
                "SSHChallenge",
            };
            return packet <= MAX
                ? data[packet]
                : "Unknown packet";
        }

        inline size_t stringsInMessage(UInt64 msg_type)
        {
            switch (msg_type)
            {
                case TableColumns:
                    return 2;
                default:
                    break;
            }
            return 0;
        }
    }

    /// Packet types that client transmits.
    namespace Client
    {
        enum Enum
        {
            Hello = 0,                      /// Name, version, revision, default DB
            Query = 1,                      /// Query id, query settings, stage up to which the query must be executed,
                                            /// whether the compression must be used,
                                            /// query text (without data for INSERTs).
            Data = 2,                       /// A block of data (compressed or not).
            Cancel = 3,                     /// Cancel the query execution.
            Ping = 4,                       /// Check that connection to the server is alive.
            TablesStatusRequest = 5,        /// Check status of tables on the server.
            KeepAlive = 6,                  /// Keep the connection alive
            Scalar = 7,                     /// A block of data (compressed or not).
            IgnoredPartUUIDs = 8,           /// List of unique parts ids to exclude from query processing
            ReadTaskResponse = 9,           /// A filename to read from s3 (used in s3Cluster)
            MergeTreeReadTaskResponse = 10, /// Coordinator's decision with a modified set of mark ranges allowed to read

            SSHChallengeRequest = 11,       /// Request SSH signature challenge
            SSHChallengeResponse = 12,      /// Reply to SSH signature challenge
            MAX = SSHChallengeResponse,
        };

        inline const char * toString(UInt64 packet)
        {
            static const char * data[] = {
                "Hello",
                "Query",
                "Data",
                "Cancel",
                "Ping",
                "TablesStatusRequest",
                "KeepAlive",
                "Scalar",
                "IgnoredPartUUIDs",
                "ReadTaskResponse",
                "MergeTreeReadTaskResponse",
                "SSHChallengeRequest",
                "SSHChallengeResponse"
            };
            return packet <= MAX
                ? data[packet]
                : "Unknown packet";
        }
    }

    /// Whether the compression must be used.
    enum class Compression : uint8_t
    {
        Disable = 0,
        Enable = 1,
    };

    /// Whether the ssl must be used.
    enum class Secure : uint8_t
    {
        Disable = 0,
        Enable = 1,
    };

}

}
