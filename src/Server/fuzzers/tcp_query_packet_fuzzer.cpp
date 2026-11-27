#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Core/Field.h>
#include <Core/ParallelReplicasProtocolDefines.h>
#include <Core/ProtocolDefines.h>
#include <Core/Settings.h>
#include <Functions/registerFunctions.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/ClientInfo.h>
#include <Interpreters/ClusterFunctionReadTask.h>
#include <Interpreters/Context.h>
#include <Parsers/SecretArguments.h>
#include <Storages/MergeTree/RequestResponse.h>
#include <Common/CurrentThread.h>
#include <Common/MemoryTracker.h>
#include <Common/ThreadStatus.h>


/// The input is a packet body the server or the client of the native protocol reads from its peer
/// (`TCPHandler`, `Connection`). The first byte selects the packet, the second one lowers the protocol revision.

using namespace DB;

/// No engine is registered, so the secrets of their arguments are shown.
[[maybe_unused]] static const bool secret_arguments_finder_installed
    = (setSecretArgumentsFinder(&NoSecretArgumentsFinder::instance()), true);

namespace
{

/// The same limits as in `TCPHandler::processQuery`.
constexpr size_t MAX_HELLO_STRING_SIZE = 64 * 1024;
constexpr size_t MAX_EXTRA_ROLES_SIZE = 1024 * 1024;
constexpr size_t MAX_EXTRA_ROLES = 65536;
constexpr size_t MAX_EXTRA_ROLE_NAME_SIZE = 64 * 1024;

ContextMutablePtr context;

/// What a peer reads back must be written the same way, otherwise a value changes on each hop.
template <typename Write, typename Read>
void checkReserialization(Write && write, Read && read)
{
    WriteBufferFromOwnString first;
    write(first);
    first.finalize();

    ReadBufferFromString in(first.str());
    read(in);
    if (!in.eof())
        __builtin_trap();

    WriteBufferFromOwnString second;
    write(second);
    second.finalize();
    if (first.str() != second.str())
        __builtin_trap();
}

void readQueryPacket(ReadBuffer & in, UInt64 revision)
{
    String query_id;
    readStringBinary(query_id, in, MAX_HELLO_STRING_SIZE);

    ClientInfo client_info;
    client_info.read(in, revision);
    if (client_info.query_kind != ClientInfo::QueryKind::INITIAL_QUERY
        && client_info.query_kind != ClientInfo::QueryKind::SECONDARY_QUERY)
        return;
    checkReserialization(
        [&](WriteBuffer & out) { client_info.write(out, revision); },
        [&](ReadBuffer & reread_in) { client_info.read(reread_in, revision); });

    Settings settings;
    if (revision >= DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS)
    {
        settings.read(in);
        checkReserialization(
            [&](WriteBuffer & out) { settings.write(out); },
            [&](ReadBuffer & reread_in) { settings.read(reread_in); });
    }

    if (revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_INTERSERVER_EXTERNALLY_GRANTED_ROLES)
    {
        String extra_roles;
        readStringBinary(extra_roles, in, MAX_EXTRA_ROLES_SIZE);
        if (!extra_roles.empty())
        {
            ReadBufferFromString roles_in(extra_roles);
            Strings roles;
            readVectorBinary(roles, roles_in, MAX_EXTRA_ROLES, MAX_EXTRA_ROLE_NAME_SIZE);
        }
    }

    if (revision >= DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET)
    {
        String hash;
        readStringBinary(hash, in, 32);
    }

    UInt64 stage = 0;
    readVarUInt(stage, in);
    UInt64 compression = 0;
    readVarUInt(compression, in);

    String query;
    readStringBinary(query, in);

    if (revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS)
    {
        auto parameters = readQueryParameters(in);
        checkReserialization(
            [&](WriteBuffer & out) { writeQueryParameters(parameters, out); },
            [&](ReadBuffer & reread_in) { parameters = readQueryParameters(reread_in); });
    }
}

}

extern "C" int LLVMFuzzerInitialize(int *, char ***);
extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size);

extern "C" int LLVMFuzzerInitialize(int *, char ***)
{
    static SharedContextHolder shared_context = Context::createShared();
    context = Context::createGlobal(shared_context.get());
    context->makeGlobalContext();

    MainThreadStatus::getInstance();

    registerFunctions();
    registerAggregateFunctions();
    return 0;
}

extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size)
{
    if (size < 2)
        return 0;

    const uint8_t packet = data[0];
    const UInt64 revision = DBMS_TCP_PROTOCOL_VERSION - data[1];
    const UInt64 pr_version = DBMS_MIN_SUPPORTED_PARALLEL_REPLICAS_PROTOCOL_VERSION
        + data[1] % (DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION - DBMS_MIN_SUPPORTED_PARALLEL_REPLICAS_PROTOCOL_VERSION + 1);

    try
    {
        total_memory_tracker.resetCounters();
        total_memory_tracker.setHardLimit(1_GiB);
        CurrentThread::get().memory_tracker.resetCounters();
        CurrentThread::get().memory_tracker.setHardLimit(1_GiB);

        ReadBufferFromMemory in(data + 2, size - 2);
        switch (packet % 7)
        {
            case 0:
                readQueryPacket(in, revision);
                break;
            case 1:
            {
                /// From the initiator to a replica, `MergeTreeReadTaskResponse`.
                ParallelReadResponse response;
                response.deserialize(in, pr_version);
                std::ignore = response.describe();
                break;
            }
            case 2:
            {
                /// From the initiator to a replica, `MergeTreeAllRangesAnnouncementResponse`.
                auto response = InitialAllRangesAnnouncementResponse::deserialize(in, pr_version);
                std::ignore = response.describe();
                break;
            }
            case 3:
            {
                /// From a replica to the initiator, `MergeTreeReadTaskRequest`.
                auto request = ParallelReadRequest::deserialize(in, pr_version);
                std::ignore = request.describe();
                break;
            }
            case 4:
            {
                /// From a replica to the initiator, `MergeTreeAllRangesAnnouncement`.
                auto announcement = InitialAllRangesAnnouncement::deserialize(in, pr_version);
                std::ignore = announcement.describe();
                break;
            }
            case 5:
            {
                /// From the initiator of a `*Cluster` table function to a replica, `ReadTaskResponse`.
                ClusterFunctionReadTaskResponse response;
                response.deserialize(in);
                break;
            }
            case 6:
            {
                /// Fields are read from query plans, statistics and lake metadata.
                Field field = readFieldBinary(in);
                std::ignore = field.dump();
                checkReserialization(
                    [&](WriteBuffer & out) { writeFieldBinary(field, out); },
                    [&](ReadBuffer & reread_in) { field = readFieldBinary(reread_in); });
                break;
            }
            default:
                break;
        }
    }
    catch (...) // NOLINT(bugprone-empty-catch)
    {
        /// Malformed packets are rejected with exceptions.
    }

    return 0;
}
