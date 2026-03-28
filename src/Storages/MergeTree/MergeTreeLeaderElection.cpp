#include <Storages/MergeTree/MergeTreeLeaderElection.h>

#include <Core/ServerUUID.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Storages/IStorage.h>
#include <Common/ErrorCodes.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <base/JSON.h>
#include <base/getFQDNOrHostName.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int S3_ERROR;
    extern const int AZURE_BLOB_STORAGE_ERROR;
    extern const int TABLE_IS_READ_ONLY;
}

static constexpr size_t MAX_LEASE_FILE_SIZE = 4096;

MergeTreeLeaderElection::MergeTreeLeaderElection(
    const StorageID & storage_id_,
    ObjectStoragePtr object_storage_,
    String lease_path_,
    ContextPtr context_,
    UInt64 heartbeat_interval_ms_,
    UInt64 session_timeout_ms_)
    : storage_id(storage_id_)
    , object_storage(std::move(object_storage_))
    , lease_path(std::move(lease_path_))
    , context(std::move(context_))
    , heartbeat_interval_ms(heartbeat_interval_ms_)
    , session_timeout_ms(session_timeout_ms_)
    , leader_id(generateLeaderId())
    , log(getLogger("MergeTreeLeaderElection"))
{
}

MergeTreeLeaderElection::~MergeTreeLeaderElection()
{
    stop();
}

void MergeTreeLeaderElection::start()
{
    task = context->getSchedulePool().createTask(storage_id, "MergeTreeLeaderElection", [this] { run(); });
    task->activateAndSchedule();
}

void MergeTreeLeaderElection::stop()
{
    if (task)
        task->deactivate();

    bool was_leader = is_leader.exchange(false, std::memory_order_acq_rel);
    if (was_leader && on_leadership_change)
        on_leadership_change(false);
}

bool MergeTreeLeaderElection::isLeader() const
{
    if (!is_leader.load(std::memory_order_acquire))
        return false;

    /// Protect against stalled heartbeat thread: if the last successful renewal
    /// was too long ago (more than session_timeout), we cannot be sure the lease
    /// is still valid.
    auto elapsed = std::chrono::steady_clock::now() - last_renewal_time.load(std::memory_order_acquire);
    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();
    return static_cast<UInt64>(elapsed_ms) < session_timeout_ms;
}

void MergeTreeLeaderElection::assertIsLeader() const
{
    if (!isLeader())
        throw Exception(ErrorCodes::TABLE_IS_READ_ONLY, "Table is in readonly mode because this instance is not the leader");
}

void MergeTreeLeaderElection::run()
{
    try
    {
        bool became_leader = false;

        /// Try to read the existing lease file.
        auto read_settings = context->getReadSettings();
        auto result = object_storage->tryGetObjectMetadata(lease_path, /* with_tags= */ false);

        if (!result)
        {
            /// Lease file does not exist. Try to create it.
            LOG_TRACE(log, "Lease file does not exist at '{}', trying to create", lease_path);
            became_leader = tryWriteLease(/* if_match= */ "", /* if_none_match= */ "*");
        }
        else
        {
            /// Lease file exists. Read its content and ETag.
            auto data_with_metadata = object_storage->readSmallObjectAndGetObjectMetadata(
                StoredObject(lease_path), read_settings, MAX_LEASE_FILE_SIZE);

            String etag = data_with_metadata.metadata.etag;
            auto [file_leader_id, timestamp] = parseLeaseContent(data_with_metadata.data);

            time_t now = time(nullptr);

            if (file_leader_id == leader_id)
            {
                /// We are the current leader. Renew the lease.
                LOG_TRACE(log, "Renewing leader lease at '{}'", lease_path);
                became_leader = tryWriteLease(/* if_match= */ etag, /* if_none_match= */ "");
            }
            else if (now - timestamp > static_cast<time_t>(session_timeout_ms / 1000))
            {
                /// The lease has expired (or was corrupted — parseLeaseContent returns timestamp 0 on failure).
                /// Try to claim leadership.
                LOG_INFO(log, "Leader lease at '{}' expired or corrupted (leader_id: {}, age: {} s), trying to claim",
                    lease_path, file_leader_id, now - timestamp);
                became_leader = tryWriteLease(/* if_match= */ etag, /* if_none_match= */ "");
            }
            else
            {
                /// Another leader holds a valid lease.
                LOG_TRACE(log, "Another leader holds the lease at '{}' (leader_id: {}, age: {} s)",
                    lease_path, file_leader_id, now - timestamp);
                became_leader = false;
            }
        }

        bool was_leader = is_leader.exchange(became_leader, std::memory_order_acq_rel);

        if (became_leader && !was_leader)
        {
            LOG_INFO(log, "Acquired leadership for lease at '{}'", lease_path);
            if (on_leadership_change)
                on_leadership_change(true);
        }
        else if (!became_leader && was_leader)
        {
            LOG_INFO(log, "Lost leadership for lease at '{}'", lease_path);
            if (on_leadership_change)
                on_leadership_change(false);
        }
    }
    catch (...)
    {
        /// On any error, conservatively assume we are not the leader.
        bool was_leader = is_leader.exchange(false, std::memory_order_acq_rel);
        if (was_leader)
        {
            LOG_WARNING(log, "Lost leadership due to exception for lease at '{}'", lease_path);
            if (on_leadership_change)
                on_leadership_change(false);
        }

        tryLogCurrentException(log, "Error in leader election heartbeat");
    }

    task->scheduleAfter(heartbeat_interval_ms);
}

bool MergeTreeLeaderElection::tryWriteLease(const String & if_match, const String & if_none_match)
{
    try
    {
        String content = buildLeaseContent();

        auto write_settings = context->getWriteSettings();
        write_settings.object_storage_write_if_match = if_match;
        write_settings.object_storage_write_if_none_match = if_none_match;

        auto buffer = object_storage->writeObject(
            StoredObject(lease_path),
            WriteMode::Rewrite,
            /* attributes= */ std::nullopt,
            DBMS_DEFAULT_BUFFER_SIZE,
            write_settings);

        buffer->write(content.data(), content.size());
        buffer->finalize();

        /// Retrieve the new ETag after a successful write.
        auto metadata = object_storage->getObjectMetadata(lease_path, /* with_tags= */ false);
        current_etag = metadata.etag;

        last_renewal_time.store(std::chrono::steady_clock::now(), std::memory_order_release);

        return true;
    }
    catch (const Exception & e)
    {
        if ((e.code() == ErrorCodes::S3_ERROR || e.code() == ErrorCodes::AZURE_BLOB_STORAGE_ERROR)
            && (e.message().find("PreconditionFailed") != String::npos
                || e.message().find("ConditionNotMet") != String::npos))
        {
            LOG_TRACE(log, "Conditional write failed (precondition not met) for lease at '{}'", lease_path);
            return false;
        }
        throw;
    }
}

String MergeTreeLeaderElection::buildLeaseContent() const
{
    WriteBufferFromOwnString out;
    writeString(R"({"version":1,"leader_id":")", out);
    writeString(leader_id, out);
    writeString(R"(","timestamp":)", out);
    writeIntText(time(nullptr), out);
    writeChar('}', out);
    return out.str();
}

std::pair<String, time_t> MergeTreeLeaderElection::parseLeaseContent(const String & content)
{
    try
    {
        JSON json(content);
        String file_leader_id = json["leader_id"].getString();
        time_t timestamp = json["timestamp"].getInt();
        return {file_leader_id, timestamp};
    }
    catch (...)
    {
        /// Corrupted lease file: return empty id and zero timestamp so the caller
        /// treats it as an expired lease and attempts to overwrite it.
        tryLogCurrentException("MergeTreeLeaderElection", "Failed to parse lease file content");
        return {"", 0};
    }
}

String MergeTreeLeaderElection::generateLeaderId()
{
    return getFQDNOrHostName() + ":" + toString(ServerUUID::get());
}

}
