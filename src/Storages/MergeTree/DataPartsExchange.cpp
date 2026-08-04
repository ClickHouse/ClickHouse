#include <Storages/MergeTree/DataPartsExchange.h>

#include "config.h"

#include <Disks/IO/createReadBufferFromFileBase.h>
#include <Disks/SingleDiskVolume.h>
#include <Disks/createVolume.h>
#include <Disks/DiskObjectStorage/MetadataStorages/IMetadataStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedExchange.h>
#include <Storages/MergeTree/MergeTreeDataPartBuilder.h>
#include <Formats/NativeWriter.h>
#include <IO/HTTPCommon.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <IO/S3Common.h>
#include <Interpreters/Context.h>
#include <Interpreters/MergeTreeTransaction/VersionMetadata.h>
#include <Server/HTTP/HTMLForm.h>
#include <Server/HTTP/HTTPServerResponse.h>
#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/MergedBlockOutputStream.h>
#include <Storages/MergeTree/ReplicatedFetchList.h>
#include <Storages/MergeTree/checkDataPart.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <base/scope_guard.h>
#include <base/sort.h>
#include <boost/algorithm/string/join.hpp>
#include <Poco/Net/HTTPRequest.h>
#include <Common/CurrentMetrics.h>
#include <Common/FailPoint.h>
#include <Common/filesystemHelpers.h>
#include <Common/Jemalloc.h>
#include <Common/JemallocMergeTreeArena.h>
#include <Common/randomDelay.h>
#include <Common/thread_local_rng.h>
#include <Core/UUID.h>

namespace fs = std::filesystem;

namespace CurrentMetrics
{
    extern const Metric ReplicatedSend;
    extern const Metric ReplicatedFetch;
}

namespace DB
{

namespace FailPoints
{
    /// CAS fetch-by-relink, receiver side. Both exist because the two exits they drive are properties of
    /// the sender/receiver PAIR and of the interval between the receiver's publish and its confirm — and
    /// neither is reachable from configuration, so an integration test cannot produce them any other way.
    extern const char cas_relink_receiver_force_mechanism_failure[];
    extern const char cas_relink_receiver_pause_before_confirm[];
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsBool allow_remote_fs_zero_copy_replication;
    extern const MergeTreeSettingsBool enable_the_endpoint_id_with_zookeeper_name_prefix;
    extern const MergeTreeSettingsBool fsync_part_directory;
    extern const MergeTreeSettingsUInt64 min_compressed_bytes_to_fsync_after_fetch;
}

namespace ErrorCodes
{
    extern const int NO_SUCH_DATA_PART;
    extern const int ABORTED;
    extern const int BAD_SIZE_OF_FILE_IN_DATA_PART;
    extern const int CHECKSUM_DOESNT_MATCH;
    extern const int INSECURE_PATH;
    extern const int LOGICAL_ERROR;
    extern const int NETWORK_ERROR;
    extern const int S3_ERROR;
    extern const int ZERO_COPY_REPLICATION_ERROR;
}

namespace DataPartsExchange
{

namespace
{
constexpr auto REPLICATION_PROTOCOL_VERSION_WITH_PARTS_SIZE = 1;
constexpr auto REPLICATION_PROTOCOL_VERSION_WITH_PARTS_SIZE_AND_TTL_INFOS = 2;
constexpr auto REPLICATION_PROTOCOL_VERSION_WITH_PARTS_TYPE = 3;
constexpr auto REPLICATION_PROTOCOL_VERSION_WITH_PARTS_DEFAULT_COMPRESSION = 4;
constexpr auto REPLICATION_PROTOCOL_VERSION_WITH_PARTS_UUID = 5;
constexpr auto REPLICATION_PROTOCOL_VERSION_WITH_PARTS_ZERO_COPY = 6;
constexpr auto REPLICATION_PROTOCOL_VERSION_WITH_PARTS_PROJECTION = 7;
constexpr auto REPLICATION_PROTOCOL_VERSION_WITH_METADATA_VERSION = 8;
constexpr auto REPLICATION_PROTOCOL_VERSION_WITH_COLUMNS_SUBSTREAMS = 9;
/// CAS replication 2b: fetch-by-relink. The receiver advertises its content-addressed pool identity
/// (`cas_pool_uuid`) and, if it matches the sender's own pool, the sender sends only the
/// part's content id (`part_id`) + the mutable header — no file bytes — and the receiver "fetches" by
/// publishing its own ref to the blobs already present in the shared pool (the CA analogue of the
/// zero-copy metadata-only fetch). Everything is gated behind a matching pool_uuid, so a non-CA fetch
/// is byte-for-byte unchanged.
/// Kept although nothing gates on it any more: the offer gate moved to `..._WITH_CA_CONFIRM` below, but
/// 10 is a version peers still advertise, and deleting the record of what it meant would leave the next
/// reader unable to tell what an incoming 10 promises (a relink it will NOT confirm).
[[maybe_unused]] constexpr auto REPLICATION_PROTOCOL_VERSION_WITH_CA_RELINK = 10;
/// CAS replication, publish-then-confirm (spec §wire-protocol). A relink offer is now accompanied by a
/// source token, and the endpoint answers a second, part-less request that asks whether that token is
/// still exactly what the sender's ref names. A server advertising this version serves the confirm
/// action; a receiver advertising it must confirm before it promotes.
constexpr auto REPLICATION_PROTOCOL_VERSION_WITH_CA_CONFIRM = 11;

std::string getEndpointId(const std::string & node_id)
{
    return "DataPartsExchange:" + node_id;
}

/// CAS replication 2b. The receiver advertises its target pool's identity under this request param so
/// the sender can decide whether a fetch-by-relink (same pool) is possible.
constexpr auto CA_POOL_UUID_PARAM = "cas_pool_uuid";
/// Set on the response when the sender chose the relink path; the receiver then reads the relink payload
/// (the opaque encoded PartManifest body — self-contained, see part_manifest_v2 below) instead of the
/// byte stream.
constexpr auto CA_RELINK_COOKIE = "cas_relink";
/// All-tree task 7: the manifest is now self-contained (uuid.txt/metadata_version.txt are ordinary
/// manifest entries, task 6), so the wire payload dropped its trailing metadata_version field (the
/// manifest bytes are now the ONLY field). Bumped from `part_manifest_v1` so a mixed-build pair (old
/// sender, new receiver) does not try to parse the old two-field payload under the new one-field shape
/// — the receiver rejects a cookie value it does not recognize and falls back to a byte fetch instead
/// of desyncing on the wire format.
constexpr auto CA_RELINK_COOKIE_VALUE = "part_manifest_v2";

/// CAS fetch-by-relink, publish-then-confirm (spec §wire-protocol). Three names make up the second
/// request of the handshake.
///
/// The request parameter both selects the confirm action and carries its only argument: the opaque
/// source token the sender minted for the offer. There is no separate action flag, because an action
/// without its token is not a question anyone can answer, and a token without the action would have to
/// be ignored — one name cannot be half-present.
constexpr auto CA_CONFIRM_ACTION_PARAM = "cas_confirm";
/// Response cookie on the relink offer: the token, opaque to the receiver, echoed back verbatim.
constexpr auto CA_CONFIRM_TOKEN_COOKIE = "cas_source_token";
/// Response cookie on the confirm: the answer.
constexpr auto CA_CONFIRM_ANSWER_COOKIE = "cas_confirm_answer";
/// The ONLY value that authorizes the receiver to promote.
constexpr auto CA_CONFIRM_ANSWER_PROVEN = "yes";
/// Everything else: the source did not prove the binding. The wire vocabulary is deliberately BINARY
/// even though `CasConfirmAnswer` has three values. `No` and `Unknown` are one outcome for every caller
/// (see `CasConfirmAnswer`): gate 1 evaluates the mount fence LAST, so a mount that has already lost
/// its fence — and can no longer speak for the namespace at all — still answers `No` for a token that
/// does not match its last-known row. Putting `no` on the wire as a distinct value would invite a
/// receiver to act on it as knowledge, and it is not knowledge. The distinction is diagnostic only, so
/// it is logged on the sender, where the gate that produced it can be named, and never transmitted.
/// An ABSENT cookie reads as unproven too, which is what makes an older peer and a failed request the
/// same safe outcome as a refusal.
constexpr auto CA_CONFIRM_ANSWER_UNPROVEN = "unproven";

/// Resolve a disk to the content-addressed exchange facade, or nullptr if the disk is not CA. The
/// cast targets the purpose-built INTERFACE (IContentAddressedExchange), never the concrete
/// metadata-storage class (M-W design section 4). Used by both the relink sender (the part's
/// disk) and the relink receiver (the target disk).
IContentAddressedExchange * tryGetContentAddressedExchange(const DiskPtr & disk)
{
    if (!disk || !disk->isContentAddressed())
        return nullptr;
    return dynamic_cast<IContentAddressedExchange *>(disk->getMetadataStorage().get());
}

/// Simple functor for tracking fetch progress in system.replicated_fetches table.
struct ReplicatedFetchReadCallback
{
    ReplicatedFetchList::Entry & replicated_fetch_entry;

    explicit ReplicatedFetchReadCallback(ReplicatedFetchList::Entry & replicated_fetch_entry_)
        : replicated_fetch_entry(replicated_fetch_entry_)
    {}

    void operator() (size_t bytes_count)
    {
        replicated_fetch_entry->bytes_read_compressed.store(bytes_count, std::memory_order_relaxed);

        /// It's possible when we fetch part from very old clickhouse version
        /// which doesn't send total size.
        if (replicated_fetch_entry->total_size_bytes_compressed != 0)
        {
            replicated_fetch_entry->progress.store(
                    static_cast<double>(bytes_count) / static_cast<double>(replicated_fetch_entry->total_size_bytes_compressed),
                    std::memory_order_relaxed);
        }
    }
};

/// Validate a projection name from an untrusted replica before it is used to build a path.
/// It becomes a single directory component ("<name>.proj"), so it must be non-empty and contain
/// no '/'. Standalone "." and ".." are safe here ("..proj"/"...proj" are single components).
bool isProjectionNameSafe(const std::string & projection_name)
{
    return !projection_name.empty()
        && projection_name.find('/') == std::string::npos;
}

}


Service::Service(StorageReplicatedMergeTree & data_)
    : data(data_)
    , log(getLogger(data.getStorageID().getNameForLogs() + " (Replicated PartsService)"))
{}

std::string Service::getId(const std::string & node_id) const
{
    return getEndpointId(node_id);
}

CasConfirmAnswer Service::resolveContentAddressedConfirm(
    const String & pool_uuid,
    const String & server_root_id,
    const String & root_namespace,
    const String & ref_name,
    const String & part_name,
    const String & manifest_ref_text) const
{
    /// CAS fetch-by-relink, publish-then-confirm (spec §confirm-primitive). The receiver's own `+1` is
    /// already durable when this runs; a `Yes` is what authorizes it to promote a part whose blobs are
    /// protected only by THIS server's committed binding of that exact manifest. Every field below comes
    /// from a remote peer, so nothing here is trusted beyond being used as a lookup key.
    if (pool_uuid.empty() || server_root_id.empty() || root_namespace.empty() || ref_name.empty() || part_name.empty())
        return CasConfirmAnswer::Unknown;

    /// Routing. A pool UUID identifies the shared pool, not the mount: every server root writing into it
    /// reports the same one, so the namespace's owner decides which instance may answer. EXACTLY one
    /// match is required — zero means this table has no such disk, several mean the question is
    /// ambiguous, and both are `Unknown` rather than a guess.
    const IContentAddressedExchange * matched = nullptr;
    DiskPtr matched_disk;
    for (const auto & disk : data.getDisks())
    {
        const auto * ca_meta = tryGetContentAddressedExchange(disk);
        if (!ca_meta || ca_meta->getPoolUUID() != pool_uuid || !ca_meta->ownsNamespace(server_root_id, root_namespace))
            continue;
        if (matched)
            return CasConfirmAnswer::Unknown;
        matched = ca_meta;
        matched_disk = disk;
    }
    if (!matched)
        return CasConfirmAnswer::Unknown;

    /// Gate 0 — the part-anchored fast filter. It is an AVAILABILITY filter and never a proof (spec
    /// §confirm-primitive, demoted in rev.5): `rollbackDeletingParts` puts a part back to `Outdated`
    /// after a failed filesystem removal, and the in-memory part path is deliberately not updated by a
    /// `delete_tmp_*` rename, so an `Active`/`Outdated` part object authorizes nothing. What it buys is
    /// a cheap `No` that costs no ledger work; every `Yes` is earned by gate 1 alone.
    ///
    /// `Deleting` is excluded by the state filter, an unknown name yields no part at all, and a part of
    /// this name living on ANOTHER disk is rejected explicitly — `MOVE ... TO DISK` leaves a same-name
    /// `Active` part behind on the destination disk, and only the instance the token routed to may be
    /// the one the confirm is about. The parts set is read under its own lock, which
    /// `getPartIfExists` takes and releases, and the part reference is dropped before any ledger lock.
    {
        const auto part_info = MergeTreePartInfo::tryParsePartName(part_name, data.format_version);
        if (!part_info)
            return CasConfirmAnswer::Unknown;
        const auto part = data.getPartIfExists(
            *part_info, {MergeTreeDataPartState::Active, MergeTreeDataPartState::Outdated});
        if (!part || part->getDataPartStorage().getDiskName() != matched_disk->getName())
            return CasConfirmAnswer::No;
    }

    /// Gate 1 — authoritative, and the only source of a `Yes`.
    return matched->confirmExactRef(root_namespace, ref_name, manifest_ref_text);
}

void Service::answerContentAddressedConfirm(const String & token_text, HTTPServerResponse & response) const
{
    /// The confirm action's whole handler. It reads no part parameter, sends no body, and touches no
    /// send metric: the request asks a question about a binding, it does not transfer anything.
    const auto token = decodeCasRelinkSourceToken(token_text);
    if (!token)
    {
        /// The raw text is NOT logged: it is unvalidated peer bytes, and a decoded token is the only
        /// form this server has established is free of the control characters that forge log lines.
        LOG_DEBUG(log, "Relink confirm is unproven: the source token ({} bytes) is not one this server minted",
            token_text.size());
        response.addCookie({CA_CONFIRM_ANSWER_COOKIE, CA_CONFIRM_ANSWER_UNPROVEN});
        return;
    }

    const CasConfirmAnswer answer = resolveContentAddressedConfirm(
        token->pool_uuid, token->server_root_id, token->root_namespace,
        token->ref_name, token->part_name, token->manifest_ref_text);

    /// The `No`/`Unknown` distinction stays here, on the node that computed it and can name the binding
    /// that produced it. It is triage information, not an authorization, and the wire carries only the
    /// authorization (`CA_CONFIRM_ANSWER_UNPROVEN`).
    if (answer != CasConfirmAnswer::Yes)
        LOG_DEBUG(log, "Relink confirm is unproven ({}) for ref '{}' (part {}, manifest {}) in namespace '{}'",
            answer == CasConfirmAnswer::No ? "no" : "unknown",
            token->ref_name, token->part_name, token->manifest_ref_text, token->root_namespace);

    response.addCookie({CA_CONFIRM_ANSWER_COOKIE,
        answer == CasConfirmAnswer::Yes ? CA_CONFIRM_ANSWER_PROVEN : CA_CONFIRM_ANSWER_UNPROVEN});
}

void Service::processQuery(const HTMLForm & params, ReadBufferPtr body, WriteBuffer & out, HTTPServerResponse & response)
{
    /// CAS fetch-by-relink, publish-then-confirm (spec §wire-protocol): the second request of the
    /// handshake, dispatched before `part` is required because a confirm carries none — the part name
    /// is inside the token. Authentication parity with the fetch is inherent: the shared handler
    /// authenticates before it dispatches to any endpoint.
    if (const String confirm_token = params.get(CA_CONFIRM_ACTION_PARAM, ""); !confirm_token.empty())
    {
        answerContentAddressedConfirm(confirm_token, response);
        return;
    }

    // nothing to read from body
    body.reset();

    int client_protocol_version = parse<int>(params.get("client_protocol_version", "0"));

    String part_name = params.get("part");

    const auto data_settings = data.getSettings();

    /// Validation of the input that may come from malicious replica.
    MergeTreePartInfo::fromPartName(part_name, data.format_version);

    /// We pretend to work as older server version, to be sure that client will correctly process our version
    response.addCookie({"server_protocol_version", toString(std::min(client_protocol_version, REPLICATION_PROTOCOL_VERSION_WITH_CA_CONFIRM))});

    LOG_TRACE(log, "Sending part {}", part_name);

    static const auto test_delay = data.getContext()->getConfigRef().getUInt64("test.data_parts_exchange.delay_before_sending_part_ms", 0);
    if (test_delay)
        randomDelayForMaxMilliseconds(test_delay, log, "DataPartsExchange: Before sending part");

    MergeTreeData::DataPartPtr part;

    auto report_broken_part = [&]()
    {
        if (part)
            data.reportBrokenPart(part);
        else
            LOG_TRACE(log, "Part {} was not found, do not report it as broken", part_name);
    };

    try
    {
        part = findPart(part_name);

        CurrentMetrics::Increment metric_increment{CurrentMetrics::ReplicatedSend};

        if (client_protocol_version >= REPLICATION_PROTOCOL_VERSION_WITH_PARTS_SIZE)
            writeBinary(part->checksums.getTotalSizeOnDisk(), out);

        if (client_protocol_version >= REPLICATION_PROTOCOL_VERSION_WITH_PARTS_SIZE_AND_TTL_INFOS)
        {
            WriteBufferFromOwnString ttl_infos_buffer;
            part->ttl_infos.write(ttl_infos_buffer);
            writeBinary(ttl_infos_buffer.str(), out);
        }

        if (client_protocol_version >= REPLICATION_PROTOCOL_VERSION_WITH_PARTS_TYPE)
            writeStringBinary(part->getType().toString(), out);

        if (client_protocol_version >= REPLICATION_PROTOCOL_VERSION_WITH_PARTS_UUID)
            writeUUIDText(part->uuid, out);

        String remote_fs_metadata = parse<String>(params.get("remote_fs_metadata", ""));

        /// Tokenize capabilities from remote_fs_metadata
        /// E.g. remote_fs_metadata = "local, s3_plain, web" --> capabilities = ["local", "s3_plain", "web"]
        Strings capabilities;
        const String delimiter(", ");
        size_t pos_start = 0;
        size_t pos_end = 0;
        while ((pos_end = remote_fs_metadata.find(delimiter, pos_start)) != std::string::npos)
        {
            const String token = remote_fs_metadata.substr(pos_start, pos_end - pos_start);
            pos_start = pos_end + delimiter.size();
            capabilities.push_back(token);
        }
        capabilities.push_back(remote_fs_metadata.substr(pos_start));

        bool send_projections = client_protocol_version >= REPLICATION_PROTOCOL_VERSION_WITH_PARTS_PROJECTION;

        if (send_projections)
        {
            const auto & projections = part->getProjectionParts();
            writeBinary(projections.size(), out);
        }

        /// CAS replication 2b — fetch-by-relink (spec §4). If the part is on a content-addressed disk and
        /// the receiver advertised a `cas_pool_uuid` equal to THIS server's own pool_uuid
        /// (same shared pool), send only the part's content id + the mutable header — no file bytes — so
        /// the receiver can "fetch" by publishing its own ref to the blobs already in the shared pool.
        /// Strictly gated on a matching pool_uuid: a non-CA part, a CA part on a different pool, or a
        /// receiver without the capability all fall through to the unchanged byte path below.
        ///
        /// The gate is `..._WITH_CA_CONFIRM`, not `..._WITH_CA_RELINK`: a receiver is offered a relink
        /// only once it advertises that it will confirm the offer before promoting it. A receiver that
        /// still advertises `..._WITH_CA_RELINK` gets the bytes — mixed versions degrade to bytes, never
        /// to an unconfirmed relink. This gate and the version the client advertises
        /// (`fetchSelectedPart`) are one change in two places; separated in either order they either
        /// disable relink outright or hand an unconfirmed relink to a receiver that claimed it confirms.
        if (client_protocol_version >= REPLICATION_PROTOCOL_VERSION_WITH_CA_CONFIRM
            && part->getDataPartStorage().isContentAddressed())
        {
            const String receiver_pool_uuid = parse<String>(params.get(CA_POOL_UUID_PARAM, ""));
            DiskPtr part_disk = data.getStoragePolicy()->tryGetDiskByName(part->getDataPartStorage().getDiskName());
            auto * ca_meta = tryGetContentAddressedExchange(part_disk);
            if (ca_meta && !receiver_pool_uuid.empty() && receiver_pool_uuid == ca_meta->getPoolUUID())
            {
                auto offer = ca_meta->getRelinkOffer(part->getDataPartStorage().getRelativePath());
                if (offer)
                {
                    LOG_DEBUG(log, "Sending part {} by relink (content-addressed, shared pool {}), manifest payload {} bytes",
                        part_name, receiver_pool_uuid, offer->manifest_bytes.size());
                    response.addCookie({CA_RELINK_COOKIE, CA_RELINK_COOKIE_VALUE});
                    /// The source token for the confirm request the receiver makes before it promotes
                    /// (spec §wire-protocol). It always accompanies the offer, and its ABSENCE is what
                    /// tells a confirm-capable receiver that this sender predates the handshake.
                    response.addCookie({CA_CONFIRM_TOKEN_COOKIE, offer->confirm_token});
                    /// The relink payload (B7 part_manifest_v2, all-tree task 7): the opaque encoded
                    /// PartManifest body (the receiver decodes it, ignores the sender identity, and
                    /// stages its OWN local manifest over the shared-pool blobs; the legacy part_id wire
                    /// field carries it). Self-contained: uuid.txt/metadata_version.txt are ordinary
                    /// manifest entries now (task 6), so no separate mutable-header field is sent.
                    writeStringBinary(offer->manifest_bytes, out);
                    data.addLastSentPart(part->info);
                    return;
                }
                /// No offer (no committed ref for this part here, or no mintable token) — fall through
                /// to the byte path.
            }
        }

        if ((*data_settings)[MergeTreeSetting::allow_remote_fs_zero_copy_replication] &&
            client_protocol_version >= REPLICATION_PROTOCOL_VERSION_WITH_PARTS_ZERO_COPY)
        {
            auto disk_type = part->getDataPartStorage().getDiskType();
            if (part->getDataPartStorage().supportZeroCopyReplication() && std::find(capabilities.begin(), capabilities.end(), disk_type) != capabilities.end())
            {
                /// Send metadata if the receiver's capabilities covers the source disk type.
                response.addCookie({"remote_fs_metadata", disk_type});
                sendPartFromDisk(part, out, client_protocol_version, true, send_projections);
                return;
            }
        }

        sendPartFromDisk(part, out, client_protocol_version, false, send_projections);
        data.addLastSentPart(part->info);
    }
    catch (...)
    {
        if (!isRetryableException(std::current_exception()))
            report_broken_part();
        throw;
    }
}


MergeTreeData::DataPart::Checksums Service::sendPartFromDisk(
    const MergeTreeData::DataPartPtr & part,
    WriteBuffer & out,
    int client_protocol_version,
    bool from_remote_disk,
    bool send_projections)
{
    NameSet files_to_replicate;
    auto file_names_without_checksums = part->getFileNamesWithoutChecksums();

    for (const auto & [name, _] : part->checksums.files)
    {
        if (endsWith(name, ".proj"))
            continue;

        files_to_replicate.insert(name);
    }

    for (const auto & name : file_names_without_checksums)
    {
        if (client_protocol_version < REPLICATION_PROTOCOL_VERSION_WITH_PARTS_DEFAULT_COMPRESSION
            && name == IMergeTreeDataPart::DEFAULT_COMPRESSION_CODEC_FILE_NAME)
            continue;

        if (client_protocol_version < REPLICATION_PROTOCOL_VERSION_WITH_METADATA_VERSION
            && name == IMergeTreeDataPart::METADATA_VERSION_FILE_NAME)
            continue;

        if (client_protocol_version < REPLICATION_PROTOCOL_VERSION_WITH_COLUMNS_SUBSTREAMS
            && name == IMergeTreeDataPart::COLUMNS_SUBSTREAMS_FILE_NAME)
            continue;

        files_to_replicate.insert(name);
    }

    auto data_part_storage = part->getDataPartStoragePtr();
    IDataPartStorage::ReplicatedFilesDescription replicated_description;

    if (from_remote_disk)
    {
        replicated_description = data_part_storage->getReplicatedFilesDescriptionForRemoteDisk(files_to_replicate);
        if (!part->isProjectionPart())
            writeStringBinary(replicated_description.unique_id, out);
    }
    else
    {
        replicated_description = data_part_storage->getReplicatedFilesDescription(files_to_replicate);
    }

    MergeTreeData::DataPart::Checksums data_checksums;
    for (const auto & [name, projection] : part->getProjectionParts())
    {
        if (send_projections)
        {
            writeStringBinary(name, out);
            MergeTreeData::DataPart::Checksums projection_checksum = sendPartFromDisk(projection, out, client_protocol_version, from_remote_disk, false);
            data_checksums.addFile(name + ".proj", projection_checksum.getTotalSizeOnDisk(), projection_checksum.getTotalChecksumUInt128());
        }
        else if (part->checksums.has(name + ".proj"))
        {
            // We don't send this projection, just add out checksum to bypass the following check
            const auto & our_checksum = part->checksums.files.find(name + ".proj")->second;
            data_checksums.addFile(name + ".proj", our_checksum.file_size, our_checksum.file_hash);
        }
    }

    /// Handle unknown projections: .proj entries in part->checksums that are
    /// not in getProjectionParts() (e.g. a projection was dropped while the
    /// part was detached, then re-attached).  Copy their stored checksums to
    /// data_checksums so that checkEqual below does not fail.
    for (const auto & [name, checksum] : part->checksums.files)
    {
        if (name.ends_with(".proj") && !data_checksums.has(name))
            data_checksums.addFile(name, checksum.file_size, checksum.file_hash);
    }

    writeBinary(replicated_description.files.size(), out);
    for (const auto & [file_name, desc] : replicated_description.files)
    {
        writeStringBinary(file_name, out);
        writeBinary(desc.file_size, out);

        auto file_in = desc.input_buffer_getter();
        HashingWriteBuffer hashing_out(out);

        const auto & is_cancelled = blocker.getCounter();
        copyDataWithThrottler(*file_in, hashing_out, is_cancelled, data.getSendsThrottler());

        hashing_out.finalize();

        if (hashing_out.count() != desc.file_size)
            throw Exception(
                ErrorCodes::BAD_SIZE_OF_FILE_IN_DATA_PART,
                "Unexpected size of file {}, expected {} got {}",
                std::string(fs::path(part->getDataPartStorage().getRelativePath()) / file_name),
                desc.file_size, hashing_out.count());

        writePODBinary(hashing_out.getHash(), out);

        if (!file_names_without_checksums.contains(file_name))
            data_checksums.addFile(file_name, hashing_out.count(), hashing_out.getHash());
    }

    if (!from_remote_disk && isFullPartStorage(part->getDataPartStorage()))
        part->checksums.checkEqual(data_checksums, false, part->name);

    return data_checksums;
}

static bool wait_loop(UInt32 wait_timeout_ms, const std::function<bool()> & pred)
{
    static const UInt32 loop_delay_ms = 5;

    /// this is sleep-based wait, it has to be short
    chassert(wait_timeout_ms < 2000);

    if (pred())
        return true;

    Stopwatch timer;
    sleepForMilliseconds(loop_delay_ms);
    while (!pred() && timer.elapsedMilliseconds() < wait_timeout_ms)
    {
        sleepForMilliseconds(loop_delay_ms);
    }

    return pred();
}

MergeTreeData::DataPartPtr Service::findPart(const String & name)
{
    /// It is important to include Outdated parts here because remote replicas cannot reliably
    /// determine the local state of the part, so queries for the parts in these states are completely normal.
    MergeTreeData::DataPartPtr part;

    part = data.getPartIfExists(name, {MergeTreeDataPartState::PreActive, MergeTreeDataPartState::Active, MergeTreeDataPartState::Outdated});

    if (!part)
        throw Exception(ErrorCodes::NO_SUCH_DATA_PART, "No part {} in table", name);

    bool zero_copy_enabled = (*data.getSettings())[MergeTreeSetting::allow_remote_fs_zero_copy_replication];
    if (!zero_copy_enabled)
        return part;

    /// Ephemeral zero-copy lock may be lost for PreActive parts
    /// do not expose PreActive parts for zero-copy

    static const UInt32 wait_timeout_ms = 1000;
    auto pred = [&] ()
    {
        auto lock = data.readLockParts();
        return part->getState() != MergeTreeDataPartState::PreActive;
    };

    bool pred_result = wait_loop(wait_timeout_ms, pred);
    if (!pred_result)
        throw Exception(
                ErrorCodes::ABORTED,
                "Could not exchange part {} as it's in preActive state ({} ms) and it uses zero copy replication. "
                "This is expected behaviour and the client will retry fetching the part automatically.",
                name, wait_timeout_ms);

    return part;
}

Fetcher::Fetcher(StorageReplicatedMergeTree & data_)
    : data(data_)
    , log(getLogger(data.getStorageID().getNameForLogs() + " (Fetcher)"))
{}

std::pair<MergeTreeData::MutableDataPartPtr, scope_guard> Fetcher::fetchSelectedPart(
    const StorageMetadataPtr & metadata_snapshot,
    ContextPtr context,
    const String & part_name,
    const String & zookeeper_name,
    const String & replica_path,
    const String & host,
    int port,
    const ConnectionTimeouts & timeouts,
    const String & user,
    const String & password,
    const String & interserver_scheme,
    ThrottlerPtr throttler,
    bool to_detached,
    const String & tmp_prefix_,
    std::optional<CurrentlySubmergingEmergingTagger> * tagger_ptr,
    bool try_zero_copy,
    DiskPtr disk,
    bool allow_ca_relink)
{
    if (blocker.isCancelled())
        throw Exception(ErrorCodes::ABORTED, "Fetching of part was cancelled");

    const auto data_settings = data.getSettings();

    if (data.canUseZeroCopyReplication() && !try_zero_copy)
        LOG_INFO(log, "Zero copy replication enabled, but trying to fetch part {} without zero copy", part_name);

    /// It should be "tmp-fetch_" and not "tmp_fetch_", because we can fetch part to detached/,
    /// but detached part name prefix should not contain underscore.
    static const String TMP_PREFIX = "tmp-fetch_";
    String tmp_prefix = tmp_prefix_.empty() ? TMP_PREFIX : tmp_prefix_;
    String part_dir = tmp_prefix + part_name;
    auto temporary_directory_lock = data.getTemporaryPartDirectoryHolder(part_dir);

    /// Validation of the input that may come from malicious replica.
    auto part_info = MergeTreePartInfo::fromPartName(part_name, data.format_version);

    String endpoint_id = getEndpointId(
            (*data_settings)[MergeTreeSetting::enable_the_endpoint_id_with_zookeeper_name_prefix] ?
        zookeeper_name + ":" + replica_path :
        replica_path);

    Poco::URI uri;
    uri.setScheme(interserver_scheme);
    uri.setHost(host);
    uri.setPort(static_cast<uint16_t>(port));
    uri.setQueryParameters(
    {
        {"endpoint",                endpoint_id},
        {"part",                    part_name},
        /// Advertising `..._WITH_CA_CONFIRM` is a PROMISE, not a capability list: this receiver will
        /// confirm a relink offer against its source before it promotes (`relinkPartToDisk`). It is the
        /// pair of the sender-side offer gate on the same constant, and the two cannot be separated —
        /// see the comment there.
        {"client_protocol_version", toString(REPLICATION_PROTOCOL_VERSION_WITH_CA_CONFIRM)},
        {"compress",                "false"}
    });

    if (disk)
        LOG_TRACE(log, "Will fetch to disk {} with type {}", disk->getName(), disk->getDataSourceDescription().toString());

    /// CAS replication 2b — fetch-by-relink (spec §4). Advertise this replica's target content-addressed
    /// pool identity so a same-pool sender can relink instead of streaming bytes. The target disk is the
    /// provided one if it is CA, else the first CA disk among the table's disks. A non-CA fetch adds
    /// nothing here and is byte-for-byte unchanged.
    /// Gated on `allow_ca_relink` alone (B66b). That flag is the RECURSION BRAKE and nothing else: not
    /// advertising is what makes the sender stream bytes, so every same-sender byte re-request below
    /// clears it, and a persistent relink-mechanism failure therefore costs exactly one relink attempt.
    /// The gate used to be `try_zero_copy && !to_detached`, and BOTH halves were accidents of that same
    /// brake — `try_zero_copy` because the fallback re-requests with it false, and `!to_detached`
    /// because the relink path staged at the ACTIVE part path and ignored `to_detached`. `to_detached`
    /// is now a parameter of `relinkPartToDisk` (it stages under the `detached/` parent), and
    /// `try_zero_copy` goes back to meaning real zero-copy only.
    String advertised_pool_uuid;
    if (allow_ca_relink)
    {
        if (auto * ca_meta = tryGetContentAddressedExchange(disk))
        {
            advertised_pool_uuid = ca_meta->getPoolUUID();
            uri.addQueryParameter(CA_POOL_UUID_PARAM, advertised_pool_uuid);
        }
        else if (!disk)
        {
            for (const auto & data_disk : data.getDisks())
            {
                if (auto * ca_disk_meta = tryGetContentAddressedExchange(data_disk))
                {
                    advertised_pool_uuid = ca_disk_meta->getPoolUUID();
                    uri.addQueryParameter(CA_POOL_UUID_PARAM, advertised_pool_uuid);
                    break;
                }
            }
        }
    }

    Strings capability;
    if (try_zero_copy && (*data_settings)[MergeTreeSetting::allow_remote_fs_zero_copy_replication])
    {
        if (!disk)
        {
            LOG_TRACE(log, "Trying to fetch with zero-copy replication, but disk is not provided, will try to select");
            Disks disks = data.getDisks();
            for (const auto & data_disk : disks)
            {
                LOG_TRACE(log, "Checking disk {} with type {}", data_disk->getName(), data_disk->getDataSourceDescription().toString());
                if (data_disk->supportZeroCopyReplication())
                {
                    LOG_TRACE(log, "Disk {} (with type {}) supports zero-copy replication", data_disk->getName(), data_disk->getDataSourceDescription().toString());
                    capability.push_back(data_disk->getDataSourceDescription().name());
                }
            }
        }
        else if (disk->supportZeroCopyReplication())
        {
            LOG_TRACE(log, "Trying to fetch with zero copy replication, provided disk {} with type {}", disk->getName(), disk->getDataSourceDescription().toString());
            capability.push_back(disk->getDataSourceDescription().name());
        }
    }

    if (!capability.empty())
    {
        ::sort(capability.begin(), capability.end());
        capability.erase(std::unique(capability.begin(), capability.end()), capability.end());
        const String & remote_fs_metadata = boost::algorithm::join(capability, ", ");
        uri.addQueryParameter("remote_fs_metadata", remote_fs_metadata);
    }
    else
    {
        if (data.canUseZeroCopyReplication())
            LOG_INFO(log, "Cannot select any zero-copy disk for {}", part_name);

        try_zero_copy = false;
    }

    Poco::Net::HTTPBasicCredentials creds{};
    if (!user.empty())
    {
        creds.setUsername(user);
        creds.setPassword(password);
    }

    ReadSettings read_settings = context->getReadSettings();
    /// Disable retries for fetches, this will be done by the engine itself.
    read_settings.http_settings.max_tries = 1;

    auto in = BuilderRWBufferFromHTTP(uri)
                  .withConnectionGroup(HTTPConnectionGroupType::HTTP)
                  .withBypassProxy(true)
                  .withMethod(Poco::Net::HTTPRequest::HTTP_POST)
                  .withTimeouts(timeouts)
                  .withSettings(read_settings)
                  .withDelayInit(false)
                  .create(creds);

    int server_protocol_version = parse<int>(in->getResponseCookie("server_protocol_version", "0"));
    String remote_fs_metadata = parse<String>(in->getResponseCookie("remote_fs_metadata", ""));

    DiskPtr preffered_disk = disk;

    if (!preffered_disk)
    {
        for (const auto & disk_candidate : data.getDisks())
        {
            if (disk_candidate->getDataSourceDescription().name() == remote_fs_metadata)
            {
                preffered_disk = disk_candidate;
                break;
            }
        }
    }

    ReservationPtr reservation;
    size_t sum_files_size = 0;
    if (server_protocol_version >= REPLICATION_PROTOCOL_VERSION_WITH_PARTS_SIZE)
    {
        readBinary(sum_files_size, *in);

        if (server_protocol_version >= REPLICATION_PROTOCOL_VERSION_WITH_PARTS_SIZE_AND_TTL_INFOS)
        {
            IMergeTreeDataPart::TTLInfos ttl_infos;
            String ttl_infos_string;
            readBinary(ttl_infos_string, *in);

            ReadBufferFromString ttl_infos_buffer(ttl_infos_string);
            assertString("ttl format version: 1\n", ttl_infos_buffer);
            ttl_infos.read(ttl_infos_buffer);

            if (!disk)
            {
                LOG_TEST(log, "Disk for fetch is not provided, reserving space using storage balanced reservation");
                reservation
                    = data.balancedReservation(metadata_snapshot, sum_files_size, 0, part_name, part_info, {}, tagger_ptr, &ttl_infos, true);

                if (!reservation)
                {
                    LOG_TEST(log, "Disk for fetch is not provided, reserving space using TTL rules");
                    reservation
                        = data.reserveSpacePreferringTTLRules(metadata_snapshot, sum_files_size, ttl_infos, std::time(nullptr), 0, true, preffered_disk);
                }
            }
        }
        else if (!disk)
        {
            LOG_TEST(log, "Making balanced reservation");
            reservation = data.balancedReservation(metadata_snapshot, sum_files_size, 0, part_name, part_info, {}, tagger_ptr, nullptr);
            if (!reservation)
            {
                LOG_TEST(log, "Making simple reservation");
                reservation = data.reserveSpace(sum_files_size);
            }
        }
    }
    else if (!disk)
    {
        LOG_TEST(log, "Making reservation on the largest disk");
        /// We don't know real size of part because sender server version is too old
        reservation = data.makeEmptyReservationOnLargestDisk();
    }

    if (!disk)
    {
        disk = reservation->getDisk();
        LOG_TRACE(log, "Disk for fetch is not provided, getting disk from reservation {} with type '{}'", disk->getName(), disk->getDataSourceDescription().toString());
    }
    else
    {
        LOG_TEST(log, "Disk for fetch is disk {} with type {}", disk->getName(), disk->getDataSourceDescription().toString());
    }

    bool sync = ((*data_settings)[MergeTreeSetting::min_compressed_bytes_to_fsync_after_fetch]
                    && sum_files_size >= (*data_settings)[MergeTreeSetting::min_compressed_bytes_to_fsync_after_fetch]);

    using PartType = MergeTreeDataPartType;
    PartType part_type = PartType::Wide;
    if (server_protocol_version >= REPLICATION_PROTOCOL_VERSION_WITH_PARTS_TYPE)
    {
        String part_type_str;
        readStringBinary(part_type_str, *in);
        part_type.fromString(part_type_str);
    }

    UUID part_uuid = UUIDHelpers::Nil;
    if (server_protocol_version >= REPLICATION_PROTOCOL_VERSION_WITH_PARTS_UUID)
        readUUIDText(part_uuid, *in);

    size_t projections = 0;
    if (server_protocol_version >= REPLICATION_PROTOCOL_VERSION_WITH_PARTS_PROJECTION)
        readBinary(projections, *in);

    /// CAS replication 2b — fetch-by-relink (spec §4; B7 part_manifest_v2, all-tree task 7). The sender
    /// chose to relink: it sent only the part's encoded PartManifest body, no file bytes. Build the part
    /// by staging this server's OWN local manifest over the blobs already in the shared pool (adopt-by-hash
    /// -> revalidate -> promote inside adoptPartFromManifest) — self-contained since task 6 routed
    /// uuid.txt/metadata_version.txt through the content path, so there is no separate mutable header to
    /// reconstruct. If the relink is not possible (blob missing/condemned — a transient or a
    /// genuinely-different pool the cheap pre-filter let through, or a mixed-build pair offering an
    /// unrecognized cookie value), fall back to a normal byte fetch by re-requesting WITHOUT relink.
    String ca_relink = parse<String>(in->getResponseCookie(CA_RELINK_COOKIE, ""));
    if (!ca_relink.empty())
    {
        /// Re-request without the relink capability: pass the SAME (CA) disk but disable zero-copy/relink
        /// so the sender streams bytes; on CA the downloaded files content-address and dedup.
        ///
        /// THE RECURSION BRAKE (B66b). `allow_ca_relink=false` is what bounds this: the re-request does
        /// not advertise the pool identity, so the sender cannot offer relink again, so this lambda
        /// cannot be reached a second time for the same fetch. Before relink had its own capability the
        /// brake was implicit in `try_zero_copy=false`; with the two decoupled it has to be spelled out,
        /// and it must be spelled out at EVERY same-sender fallback — a relink failure that is a
        /// property of the pair reproduces on every attempt, so without the brake the fallback re-offers
        /// and recurses without bound. The failures it actually bounds are the ones that leave the CA
        /// disk resolved and matching: a mixed build offering an unrecognized cookie value, a sender that
        /// predates the confirm handshake, an undecodable manifest, a local ref conflict. (The
        /// reservation-outside-the-pool exit below is bounded twice over — it re-requests with the
        /// non-CA disk it resolved, which cannot advertise anything either way — so do not read that one
        /// as evidence that the brake is redundant.)
        auto fall_back_to_byte_fetch = [&]
        {
            temporary_directory_lock = {};
            return fetchSelectedPart(
                metadata_snapshot, context, part_name, zookeeper_name, replica_path, host, port, timeouts,
                user, password, interserver_scheme, throttler, to_detached, tmp_prefix, nullptr, false, disk,
                /*allow_ca_relink=*/ false);
        };

        if (ca_relink != CA_RELINK_COOKIE_VALUE)
        {
            /// Mixed-build cluster (rolling upgrade): this receiver build does not recognize the sender's
            /// relink wire format. Bail out before reading anything else off the stream rather than
            /// misparsing an incompatible payload shape.
            LOG_INFO(log, "Part {} was offered by relink with cookie '{}' (this build expects '{}'); "
                "falling back to a byte fetch", part_name, ca_relink, CA_RELINK_COOKIE_VALUE);
            return fall_back_to_byte_fetch();
        }

        auto * chosen_ca = tryGetContentAddressedExchange(disk);
        if (!chosen_ca || chosen_ca->getPoolUUID() != advertised_pool_uuid)
        {
            LOG_INFO(log, "Part {} was offered by relink for content-addressed pool '{}', but reservation landed "
                "outside the advertised pool on disk {} (chosen pool: '{}'); falling back to a byte fetch",
                part_name, advertised_pool_uuid, disk->getName(), chosen_ca ? chosen_ca->getPoolUUID() : "<none>");
            return fall_back_to_byte_fetch();
        }

        String sender_manifest_bytes;
        readStringBinary(sender_manifest_bytes, *in);
        assertEOF(*in);

        /// Publish-then-confirm (spec §core-idea) happens inside `relinkPartToDisk`, including the second
        /// interserver request; the token cookie is the sender's offer identity and is opaque here. A
        /// `nullptr` means the mechanism cannot work but the sender still has the part, so the byte
        /// re-request below is sound; a THROW means the source did not prove the binding, and the whole
        /// point of it being a throw is that this fallback must NOT run for it.
        auto relinked = relinkPartToDisk(part_name, tmp_prefix, disk, to_detached, sender_manifest_bytes,
            in->getResponseCookie(CA_CONFIRM_TOKEN_COOKIE, ""), uri, creds, timeouts, read_settings);
        if (relinked)
            return std::make_pair(std::move(relinked), std::move(temporary_directory_lock));

        LOG_INFO(log, "Relink of part {} is not possible on this pair; falling back to a byte fetch", part_name);
        return fall_back_to_byte_fetch();
    }

    if (!remote_fs_metadata.empty())
    {
        if (!try_zero_copy)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Got unexpected 'remote_fs_metadata' cookie");
        if (std::find(capability.begin(), capability.end(), remote_fs_metadata) == capability.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Got 'remote_fs_metadata' cookie {}, expect one from {}",
                            remote_fs_metadata, fmt::join(capability, ", "));
        if (server_protocol_version < REPLICATION_PROTOCOL_VERSION_WITH_PARTS_ZERO_COPY)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Got 'remote_fs_metadata' cookie with old protocol version {}", server_protocol_version);

        try
        {
            auto output_buffer_getter = [](IDataPartStorage & part_storage, const auto & file_name, size_t file_size)
            {
                auto full_path = fs::path(part_storage.getFullPath()) / file_name;
                return std::make_unique<WriteBufferFromFile>(full_path, std::min<UInt64>(DBMS_DEFAULT_BUFFER_SIZE, file_size));
            };

            return std::make_pair(downloadPartToDisk(part_name, replica_path, to_detached, tmp_prefix, disk, true, *in, output_buffer_getter, projections, throttler, sync), std::move(temporary_directory_lock));
        }
        catch (const Exception & e)
        {
            if (e.code() != ErrorCodes::S3_ERROR && e.code() != ErrorCodes::ZERO_COPY_REPLICATION_ERROR)
                throw;

#if USE_AWS_S3
            if (const auto * s3_exception = dynamic_cast<const S3Exception *>(&e))
            {
                /// It doesn't make sense to retry Access Denied or No Such Key
                if (!s3_exception->isRetryableError())
                {
                    tryLogCurrentException(log, fmt::format("while fetching part: {}", part_name));
                    throw;
                }
            }
#endif

            LOG_WARNING(log, "Will retry fetching part without zero-copy: {}", e.message());

            temporary_directory_lock = {};

            /// Try again but without zero-copy. `allow_ca_relink=false` for the same reason as the relink
            /// branch's fallback above: this is a same-sender byte re-request, and it must not re-open a
            /// capability the failed attempt is not evidence about. It also preserves the behaviour this
            /// call had while relink rode on `try_zero_copy` — the flag it already passes as false.
            return fetchSelectedPart(
                metadata_snapshot,
                context,
                part_name,
                zookeeper_name,
                replica_path,
                host,
                port,
                timeouts,
                user, password, interserver_scheme, throttler, to_detached, tmp_prefix, nullptr, false, disk,
                /*allow_ca_relink=*/ false);
        }
    }

    auto storage_id = data.getStorageID();
    String new_part_path = fs::path(data.getFullPathOnDisk(disk)) / part_name / "";
    auto entry = data.getContext()->getReplicatedFetchList().insert(
        storage_id.getDatabaseName(), storage_id.getTableName(),
        part_info.getPartitionId(), part_name, new_part_path,
        replica_path, uri, to_detached, sum_files_size);

    in->setNextCallback(ReplicatedFetchReadCallback(*entry));

    auto output_buffer_getter = [](IDataPartStorage & part_storage, const String & file_name, size_t file_size)
    {
        return part_storage.writeFile(file_name, std::min<UInt64>(file_size, DBMS_DEFAULT_BUFFER_SIZE), {});
    };

    return std::make_pair(downloadPartToDisk(
        part_name, replica_path, to_detached, tmp_prefix,
        disk, false, *in, output_buffer_getter,
        projections, throttler, sync),std::move(temporary_directory_lock));
}


void Fetcher::downloadBaseOrProjectionPartToDisk(
    const String & replica_path,
    const MutableDataPartStoragePtr & data_part_storage,
    ReadWriteBufferFromHTTP & in,
    OutputBufferGetter output_buffer_getter,
    MergeTreeData::DataPart::Checksums & checksums,
    ThrottlerPtr throttler,
    bool sync) const
{
    size_t files = 0;
    readBinary(files, in);
    LOG_DEBUG(log, "Downloading files {}", files);


    std::vector<std::unique_ptr<WriteBufferFromFileBase>> written_files;

    for (size_t i = 0; i < files; ++i)
    {
        String file_name;
        UInt64 file_size = 0;

        readStringBinary(file_name, in);
        readBinary(file_size, in);

        /// Guard against a malicious replica writing outside the part directory.
        /// Runs for both the base part and projection parts.
        const auto absolute_file_path = fs::weakly_canonical(fs::path(data_part_storage->getRelativePath()) / file_name);
        if (!pathStartsWith(absolute_file_path, fs::path(data_part_storage->getRelativePath())))
            throw Exception(ErrorCodes::INSECURE_PATH,
                "File path ({}) doesn't appear to be inside part path ({}). "
                "This may happen if we are trying to download part from malicious replica or logical error.",
                absolute_file_path.string(),
                data_part_storage->getRelativePath());

        written_files.emplace_back(output_buffer_getter(*data_part_storage, file_name, file_size));
        HashingWriteBuffer hashing_out(*written_files.back());
        copyDataWithThrottler(in, hashing_out, file_size, blocker.getCounter(), throttler);
        hashing_out.finalize();

        if (blocker.isCancelled())
        {
            /// NOTE The is_cancelled flag also makes sense to check every time you read over the network,
            /// performing a poll with a not very large timeout.
            /// And now we check it only between read chunks (in the `copyData` function).
            throw Exception(ErrorCodes::ABORTED, "Fetching of part was cancelled");
        }

        MergeTreeDataPartChecksum::uint128 expected_hash;
        readPODBinary(expected_hash, in);

        if (expected_hash != hashing_out.getHash())
            throw Exception(ErrorCodes::CHECKSUM_DOESNT_MATCH,
                "Checksum mismatch for file {} transferred from {} (0x{} vs 0x{})",
                (fs::path(data_part_storage->getFullPath()) / file_name).string(),
                replica_path,
                getHexUIntLowercase(expected_hash),
                getHexUIntLowercase(hashing_out.getHash()));

        if (file_name != "checksums.txt" &&
            file_name != "columns.txt" &&
            file_name != IMergeTreeDataPart::COLUMNS_SUBSTREAMS_FILE_NAME &&
            file_name != IMergeTreeDataPart::DEFAULT_COMPRESSION_CODEC_FILE_NAME &&
            file_name != IMergeTreeDataPart::METADATA_VERSION_FILE_NAME)
            checksums.addFile(file_name, file_size, expected_hash);
    }

    /// Call fsync for all files at once in attempt to decrease the latency
    for (auto & file : written_files)
    {
        file->finalize();
        if (sync)
            file->sync();
    }
}

MergeTreeData::MutableDataPartPtr Fetcher::downloadPartToDisk(
    const String & part_name,
    const String & replica_path,
    bool to_detached,
    const String & tmp_prefix,
    DiskPtr disk,
    bool to_remote_disk,
    ReadWriteBufferFromHTTP & in,
    OutputBufferGetter output_buffer_getter,
    size_t projections,
    ThrottlerPtr throttler,
    bool sync)
{
    String part_id;
    const auto data_settings = data.getSettings();
    MergeTreeData::DataPart::Checksums data_checksums;

    zkutil::EphemeralNodeHolderPtr zero_copy_temporary_lock_holder;
    if (to_remote_disk)
    {
        readStringBinary(part_id, in);

        if (!disk->supportZeroCopyReplication() || !disk->checkUniqueId(part_id))
            throw Exception(ErrorCodes::ZERO_COPY_REPLICATION_ERROR, "Part {} unique id {} doesn't exist on {} (with type {}).", part_name, part_id, disk->getName(), disk->getDataSourceDescription().toString());

        LOG_DEBUG(log, "Downloading part {} unique id {} metadata onto disk {}.", part_name, part_id, disk->getName());
        zero_copy_temporary_lock_holder = data.lockSharedDataTemporary(part_name, part_id, disk);
    }
    else
    {
        LOG_DEBUG(log, "Downloading part {} onto disk {}.", part_name, disk->getName());
    }

    /// We will remove directory if it's already exists. Make precautions.
    if (tmp_prefix.empty()
        || part_name.empty()
        || std::string::npos != tmp_prefix.find_first_of("/.")
        || std::string::npos != part_name.find_first_of("/."))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "`tmp_prefix` and `part_name` cannot be empty or contain '.' or '/' characters.");

    auto part_dir = tmp_prefix + part_name;
    auto part_relative_path = data.getRelativeDataPath() + String(to_detached ? MergeTreeData::DETACHED_DIR_NAME : "");

    /// Same rationale as `MergeTreeData::loadDataPart`: the `SingleDiskVolume` and the
    /// `DataPartStorageOnDiskFull` below are stored on the resulting part and live for its
    /// lifetime. Only this two-line scope is wrapped — the actual fetch I/O buffers below
    /// are short-lived and stay in the default arena.
    auto [volume, part_storage_for_loading] = [&]
    {
        ScopedJemallocThreadArena mergetree_arena_scope(JemallocMergeTreeArena::getArenaIndex());
        auto v = std::make_shared<SingleDiskVolume>("volume_" + part_name, disk);
        auto s = std::make_shared<DataPartStorageOnDiskFull>(v, part_relative_path, part_dir);
        return std::pair{std::move(v), std::move(s)};
    }();

    part_storage_for_loading->beginTransaction();

    if (part_storage_for_loading->exists())
    {
        LOG_WARNING(log, "Directory {} already exists, probably result of a failed fetch. Will remove it before fetching part.",
            part_storage_for_loading->getFullPath());

        /// Even if it's a temporary part it could be downloaded with zero copy replication and this function
        /// is executed as a callback.
        ///
        /// We don't control the amount of refs for temporary parts so we cannot decide can we remove blobs
        /// or not. So we are not doing it
        bool keep_shared = part_storage_for_loading->supportZeroCopyReplication() && (*data_settings)[MergeTreeSetting::allow_remote_fs_zero_copy_replication];
        part_storage_for_loading->removeSharedRecursive(keep_shared);
    }

    part_storage_for_loading->createDirectories();

    SyncGuardPtr sync_guard;
    if ((*data.getSettings())[MergeTreeSetting::fsync_part_directory])
        sync_guard = part_storage_for_loading->getDirectorySyncGuard();

    CurrentMetrics::Increment metric_increment{CurrentMetrics::ReplicatedFetch};

    try
    {
        for (size_t i = 0; i < projections; ++i)
        {
            String projection_name;
            readStringBinary(projection_name, in);

            /// Validate before getProjection()/createDirectories() so no attacker-named
            /// directory is ever created from an untrusted replica's projection name.
            if (!isProjectionNameSafe(projection_name))
                throw Exception(ErrorCodes::INSECURE_PATH,
                    "Projection name ({}) doesn't appear to be a valid name. "
                    "This may happen if we are trying to download part from malicious replica or logical error.",
                    projection_name);

            MergeTreeData::DataPart::Checksums projection_checksum;

            auto projection_part_storage = part_storage_for_loading->getProjection(projection_name + ".proj");
            projection_part_storage->createDirectories();

            downloadBaseOrProjectionPartToDisk(
                replica_path, projection_part_storage, in, output_buffer_getter, projection_checksum, throttler, sync);

            data_checksums.addFile(
                projection_name + ".proj", projection_checksum.getTotalSizeOnDisk(), projection_checksum.getTotalChecksumUInt128());
        }

        downloadBaseOrProjectionPartToDisk(
            replica_path, part_storage_for_loading, in, output_buffer_getter, data_checksums, throttler, sync);
    }
    catch (const Exception & e)
    {
        /// Remove the whole part directory if fetch of base
        /// part or fetch of any projection was stopped.
        if (e.code() == ErrorCodes::ABORTED)
        {
            part_storage_for_loading->removeSharedRecursive(true);
            part_storage_for_loading->commitTransaction();
        }
        throw;
    }

    assertEOF(in);
    MergeTreeData::MutableDataPartPtr new_data_part;
    try
    {
        part_storage_for_loading->commitTransaction();

        MergeTreeDataPartBuilder builder(data, part_name, volume, part_relative_path, part_dir, getReadSettings());
        new_data_part = builder.withPartFormatFromDisk().build();

        new_data_part->version->setAndStoreCreationTID(Tx::NonTransactionalTID, nullptr);
        new_data_part->is_temp = true;
        /// In case of replicated merge tree with zero copy replication
        /// Here Clickhouse claims that this new part can be deleted in temporary state without unlocking the blobs
        /// The blobs have to stay intact, this temporary part does not own them and does not share them yet.
        new_data_part->remove_tmp_policy = IMergeTreeDataPart::BlobsRemovalPolicyForTemporaryParts::PRESERVE_BLOBS;
        new_data_part->modification_time = time(nullptr);
        new_data_part->loadColumnsChecksumsIndexes(true, false);
    }
#if USE_AWS_S3
    catch (const S3Exception & ex)
    {
        if (ex.getS3ErrorCode() == Aws::S3::S3Errors::NO_SUCH_KEY)
        {
            throw Exception(ErrorCodes::S3_ERROR, "Cannot fetch part {} because we lost lock and it was concurrently removed", part_name);
        }
        throw;
    }
#endif
    catch (...) /// Redundant catch, just to be able to add first one with #if
    {
        throw;
    }

    if (to_remote_disk)
    {
        LOG_DEBUG(log, "Download of part {} unique id {} metadata onto disk {} finished.", part_name, part_id, disk->getName());
    }
    else
    {
        if (isFullPartStorage(new_data_part->getDataPartStorage()))
        {
            /// Handle unknown projections on the fetch side: .proj entries in
            /// checksums.txt that were not transferred (e.g. a projection was
            /// dropped while the part was detached, then re-attached on the
            /// sender).  Copy them to data_checksums so that checkEqual does
            /// not fail.
            for (const auto & [name, checksum] : new_data_part->checksums.files)
            {
                if (name.ends_with(".proj") && !data_checksums.has(name))
                    data_checksums.addFile(name, checksum.file_size, checksum.file_hash);
            }
            new_data_part->checksums.checkEqual(data_checksums, false, new_data_part->name);
        }
        LOG_DEBUG(log, "Download of part {} onto disk {} finished.", part_name, disk->getName());
    }

    if (zero_copy_temporary_lock_holder)
        zero_copy_temporary_lock_holder->setAlreadyRemoved();

    return new_data_part;
}

/// The receiver's half of publish-then-confirm, and its complete failure taxonomy (spec
/// §failure-taxonomy). Every exit of `relinkPartToDisk` is one of these seven rows; the last two columns
/// are the questions a reviewer has to be able to answer without reading the control flow, because a
/// part-exchange path that gets them wrong either loses a part or commits it twice.
///
/// 1. THE SOURCE SENT NO TOKEN (a peer that predates the handshake).
///    `+1`: never staged. Action: return `nullptr`, the caller byte-fetches from the same sender.
///    Lose a part? No -- the sender still has it and streams it.
///    Double-promote? No -- nothing was staged, so there is nothing to promote.
///
/// 2. `prepareAdoptFromManifest` -> `MechanismFallbackAllowed` (manifest decode failure, or the
///    retryable staging class: body-absent precommit / precommit no longer the live owner / ref
///    conflict).
///    `+1`: NOTHING IS PUBLISHED, and that -- not "never staged" -- is what makes the byte fallback
///    sound here. A precommit whose ref-log append came back `Unresolved` may in fact be durable, so
///    `prepareEntries`' own `abandon` queues the exact removal for it (`PartWriteTxn::precommitAdd`
///    records the intent BEFORE the append precisely so that removal is never skipped) and leaves the
///    manifest body for GC rather than deleting it. A precommit is not a committed ref: a later byte
///    fetch publishes the same ref name over it without conflict, and a removal that could not be
///    appended at all leaks retained blobs -- it never double-publishes.
///    Action: return `nullptr`, the caller byte-fetches. Lose a part? No, as row 1.
///    Double-promote? No -- no handle exists, and nothing was committed.
///
/// 3. THE CONFIRM DID NOT PROVE THE SOURCE: an `unproven` answer, an absent answer cookie, a transport
///    failure, a timeout. All one outcome, deliberately (`CasConfirmAnswer`: only `yes` authorizes).
///    `+1`: durable, then released by `abort`. Action: THROW a locally generated retry-later
///    `NETWORK_ERROR` naming the source and the part -- never `nullptr`, because a byte re-request goes
///    back to the very source whose state is in doubt.
///    Lose a part? No -- the queue stores the exception, backs off, and re-executes the entry, which
///    recomputes the source and the covering-part discovery. The fetch is postponed, not dropped.
///    Double-promote? No -- `abort` appends the exact precommit removal and no committed ref exists.
///
/// 4. CONFIRM `yes`, `promote` -> `Committed`.
///    `+1`: committed. Action: return the relinked part; the usual `tmp-fetch_<part>` re-key follows.
///    Lose a part? No. Double-promote? No -- `promote` is the handle's single terminal operation, the
///    handle is released immediately after it, and a second call is rejected rather than re-driving a
///    finished transaction.
///
/// 5. CONFIRM `yes`, `promote` -> `MechanismFallbackAllowed` (a local ref conflict; the source proved
///    its side, this receiver could not commit its own). The promote was rejected BEFORE its ref-log
///    append, so "nothing was committed" is proven, not assumed -- see row 5b for the case where it is
///    not.
///    `+1`: released -- a failed `promote` abandons its build on the way out.
///    Action: return `nullptr`, the caller byte-fetches. Lose a part? No, as row 1.
///    Double-promote? No -- the byte fetch starts from a clean slate.
///
/// 5b. CONFIRM `yes`, `promote` -> `Unresolved` (the promotion's ref-log append was attempted and came
///    back without a verdict; the receiver's ref MAY be committed).
///    `+1`: still owed -- the handle attempts its abandon, which is REJECTED by the state machine if
///    the promote in fact landed (a promoted binding is no longer a precommit), so no committed ref is
///    ever undone here.
///    Action: THROW the retry-later `NETWORK_ERROR`, as row 3 -- returning `nullptr` is the one thing
///    that must not happen, because a byte fetch would publish the part a SECOND time over a relink
///    that may already be committed.
///    Lose a part? No -- retry-later, as row 3. Double-promote? No -- nothing is published on this exit.
///
/// 6. ANY OTHER EXCEPTION (an unclassified local error, or a `promote` failure outside the known
///    retryable class).
///    `+1`: durable if one was staged, then released by the scope guard. Action: propagate.
///    Lose a part? No -- retry-later, exactly as row 3. Double-promote? No -- the scope guard runs
///    `abort` before the exception leaves the function, and the handle's own destructor is the backstop
///    if that abort's append fails.
///
/// The asymmetry between rows 2/5 and row 3 is the entire point of the typed boundary. A byte
/// re-request goes back to the SAME sender, so it is a sound recovery exactly when the doubt is about
/// the MECHANISM and the sender is known to still hold the part -- and never when the doubt is about
/// the source itself. `adoptPartFromManifest` used to collapse the two by catching every `Exception`
/// and returning `false`.
///
/// B66b — WHAT CHANGES WHEN THE TARGET IS `detached/`. Every row above still holds, and the two columns
/// that matter are unchanged in every one of them, but two rows hold for a DIFFERENT reason and that
/// difference is worth stating rather than rediscovering:
///
/// - Row 3 (and row 6, which recovers the same way) argues "no part is lost" from the replication queue:
///   it stores the exception, backs off, and re-executes the entry. Two of the three detached callers
///   have no queue entry -- `FETCH PARTITION`/`FETCH PART ... FROM` are user DDL -- so the retry-later
///   error surfaces to the user, who re-issues the statement. Nothing is lost either way, and for a
///   stronger reason than in the active case: a detached fetch is not replication, so no replicated
///   state was ever expecting the part. (The third, `executeClonePartFromShard`, IS a queue entry and
///   recovers exactly as the active path does.)
/// - Row 4's "no double-promote" is about the relink's own terminal operation and is unaffected. What
///   the CALLER then does with the part differs: `renameTo(detached/<part>, true)` rather than
///   `renameTempPartAndReplace`. Both are ref repoints within one namespace on a content-addressed disk
///   (`detached/` is a ref-name prefix, not a namespace), and the detached one keeps its existing
///   collision behaviour -- an existing `detached/<part>` is displaced. That is the pre-existing
///   semantic of a detached BYTE fetch, deliberately left alone: relink must not change what a fetch
///   into `detached/` means, only how the bytes get there.
///
/// The staged ref itself is `detached/tmp-fetch_<part>` rather than `tmp-fetch_<part>`, which is what
/// keeps a failed detached relink from ever being visible as a live part: the abandoned precommit and
/// the abandoned staging directory both live in the detached ref space.
///
/// What a `yes` does NOT prove: `CaRelinkConfirmCore.tla` config `_sab_holeylist` shows that with every
/// confirm rule intact and one incomplete listing page permitted, `ConfirmedRelinkNeverDangles` still
/// breaks (BACKLOG `{#list-as-journal-dataloss-2026-07-25}`). A confirmed relink is therefore NOT proven
/// dangle-free; a `yes` means only "the source still holds exactly this manifest right now", which is
/// what closes the codex-6 handoff window and nothing more.
MergeTreeData::MutableDataPartPtr Fetcher::relinkPartToDisk(
    const String & part_name,
    const String & tmp_prefix,
    DiskPtr disk,
    bool to_detached,
    const String & sender_manifest_bytes,
    const String & source_token,
    const Poco::URI & fetch_uri,
    const Poco::Net::HTTPBasicCredentials & credentials,
    const ConnectionTimeouts & timeouts,
    const ReadSettings & read_settings)
{
    auto * ca_meta = tryGetContentAddressedExchange(disk);
    if (!ca_meta)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "relinkPartToDisk called for a non-content-addressed disk {}", disk->getName());

    if (tmp_prefix.empty()
        || part_name.empty()
        || std::string::npos != tmp_prefix.find_first_of("/.")
        || std::string::npos != part_name.find_first_of("/."))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "`tmp_prefix` and `part_name` cannot be empty or contain '.' or '/' characters.");

    /// Taxonomy row 1 — the capability gate, and it comes FIRST so a pre-confirm sender costs nothing.
    /// An absent token is how such a sender identifies itself: it offers relink exactly as before and
    /// simply has no token cookie to attach. There is no version number to consult here and that is
    /// deliberate — the sender's advertised version says what it can serve, while the token's presence
    /// says what it actually did for THIS offer, and only the latter can be confirmed. A relink that
    /// cannot be confirmed is never promoted, so the bytes are fetched instead.
    if (source_token.empty())
    {
        LOG_INFO(log, "Part {} was offered by relink without a source token, so the offer cannot be confirmed "
            "(the sender predates the publish-then-confirm handshake); falling back to a byte fetch", part_name);
        return nullptr;
    }

    /// Test-only. Forces the "mechanism failed, the sender still has the part" exit (the ACTION of
    /// taxonomy rows 2 and 5) on EVERY attempt, which is precisely the shape the recursion brake has to
    /// bound: a persistent property of this sender/receiver pair, so the byte re-request re-offers and
    /// re-fails unless it clears `allow_ca_relink`. It fires AFTER the token gate and BEFORE
    /// `prepareAdoptFromManifest`, so nothing is staged and no `+1` has to be released — the failpoint
    /// injects the exit, never a half-finished transaction.
    fiu_do_on(FailPoints::cas_relink_receiver_force_mechanism_failure,
    {
        LOG_INFO(log, "Failpoint cas_relink_receiver_force_mechanism_failure: abandoning the relink of part {} "
            "before anything is staged", part_name);
        return nullptr;
    });

    /// Stage under the tmp-fetch dir OF THE TARGET PARENT — the table dir, or `TABLE/detached` when
    /// the caller asked for a detached fetch (B66b). The parent is composed exactly as
    /// `downloadPartToDisk` composes it, so the two fetch paths put a part in the same place and the
    /// caller's finalization is unchanged: `renameTempPartAndReplace`'s moveDirectory(tmp-fetch_<part>
    /// -> <part>) for the active path, `renameTo(detached/<part>)` for the detached one. Both are ref
    /// repoints within one namespace on a content-addressed disk (`detached/` is a ref-name prefix, not
    /// a namespace), so a relinked part re-keys exactly as a byte-fetched one does.
    ///
    /// The ref name is NOT built here: the disk-relative path is handed to the CA exchange whole and its
    /// router folds `TABLE/detached/DIR` onto the `detached/DIR` ref, the same routing every other
    /// read and write of a detached part goes through. This side has no business knowing that prefix,
    /// and the sender's half of the offer (`getRelinkOffer`) is already addressed by path too.
    const String part_dir = tmp_prefix + part_name;
    const String part_relative_path
        = data.getRelativeDataPath() + String(to_detached ? MergeTreeData::DETACHED_DIR_NAME : "");
    const String part_path = fs::path(part_relative_path) / part_dir;

    LOG_DEBUG(log, "Relinking part {} (staged as {}) onto content-addressed disk {} from a {}-byte transferred manifest.",
        part_name, part_path, disk->getName(), sender_manifest_bytes.size());

    /// T1 — PUBLISH. Adopt-from-manifest and precommit, stopping short of the promote (B7
    /// part_manifest_v2, all-tree task 7): the receiver decodes the transferred body and stages its OWN
    /// local manifest over the shared-pool blobs (adopt-by-hash). Self-contained:
    /// uuid.txt/metadata_version.txt are ordinary entries in the transferred manifest (task 6), so there
    /// is no sidecar to reconstruct. Trust boundary is the interserver channel, as for a normal part
    /// fetch — see `prepareAdoptFromManifest`.
    ///
    /// The order is the whole protocol. This `+1` must be DURABLE before the source is asked anything,
    /// because the question "do you still hold it?" only excludes a later removal if the receiver's own
    /// reference is already in the ref log when that removal is appended (spec §correctness). Asking
    /// first and publishing after would prove nothing about the interval in between. What it does NOT
    /// establish is that every subsequent GC fold OBSERVES that reference -- see "What a `yes` does NOT
    /// prove" above; ordering is necessary here, not sufficient.
    std::unique_ptr<ICaPreparedRelink> prepared;
    if (ca_meta->prepareAdoptFromManifest(part_path, sender_manifest_bytes, prepared)
        == CaRelinkPrepare::MechanismFallbackAllowed)
        return nullptr;                                     /// taxonomy row 2
    if (!prepared)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Relink of part {} reported a prepared write but produced no handle", part_name);

    /// Belt-and-braces over the handle's own destructor: the durable `+1` is released on EVERY exit that
    /// is not a completed promote, including an exception. `abort` is the non-throwing form by contract —
    /// this runs while the retry-later error of row 3 is already in flight.
    SCOPE_EXIT({
        if (prepared)
            prepared->abort();
    });

    /// Test-only, and this is the ONE seam worth injecting on the whole path: it opens the window the
    /// protocol exists to make safe. The receiver's `+1` is durable and its release is armed, and the
    /// source has not been asked anything yet, so a test that holds the fetch here can do to the source
    /// exactly what codex-6 described — merge the part away, run GC to fixpoint — and then observe both
    /// halves of the contract: the source's blobs survive the round (this receiver's binding protects
    /// them) and the confirm that follows refuses to authorize a promote (the binding it named is gone).
    FailPointInjection::pauseFailPoint(FailPoints::cas_relink_receiver_pause_before_confirm);

    /// T2 — CONFIRM. One read-only interserver question, aimed at the endpoint copied out of the fetch
    /// URI so it reaches exactly the table and replica that made the offer. Only the literal
    /// `CA_CONFIRM_ANSWER_PROVEN` cookie authorizes a promote: an `unproven` answer, an absent cookie
    /// (any peer that does not implement the action) and a failed request are ONE outcome, and it is not
    /// knowledge about the source — see `CasConfirmAnswer` on why `no` is never put on the wire.
    bool source_proved_the_binding = false;
    try
    {
        Poco::URI confirm_uri;
        confirm_uri.setScheme(fetch_uri.getScheme());
        confirm_uri.setHost(fetch_uri.getHost());
        confirm_uri.setPort(fetch_uri.getPort());
        Poco::URI::QueryParameters confirm_params;
        for (const auto & fetch_param : fetch_uri.getQueryParameters())
            if (fetch_param.first == "endpoint")
                confirm_params.push_back(fetch_param);
        confirm_params.emplace_back(CA_CONFIRM_ACTION_PARAM, source_token);
        confirm_params.emplace_back("compress", "false");
        confirm_uri.setQueryParameters(confirm_params);

        /// `read_settings` is the caller's, which already caps HTTP retries at one: the queue owns the
        /// retry policy for a fetch, and a silently retried confirm would widen the window it measures.
        auto confirm_in = BuilderRWBufferFromHTTP(confirm_uri)
                              .withConnectionGroup(HTTPConnectionGroupType::HTTP)
                              .withBypassProxy(true)
                              .withMethod(Poco::Net::HTTPRequest::HTTP_POST)
                              .withTimeouts(timeouts)
                              .withSettings(read_settings)
                              .withDelayInit(false)
                              .create(credentials);
        /// The confirm answer is a cookie and the response body is empty by construction. Requiring EOF
        /// before reading the answer means a response carrying anything at all — a misrouted reply, a
        /// desynchronized peer — is unproven rather than half-parsed.
        assertEOF(*confirm_in);
        source_proved_the_binding
            = confirm_in->getResponseCookie(CA_CONFIRM_ANSWER_COOKIE, "") == CA_CONFIRM_ANSWER_PROVEN;
    }
    catch (...)
    {
        /// Not a fallback: a confirm that could not be delivered is the same "not proven" as a refusal,
        /// and it takes the same path out. Logged rather than propagated so the error the caller sees is
        /// the one that names the relink — but logged in full, because the reason (refused, timed out,
        /// 500) exists nowhere else. `information`, not `error`: a peer restarting mid-fetch is ordinary,
        /// and the throw below is what makes the failure loud.
        tryLogCurrentException(log, fmt::format("while confirming the relink offer for part {} with {}",
            part_name, fetch_uri.getHost()), LogsLevel::information);
        source_proved_the_binding = false;
    }

    if (!source_proved_the_binding)
    {
        /// Taxonomy row 3. Locally generated on purpose — nothing here is the source's error to report —
        /// and thrown rather than returned, because the one recovery that is NOT sound after this is a
        /// byte re-request to the same source. `NETWORK_ERROR` puts it in the retry-later class, so the
        /// queue stores it, backs off, and re-selects on re-execution.
        throw Exception(ErrorCodes::NETWORK_ERROR,
            "Source {} did not prove it still holds the manifest it offered for part {} by relink; "
            "the relink is abandoned and the fetch will be retried later",
            fetch_uri.getHost(), part_name);
    }

    /// T3 — PROMOTE. Only now, and only because the source proved the binding at T2 > T1.
    switch (prepared->promote())
    {
        case CaRelinkPromote::Committed:
            break;
        case CaRelinkPromote::MechanismFallbackAllowed:
            return nullptr;                                 /// taxonomy row 5
        case CaRelinkPromote::Unresolved:
            /// The promotion append may have landed, so this is the ONE promote outcome that is not row
            /// 5: returning `nullptr` would send the caller to fetch the bytes and publish the part a
            /// second time over a relink that may already be committed. Thrown in the retry-later class
            /// instead, exactly as an unproven confirm is (row 3) -- the queue stores it, backs off, and
            /// re-executes, by which time the ref lane has resolved the ambiguity one way or the other.
            throw Exception(ErrorCodes::NETWORK_ERROR,
                "Relink of part {} from {} could not be resolved: the promotion may or may not have "
                "committed, so the bytes must NOT be fetched; the fetch will be retried later",
                part_name, fetch_uri.getHost());
    }
    /// The single terminal operation is done, so the handle owes nothing; releasing it here also disarms
    /// the scope guard for the part-building code below.
    prepared.reset();

    auto volume = std::make_shared<SingleDiskVolume>("volume_" + part_name, disk);

    MergeTreeData::MutableDataPartPtr new_data_part;
    MergeTreeDataPartBuilder builder(data, part_name, volume, part_relative_path, part_dir, getReadSettings());
    /// Read the part format from the now-published manifest (type + storage type), exactly as the byte
    /// fetch does — authoritative over the transferred `part_type` header (kept for protocol symmetry).
    new_data_part = builder.withPartFormatFromDisk().build();

    new_data_part->version->setAndStoreCreationTID(Tx::NonTransactionalTID, nullptr);
    new_data_part->is_temp = true;
    /// The blobs are shared in the pool; a discarded temporary relink part must NOT reclaim them (another
    /// replica's ref keeps them alive). Same policy a zero-copy-fetched temporary part uses.
    new_data_part->remove_tmp_policy = IMergeTreeDataPart::BlobsRemovalPolicyForTemporaryParts::PRESERVE_BLOBS;
    new_data_part->modification_time = time(nullptr);
    new_data_part->loadColumnsChecksumsIndexes(true, false);

    LOG_DEBUG(log, "Relink of part {} onto disk {} finished (no bytes transferred).", part_name, disk->getName());
    return new_data_part;
}

}

}
