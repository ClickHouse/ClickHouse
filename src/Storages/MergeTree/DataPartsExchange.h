#pragma once

#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Interpreters/InterserverIOHandler.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/IStorage_fwd.h>
#include <IO/HashingWriteBuffer.h>
#include <IO/copyData.h>
#include <IO/ConnectionTimeouts.h>
#include <Common/Throttler.h>
#include <Common/ActionBlocker.h>
#include <IO/ReadBuffer.h>


namespace zkutil
{
    class ZooKeeper;
    using ZooKeeperPtr = std::shared_ptr<ZooKeeper>;
}

/// Only the content-addressed relink's confirm request needs these, and only as parameter types, so
/// they are declared rather than included — this header is pulled in by the whole replication tree.
namespace Poco { class URI; }
namespace Poco::Net { class HTTPBasicCredentials; }

namespace DB
{

class StorageReplicatedMergeTree;
class ReadWriteBufferFromHTTP;
struct ReadSettings;

/// Declared by `ContentAddressedExchange.h` (the narrow content-addressed seam). Opaque-enum-declared
/// here so this header stays free of content-addressed includes; the definition must keep the same
/// underlying type.
enum class CasConfirmAnswer : uint8_t;

namespace DataPartsExchange
{

/** Service for sending parts from the table *ReplicatedMergeTree.
  */
class Service final : public InterserverIOEndpoint
{
public:
    explicit Service(StorageReplicatedMergeTree & data_);

    Service(const Service &) = delete;
    Service & operator=(const Service &) = delete;

    std::string getId(const std::string & node_id) const override;
    void processQuery(const HTMLForm & params, ReadBufferPtr body, WriteBuffer & out, HTTPServerResponse & response) override;

private:
    /// CAS fetch-by-relink, publish-then-confirm: answer one relink confirm token — "is `manifest_ref_text`
    /// still exactly what `ref_name` names here?" — for a receiver that has already made its own `+1`
    /// durable and may promote only on `Yes`. Everything content-addressed is behind
    /// `IContentAddressedExchange`; what has to live here is what only the storage can see: which of this
    /// table's disks is entitled to answer (`ownsNamespace` under a matching pool UUID, exactly one match
    /// or `Unknown`), and gate 0, the part-anchored filter over this table's parts set. Never throws, and
    /// `No` is not knowledge — see `CasConfirmAnswer`.
    /// The confirm action's handler: decode the peer's token, resolve it, and set the answer cookie.
    /// Exactly two answers cross the wire — proven, and not proven — because only `Yes` authorizes
    /// anything and `No` is not knowledge (see `CasConfirmAnswer`). Never throws: an unparsable token
    /// is one more unproven answer, not an error the receiver would have to classify.
    void answerContentAddressedConfirm(const String & token_text, HTTPServerResponse & response) const;

    CasConfirmAnswer resolveContentAddressedConfirm(
        const String & pool_uuid,
        const String & server_root_id,
        const String & root_namespace,
        const String & ref_name,
        const String & part_name,
        const String & manifest_ref_text) const;

    MergeTreeData::DataPartPtr findPart(const String & name);

    MergeTreeData::DataPart::Checksums sendPartFromDisk(
        const MergeTreeData::DataPartPtr & part,
        WriteBuffer & out,
        int client_protocol_version,
        bool from_remote_disk,
        bool send_projections);

    /// StorageReplicatedMergeTree::shutdown() waits for all parts exchange handlers to finish,
    /// so Service will never access dangling reference to storage
    StorageReplicatedMergeTree & data;
    LoggerPtr log;
};

/** Client for getting the parts from the table *MergeTree.
  */
class Fetcher final : private boost::noncopyable
{
public:
    explicit Fetcher(StorageReplicatedMergeTree & data_);

    /// Downloads a part to tmp_directory. If to_detached - downloads to the `detached` directory.
    std::pair<MergeTreeData::MutableDataPartPtr, scope_guard> fetchSelectedPart(
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
        bool to_detached = false,
        const String & tmp_prefix_ = "",
        std::optional<CurrentlySubmergingEmergingTagger> * tagger_ptr = nullptr,
        bool try_zero_copy = true,
        DiskPtr dest_disk = nullptr,
        /// CAS fetch-by-relink (spec §B66b): may this request advertise its content-addressed pool
        /// identity, i.e. may the sender answer with a relink offer instead of the part's bytes?
        ///
        /// It is a capability of its own rather than a rider on `try_zero_copy`, and it carries the
        /// RECURSION BRAKE. Relink used to be gated on `try_zero_copy` purely because the byte-fetch
        /// fallback re-requests with `try_zero_copy=false`, so the brake came for free; with the two
        /// decoupled, every same-sender byte re-request must clear THIS flag explicitly or a
        /// persistent relink-mechanism failure re-offers, re-fails and re-requests without bound.
        ///
        /// It defaults to `true`, and that default is what makes a manual `FETCH PARTITION`/`FETCH
        /// PART` (which passes `try_fetch_shared=false`, so `try_zero_copy` is already false) relink,
        /// into `detached/` as well as into the active part path.
        bool allow_ca_relink = true);

    /// You need to stop the data transfer.
    ActionBlocker blocker;

private:
    using OutputBufferGetter = std::function<std::unique_ptr<WriteBufferFromFileBase>(IDataPartStorage &, const String &, size_t)>;

    void downloadBaseOrProjectionPartToDisk(
        const String & replica_path,
        const MutableDataPartStoragePtr & data_part_storage,
        ReadWriteBufferFromHTTP & in,
        OutputBufferGetter output_buffer_getter,
        MergeTreeData::DataPart::Checksums & checksums,
        ThrottlerPtr throttler,
        bool sync) const;

    MergeTreeData::MutableDataPartPtr downloadPartToDisk(
        const String & part_name,
        const String & replica_path,
        bool to_detached,
        const String & tmp_prefix_,
        DiskPtr disk,
        bool to_remote_disk,
        ReadWriteBufferFromHTTP & in,
        OutputBufferGetter output_buffer_getter,
        size_t projections,
        ThrottlerPtr throttler,
        bool sync);

    /// CAS replication 2b — fetch-by-relink (spec §4), publish-then-confirm (spec §core-idea). Build a
    /// part WITHOUT downloading any bytes by publishing this server's own ref to the blobs already in the
    /// shared content-addressed pool. Stages the ref under the tmp-fetch dir of the target parent — the
    /// table dir, or `detached/` when `to_detached` (B66b) — so the caller's finalization re-keys it to
    /// the final part name, exactly as for a byte-fetched part: `renameTempPartAndReplace` for the
    /// active path, `renameTo(detached/<part>)` for the detached one. Then it ASKS THE SOURCE whether it
    /// still holds exactly the manifest it offered, and only then promotes and loads the part.
    /// Self-contained (all-tree task 7): the transferred manifest alone is enough to rebuild the part —
    /// no separate uuid/metadata_version wire fields to reconstruct as a sidecar.
    ///
    /// The whole failure taxonomy lives at the definition; the two outcomes a CALLER must distinguish:
    /// `nullptr` means relink cannot work here and the source still has the part, so a byte re-request to
    /// the SAME source is sound; a THROW means the source could not prove it still holds the manifest,
    /// and the one recovery that is not sound is asking that same source for the bytes.
    ///
    /// `source_token`, `fetch_uri` and the connection parameters are what the confirm request is built
    /// from: the token is the sender's opaque offer identity, and the request is aimed at the endpoint
    /// COPIED out of the fetch URI so it cannot reach a different table or replica than the offer did.
    MergeTreeData::MutableDataPartPtr relinkPartToDisk(
        const String & part_name,
        const String & tmp_prefix,
        DiskPtr disk,
        bool to_detached,
        const String & sender_manifest_bytes,
        const String & source_token,
        const Poco::URI & fetch_uri,
        const Poco::Net::HTTPBasicCredentials & credentials,
        const ConnectionTimeouts & timeouts,
        const ReadSettings & read_settings);

    MergeTreeData::MutableDataPartPtr downloadPartToDiskRemoteMeta(
       const String & part_name,
       const String & replica_path,
       bool to_detached,
       const String & tmp_prefix_,
       DiskPtr disk,
       ReadWriteBufferFromHTTP & in,
       size_t projections,
       MergeTreeData::DataPart::Checksums & checksums,
       ThrottlerPtr throttler);

    StorageReplicatedMergeTree & data;
    LoggerPtr log;
};

}

}
