#pragma once

#include <Storages/StorageSnapshot.h>

namespace DB
{

namespace Iceberg
{

struct TableStateSnapshot
{
    String metadata_file_path;
    Int32 metadata_version{};
    Int32 schema_id{};
    std::optional<Int64> snapshot_id;

    /// The `TrustedTableUuid` incarnation in effect when this state was pinned, used to decide
    /// whether `metadata_file_path` may still be reopened through the metadata content cache.
    /// See `TrustedTableUuid::getForPinnedIncarnation`.
    ///
    /// Deliberately neither serialized nor compared: it identifies an incarnation counted by one
    /// server's own cell and is meaningless on another one, which therefore reads the pinned file
    /// from storage rather than trusting a number it cannot interpret.
    std::optional<UInt64> trusted_uuid_incarnation;

    /// A fingerprint of the pinned metadata file's own content, taken when this state was pinned,
    /// so that a later reopen of `metadata_file_path` can tell whether the file is still the one
    /// that was analysed. A metadata file is immutable, so a change of content means another
    /// table took the path over - the only replacement token a format-version 1 table that omits
    /// `table-uuid` has. See `PersistentTableComponents::checkMetadataMatchesPinnedState`.
    ///
    /// Deliberately neither serialized nor compared, like `trusted_uuid_incarnation`: a server
    /// that receives this state reads the pinned file itself and pins its own token.
    std::optional<UInt64> metadata_content_token;

    void serialize(WriteBuffer & out) const;

    static TableStateSnapshot deserialize(ReadBuffer & in, int datalake_state_protocol_version);

    bool operator==(const TableStateSnapshot & other) const;
};

using TableStateSnapshotPtr = std::shared_ptr<TableStateSnapshot>;
}
}
