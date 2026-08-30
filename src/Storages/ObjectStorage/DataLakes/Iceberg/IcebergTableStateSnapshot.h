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

    void serialize(WriteBuffer & out) const;

    static TableStateSnapshot deserialize(ReadBuffer & in, int datalake_state_protocol_version);

    bool operator==(const TableStateSnapshot & other) const;
};

using TableStateSnapshotPtr = std::shared_ptr<TableStateSnapshot>;
}
}
