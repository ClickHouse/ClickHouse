#pragma once

#include <Databases/UDT/AuthorityRoot.h>

#include <Core/UUID.h>

#include <optional>
#include <string_view>

namespace DB::UDT
{

/// Read-only image captured by DatabaseAtomic while both databases' schema
/// mutation mutexes are held. Keeping this policy independent from the disk
/// backend lets feature-inert tests exercise table expectations before
/// production table artifacts are registered by that backend.
struct AtomicCrossDatabaseAuthorityView
{
    UUID database_uuid = UUIDHelpers::Nil;
    const AuthorityRoot * published_root = nullptr;
    bool durable_storage_present = false;
    std::optional<AuthorityState> durable_state;
    std::optional<UInt64> recovery_required_transaction_id;
    bool durable_authority_marker = false;
};

/// Returns whether `table_uuid` is owned by the database's UDT authority.
/// Inconsistent or recovery-required authority views fail closed.
bool hasDatabaseOwnedTableExpectationForCrossDatabaseMove(const AtomicCrossDatabaseAuthorityView & view, UUID table_uuid);

void assertTableCanLeaveAtomicDatabase(bool has_database_owned_table, std::string_view table_name_for_logs);

void assertTableCanEnterAtomicDatabase(
    bool has_database_owned_expectation, std::string_view table_name_for_logs, std::string_view target_database_name);

}
