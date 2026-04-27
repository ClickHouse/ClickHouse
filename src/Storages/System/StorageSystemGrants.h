#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{
class AccessRightsElements;
enum class AccessEntityType : uint8_t;
class Context;

/// Implements `grants` system table, which allows you to get information about grants.
class StorageSystemGrants final : public IStorageSystemOneBlock
{
public:
    std::string getName() const override { return "SystemGrants"; }
    static ColumnsDescription getColumnsDescription();

    /// Append `system.grants`-shaped rows describing `elements` of the given grantee
    /// to `res_columns`. Shared by `system.grants` and `EXPLAIN GRANT` / `EXPLAIN REVOKE`
    /// to keep the row layout in a single source.
    static void emitGranteeRows(
        MutableColumns & res_columns,
        const String & grantee_name,
        AccessEntityType grantee_type,
        const AccessRightsElements & elements,
        bool is_enabled_read_write_grants);

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const override;
};

}
