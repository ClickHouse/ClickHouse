#pragma once

#include <Core/NamesAndTypes.h>
#include <Parsers/IAST_fwd.h>

#include <span>
#include <vector>

namespace DB
{

class ASTCreateQuery;
class IDatabase;
struct StorageID;
struct StorageInMemoryMetadata;

namespace UDT
{

/// One exact current-name presentation of a persisted logical occurrence.
/// declared_type replays nested logical TYPE actuals, while callers may still
/// expose the descriptor's canonical physical arguments separately.
struct CurrentDeclaredTableTypeOccurrence
{
    std::vector<UInt64> type_child_ordinals;
    UInt64 occurrence_ordinal = 0;
    UInt32 descriptor_index = 0;
    ASTPtr declared_type;
};

/// One current-name presentation of an immutable, UUID-bound physical column.
/// The physical type remains the storage/runtime authority; declared_type is
/// an introspection-only AST rebuilt from the bound occurrence index.
struct CurrentDeclaredTableColumnType
{
    String column_name;
    DataTypePtr physical_type;
    ASTPtr declared_type;
    bool has_logical_references = false;
    std::vector<CurrentDeclaredTableTypeOccurrence> logical_occurrences;
};

/// Returns an empty vector for a physical-only table. For a mapped table this
/// validates the complete bound metadata package and resolves every retained
/// definition identity against one current database-authority snapshot before
/// returning one entry per physical column in schema order.
[[nodiscard]] std::vector<CurrentDeclaredTableColumnType> projectCurrentDeclaredTableColumnTypes(
    const StorageID & table_id,
    const StorageInMemoryMetadata & metadata,
    IDatabase & database);

/// The selective form is intended for access-filtered metadata surfaces. The
/// mask is indexed by physical-column ordinal. Hidden columns are not resolved
/// against the UDT authority. Callers request the comparatively expensive
/// per-occurrence presentations only when their output surface needs them.
[[nodiscard]] std::vector<CurrentDeclaredTableColumnType> projectCurrentDeclaredTableColumnTypes(
    const StorageID & table_id,
    const StorageInMemoryMetadata & metadata,
    IDatabase & database,
    std::span<const UInt8> included_physical_columns,
    bool include_occurrence_presentations);

/// Validates that a fetched physical CREATE AST is the exact schema projected
/// above, then replaces only reference-bearing column type ASTs. This prevents
/// SHOW CREATE from combining an old CREATE snapshot with newer bound metadata.
void applyCurrentDeclaredTableColumnTypes(
    ASTCreateQuery & create,
    const StorageID & table_id,
    const std::vector<CurrentDeclaredTableColumnType> & columns);

/// Kind-aware SHOW CREATE projection for mapped View/MV and Dictionary
/// metadata. It validates the exact persisted endpoint inventory and renders
/// current names by UUID-bound descriptors; runtime metadata remains physical.
void applyCurrentDeclaredStoredObjectTypes(
    ASTCreateQuery & create, const StorageID & object_id, const StorageInMemoryMetadata & metadata, IDatabase & database);
}
}
