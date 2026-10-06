#pragma once

#include <DataTypes/IDataType.h>
#include <Interpreters/StorageID.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

struct TimeSeriesIDGenerator
{
    /// Builds the default id-generator expression for a given `id_type`
    /// (e.g. `reinterpretAsUUID(sipHash128(tags))` for `UUID`).
    /// Throws if there is no default expression for the type; `table_id` is used in the error message.
    static ASTPtr getDefault(const DataTypePtr & id_type, const StorageID & table_id);

    /// The same as `getDefault`, but returns null if there is no default expression for the type.
    static ASTPtr tryGetDefault(const DataTypePtr & id_type);

    /// Returns true if `id_generator` references the `all_tags` identifier
    /// (so that column needs to be populated at INSERT time).
    static bool usesAllTags(const ASTPtr & id_generator);
};

}
