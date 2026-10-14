#pragma once

#include <DataTypes/IDataType.h>
#include <Interpreters/StorageID.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{
struct TimeSeriesSettings;

struct TimeSeriesIDGenerator
{
    /// Builds the default id-generator expression for a given `id_type`
    /// (e.g. `reinterpretAsUUID(sipHash128(...))` for `UUID`).
    static ASTPtr getDefault(
        const DataTypePtr & id_type, const TimeSeriesSettings & settings, const StorageID & table_id);

    /// Returns true if `id_generator` references the `all_tags` identifier
    /// (so that column needs to be populated at INSERT time).
    static bool usesAllTags(const ASTPtr & id_generator);
};

}
