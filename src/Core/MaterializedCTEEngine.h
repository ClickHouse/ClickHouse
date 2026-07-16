#pragma once

#include <Core/Joins.h>
#include <Core/Names.h>
#include <Common/SipHash.h>

#include <cstdint>
#include <optional>

namespace DB
{

/// Parameters of the `Join` engine chosen for a materialized CTE:
///   WITH t AS MATERIALIZED ENGINE = Join(<strictness>, <kind>, key1, ...) (subquery)
/// The strictness is stored in its surface form (Any/All/Semi/Anti) exactly as written; the
/// setting-dependent interpretation (e.g. Any vs RightAny via `any_join_distinct_right_table_keys`)
/// is left to `StorageFactory` when the temporary table is created.
struct MaterializedJoinEngineParams
{
    JoinStrictness strictness = JoinStrictness::Unspecified;
    JoinKind kind = JoinKind::Comma;
    Names key_columns;

    bool operator==(const MaterializedJoinEngineParams &) const = default;
};

/// Which table engine backs a materialized CTE temporary table.
enum class MaterializedCTEEngineKind : uint8_t
{
    Memory,
    Set,
    Join,
};

/// The engine (and, for Join, its parameters) requested for a materialized CTE. A missing engine
/// clause is represented by the absence of this value (std::nullopt), which means the default Memory
/// engine.
struct MaterializedCTEEngine
{
    MaterializedCTEEngineKind kind = MaterializedCTEEngineKind::Memory;
    std::optional<MaterializedJoinEngineParams> join_params; /// set iff kind == Join

    bool operator==(const MaterializedCTEEngine &) const = default;
};

/// Mix a materialized CTE engine descriptor into a query-tree hash.
inline void updateHash(SipHash & hash, const std::optional<MaterializedCTEEngine> & engine)
{
    hash.update(engine.has_value());
    if (!engine)
        return;

    hash.update(static_cast<uint8_t>(engine->kind));
    hash.update(engine->join_params.has_value());
    if (const auto & join_params = engine->join_params)
    {
        hash.update(static_cast<uint8_t>(join_params->strictness));
        hash.update(static_cast<uint8_t>(join_params->kind));
        hash.update(join_params->key_columns.size());
        for (const auto & key : join_params->key_columns)
            hash.update(key);
    }
}

}
