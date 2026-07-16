#pragma once

#include <Core/Joins.h>
#include <Core/Names.h>
#include <Common/SipHash.h>

#include <cstdint>
#include <optional>

namespace DB
{

/// Join engine parameters of a materialized CTE. Strictness is the surface form (Any/All/Semi/Anti).
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

/// Engine requested for a materialized CTE (std::nullopt = default Memory).
struct MaterializedCTEEngine
{
    MaterializedCTEEngineKind kind = MaterializedCTEEngineKind::Memory;
    std::optional<MaterializedJoinEngineParams> join_params; /// set iff kind == Join

    bool operator==(const MaterializedCTEEngine &) const = default;
};

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
