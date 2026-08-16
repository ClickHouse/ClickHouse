#pragma once

#include <Common/SipHash.h>

#include <cstdint>
#include <optional>

namespace DB
{

/// Which table engine backs a materialized CTE temporary table.
enum class MaterializedCTEEngineKind : uint8_t
{
    Memory,
    Set,
};

/// Engine requested for a materialized CTE (std::nullopt = default Memory).
struct MaterializedCTEEngine
{
    MaterializedCTEEngineKind kind = MaterializedCTEEngineKind::Memory;

    bool operator==(const MaterializedCTEEngine &) const = default;
};

inline void updateHash(SipHash & hash, const std::optional<MaterializedCTEEngine> & engine)
{
    hash.update(engine.has_value());
    if (engine)
        hash.update(static_cast<uint8_t>(engine->kind));
}

}
