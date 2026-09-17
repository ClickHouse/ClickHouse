#pragma once

#include <Core/Types.h>

#include <span>
#include <string_view>

namespace DB::UDT
{

enum class StoredObjectTableFunctionSourceProvenance : UInt8
{
    Unclassified,
    PhysicalInference,
    ExactLogicalAuthorityRequired,
};

struct StoredObjectTableFunctionSourceContract
{
    std::string_view function_name;
    StoredObjectTableFunctionSourceProvenance provenance{};
    bool case_insensitive = false;
};

/// Closed inventory of table-function schema provenance. Absence is
/// Unclassified, never a physical-only proof. Any newly registered function
/// must be classified here before UDT-enabled inferred CREATE may use it.
[[nodiscard]] std::span<const StoredObjectTableFunctionSourceContract> getStoredObjectTableFunctionSourceContracts() noexcept;

[[nodiscard]] const StoredObjectTableFunctionSourceContract *
tryGetStoredObjectTableFunctionSourceContract(std::string_view function_name) noexcept;

[[nodiscard]] StoredObjectTableFunctionSourceProvenance classifyStoredObjectTableFunctionSource(std::string_view function_name) noexcept;

[[nodiscard]] bool storedObjectTableFunctionRequiresExactLogicalAuthority(std::string_view function_name) noexcept;

}
