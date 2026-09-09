#pragma once
#include <span>
#include <cstdint>
namespace ProfileEvents::PagedExperiment
{
/// Resolved from the actual constant-initialized named events in `ProfileEvents.cpp`.
std::span<const uint16_t> requiredHotEvents() noexcept;
}
