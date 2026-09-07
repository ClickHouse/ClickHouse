#pragma once
#include <array>
#include <cstdint>
namespace ProfileEvents::PagedExperiment
{
/// Resolved from the actual constant-initialized named events in `ProfileEvents.cpp`.
std::array<uint16_t, 10> requiredHotEvents() noexcept;
}
