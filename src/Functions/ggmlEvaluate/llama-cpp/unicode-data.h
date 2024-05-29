#pragma once

#include <cstdint>
#include <unordered_map>
#include <unordered_set>
#include <vector>

struct range_nfd  // NOLINT
{
    uint32_t first;
    uint32_t last;
    uint32_t nfd;
};

static const uint32_t MAX_CODEPOINTS = 0x110000;

extern const std::vector<std::pair<uint32_t, uint16_t>> unicode_ranges_flags;
extern const std::unordered_set<uint32_t> unicode_set_whitespace;
extern const std::unordered_map<uint32_t, uint32_t> unicode_map_lowercase;
extern const std::unordered_map<uint32_t, uint32_t> unicode_map_uppercase;
extern const std::vector<range_nfd> unicode_ranges_nfd;
