#pragma once

#include <cstdint>

/// Marks that extra information is sent to a shard. It could be any magic numbers.
constexpr uint64_t DBMS_DISTRIBUTED_SIGNATURE_HEADER = 0xCAFEDACEull;
constexpr uint64_t DBMS_DISTRIBUTED_SIGNATURE_HEADER_OLD_FORMAT = 0xCAFECABEull;
