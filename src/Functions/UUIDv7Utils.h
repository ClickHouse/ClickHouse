#pragma once
#include <Common/SharedMutex.h>
#include <DataTypes/DataTypeUUID.h>

/// Common functionality for UUIDv7-related functions.

namespace DB
{

namespace UUIDv7Utils
{

/* Bit layout of UUIDv7

 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
├─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┤
|                           unix_ts_ms                          |
├─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┤
|          unix_ts_ms           |  ver  |   counter_high_bits   |
├─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┤
|var|                   counter_low_bits                        |
├─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┤
|                            rand_b                             |
└─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┘
*/

/// Bit counts
constexpr auto rand_a_bits_count = 12;
constexpr auto rand_b_bits_count = 62;
constexpr auto rand_b_low_bits_count = 32;
constexpr auto counter_high_bits_count = rand_a_bits_count;
constexpr auto counter_low_bits_count = 30;
constexpr auto bits_in_counter = counter_high_bits_count + counter_low_bits_count;
constexpr uint64_t counter_limit = (1ull << bits_in_counter);

/// Bit masks for UUIDv7 components
constexpr uint64_t variant_2_mask  = (2ull << rand_b_bits_count);
constexpr uint64_t rand_a_bits_mask = (1ull << rand_a_bits_count) - 1;
constexpr uint64_t rand_b_bits_mask = (1ull << rand_b_bits_count) - 1;
constexpr uint64_t rand_b_with_counter_bits_mask = (1ull << rand_b_low_bits_count) - 1;
constexpr uint64_t counter_low_bits_mask = (1ull << counter_low_bits_count) - 1;
constexpr uint64_t counter_high_bits_mask = rand_a_bits_mask;

void setTimestampAndVersion(UUID & uuid, uint64_t timestamp);
void setVariant(UUID & uuid);

struct CounterFields
{
    public:
        void resetCounter(const UUID & uuid);
        void incrementCounter(UUID & uuid);
        void generate(UUID & uuid, uint64_t timestamp);

    private:
        uint64_t last_timestamp = 0;
        uint64_t counter = 0;
};

struct Data
{
    /// Guarantee counter monotonicity within one timestamp across all threads generating UUIDv7 simultaneously.
    static inline CounterFields fields;
    static inline SharedMutex mutex; /// works a little bit faster than std::mutex here
    std::lock_guard<SharedMutex> guard;

    Data()
        : guard(mutex)
    {}

    void generate(UUID & uuid, uint64_t timestamp);
};

}

}
