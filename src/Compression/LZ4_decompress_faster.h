#pragma once

#include <cmath>
#include <random>
#include <pcg_random.hpp>


namespace LZ4
{

/** There are many implementation details of LZ4 decompression loop, that affect performance.
  * For example: copy by 8 or by 16 (SSE2) bytes at once; use shuffle (SSSE3) instruction to replicate match or not.
  *
  * The optimal algorithm is dependent on:
  *
  * 1. CPU architecture.
  * (example: on Skylake it's almost always better to copy by 16 bytes and use shuffle,
  *  but on Westmere using shuffle is worse and copy by 16 bytes is better only for high compression ratios)
  *
  * 2. Data distribution.
  * (example: when compression ratio is higher than 10.20,
  *  it's usually better to copy by 16 bytes rather than 8).
  *
  * It's very difficult to test all combinations on different CPUs and to choose correct rule to select best variant.
  * (Even if you do this, you have high chance to over-optimize for specific CPU while downgrading performance on another.)
  *
  * Instead of this, we choose best algorithm by using performance statistics
  *  with something like "Bayesian Bandits" method.
  */


/** Both buffers passed to 'decompress' function must have
  *  at least this amount of excessive bytes after end of data
  *  that is allowed to read/write.
  * This value is a little overestimation.
  */
static constexpr size_t ADDITIONAL_BYTES_AT_END_OF_BUFFER = 64;


/** When decompressing uniform sequence of blocks (for example, blocks from one file),
  *  you can pass single PerformanceStatistics object to subsequent invocations of 'decompress' method.
  * It will accumulate statistics and use it as a feedback to choose best specialization of algorithm at runtime.
  * One PerformanceStatistics object cannot be used concurrently from different threads.
  */
struct PerformanceStatistics
{
    struct Element
    {
        double count = 0;
        double sum = 0;

        double adjustedCount() const
        {
            return count - NUM_INVOCATIONS_TO_THROW_OFF;
        }

        double mean() const
        {
            return sum / adjustedCount();
        }

        /// For better convergence, we don't use proper estimate of stddev.
        /// We want to eventually separate between two algorithms even in case
        ///  when there is no statistical significant difference between them.
        double sigma() const
        {
            return mean() / sqrt(adjustedCount());
        }

        void update(double seconds, double bytes)
        {
            ++count;

            if (count > NUM_INVOCATIONS_TO_THROW_OFF)
                sum += seconds / bytes;
        }

        double sample(pcg64 & stat_rng) const
        {
            /// If there is a variant with not enough statistics, always choose it.
            /// And in that case prefer variant with less number of invocations.

            if (adjustedCount() < 2)
                return adjustedCount() - 1;
            else
                return std::normal_distribution<>(mean(), sigma())(stat_rng);
        }
    };

    /// Number of different algorithms to select from.
    static constexpr size_t NUM_ELEMENTS = 4;

    /// Cold invocations may be affected by additional memory latencies. Don't take first invocations into account.
    static constexpr double NUM_INVOCATIONS_TO_THROW_OFF = 2;

    /// How to select method to run.
    /// -1 - automatically, based on statistics (default);
    /// >= 0 - always choose specified method (for performance testing);
    /// -2 - choose methods in round robin fashion (for performance testing).
    ssize_t choose_method = -1;

    Element data[NUM_ELEMENTS];

    /// It's Ok that generator is not seeded.
    pcg64 rng;

    /// To select from different algorithms we use a kind of "bandits" algorithm.
    /// Sample random values from estimated normal distributions and choose the minimal.
    size_t select()
    {
        if (choose_method < 0)
        {
            double samples[NUM_ELEMENTS];
            for (size_t i = 0; i < NUM_ELEMENTS; ++i)
                samples[i] = choose_method == -1
                    ? data[i].sample(rng)
                    : data[i].adjustedCount();

            return std::min_element(samples, samples + NUM_ELEMENTS) - samples;
        }
        else
            return choose_method;
    }

    PerformanceStatistics() = default;
    explicit PerformanceStatistics(ssize_t choose_method_) : choose_method(choose_method_) {}
};


/** This method dispatch to one of different implementations depending on performance statistics.
  */
bool decompress(
    const char * const source,
    char * const dest,
    size_t source_size,
    size_t dest_size,
    PerformanceStatistics & statistics);


/** Obtain statistics about LZ4 block useful for development.
  */
struct StreamStatistics
{
    size_t num_tokens = 0;
    size_t sum_literal_lengths = 0;
    size_t sum_match_lengths = 0;
    size_t sum_match_offsets = 0;
    size_t count_match_offset_less_8 = 0;
    size_t count_match_offset_less_16 = 0;
    size_t count_match_replicate_itself = 0;

    void literal(size_t length);
    void match(size_t length, size_t offset);

    void print() const;
};

void statistics(
    const char * const source,
    char * const dest,
    size_t dest_size,
    StreamStatistics & stat);

}
