#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>

namespace DB
{

/// Experimental work-score heuristic, constrained to the sizes and low match rates studied on Arm.
/// Sampled key presence is row-weighted and independent of duplicate output fanout.
struct AutoFingerprintPolicy
{
    static constexpr size_t initial_sample_rows = 4096;
    static constexpr size_t monitor_sample_rows = 128;
    static constexpr size_t monitor_disable_hits = 8;

    enum class Reason : uint8_t
    {
        Selected,
        MissingEstimate,
        SmallBuild,
        LargeBuild,
        ShortProbe,
        SmallSample,
        HighMatchRate,
        InsufficientSaving,
    };

    static Reason select(
        std::optional<uint64_t> estimated_probe_rows, size_t build_keys, size_t capacity, size_t sample_rows, size_t sample_hits)
    {
        if (!estimated_probe_rows)
            return Reason::MissingEstimate;
        if (build_keys < 65536 || capacity < 262144)
            return Reason::SmallBuild;
        if (capacity > 4194304)
            return Reason::LargeBuild;
        const double probes = static_cast<double>(*estimated_probe_rows);
        if (probes < 16.0 * static_cast<double>(build_keys) || probes < 4.0 * static_cast<double>(capacity))
            return Reason::ShortProbe;
        if (sample_rows < initial_sample_rows)
            return Reason::SmallSample;

        /// Add eight possible hits as a conservative score, not an IID confidence interval over the future stream.
        const double hit_rate = static_cast<double>(sample_hits + 8) / static_cast<double>(sample_rows + 8);
        if (hit_rate > 0.02)
            return Reason::HighMatchRate;

        /// Relative work units: miss saving 1, hit penalty 8, preparation 2 per allocated slot.
        /// Require another 10% of probe work after charging the sample itself.
        const double saving = probes * (1.0 - 9.0 * hit_rate)
            - 2.0 * static_cast<double>(capacity) - 2.0 * static_cast<double>(sample_rows);
        if (saving < 0.1 * probes)
            return Reason::InsufficientSaving;
        return Reason::Selected;
    }
};

}
