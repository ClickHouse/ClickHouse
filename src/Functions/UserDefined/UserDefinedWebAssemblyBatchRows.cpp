#include <Functions/UserDefined/UserDefinedWebAssemblyBatchRows.h>

#include <base/types.h>

#include <algorithm>

namespace DB
{

size_t chooseWasmBatchRows(
    const ColumnsWithTypeAndName & arguments,
    size_t start_idx,
    size_t remaining,
    size_t budget,
    std::optional<size_t> memory_ceiling,
    const WasmBatchMeasurer & measure)
{
    /// A function without arguments is handed no input buffer, so no size bounds its calls.
    if (arguments.empty())
        return remaining;

    static constexpr size_t max_probes = 24;
    /// A probe may only ask for this many times the rows the previous probe measured. The
    /// extrapolated count is read off a prefix, and a prefix of narrow rows says nothing about
    /// wider rows later in the block, so growth is paid for by rows already materialized.
    /// Reaching any batch size still costs a logarithmic number of probes.
    static constexpr size_t max_growth_per_probe = 4;

    /// Probe upwards from a single row, rather than downwards from the whole block. A probe
    /// serializes the candidate, and for a wire that does not carry constness a `ColumnConst`
    /// argument is materialized to do it, so a first probe of the whole block would expand
    /// exactly the input the splitting exists to rescue.
    size_t candidate = 1;
    size_t largest_fitting = 0;
    size_t smallest_overflowing = remaining + 1;

    /// The previous measurement, so the next candidate can be read off a slope. There is no
    /// previous measurement while `previous_rows` is zero.
    size_t previous_rows = 0;
    size_t previous_bytes = 0;

    for (size_t probe = 0; probe < max_probes; ++probe)
    {
        const size_t measured = measure(arguments, start_idx, candidate, {});
        if (measured > budget && candidate == 1)
        {
            const size_t fixed = measure(arguments, start_idx, 0, {});
            /// A row that does not fit next to a fixed part that does is still sent alone.
            if (fixed < budget)
                break;
            /// Every call repeats a fixed part over the budget, so the budget can only bound
            /// the rows on top of it. Sending the whole block instead would leave the call unbounded.
            budget += fixed;
            /// The enlarged budget must still fit the memory the guest can reach.
            if (memory_ceiling)
                budget = std::min(budget, *memory_ceiling);
        }

        if (measured <= budget)
        {
            largest_fitting = candidate;
            if (candidate == remaining)
                break;
        }
        else
        {
            smallest_overflowing = candidate;
            if (candidate == 1)
                break;
        }

        /// The boundary is known exactly once the bracket has nothing left between its ends.
        if (largest_fitting + 1 >= smallest_overflowing)
            break;

        /// An empty payload gives no slope to follow, so nothing bounds the batch but the block.
        if (measured == 0)
        {
            candidate = remaining;
            continue;
        }

        /// The marginal bytes a row adds. With one measurement in hand the average is all there
        /// is; it over-states the marginal cost, so the step it proposes is an undershoot, and
        /// the clamp below still moves the walk on by a row, which buys the second measurement
        /// the slope needs.
        Float64 bytes_per_row = static_cast<Float64>(measured) / static_cast<Float64>(candidate);
        if (previous_rows != 0 && candidate != previous_rows)
        {
            const Float64 slope = (static_cast<Float64>(measured) - static_cast<Float64>(previous_bytes))
                / (static_cast<Float64>(candidate) - static_cast<Float64>(previous_rows));
            if (slope > 0.0)
                bytes_per_row = slope;
        }
        previous_rows = candidate;
        previous_bytes = measured;

        const Float64 target = static_cast<Float64>(candidate)
            + (static_cast<Float64>(budget) - static_cast<Float64>(measured)) / bytes_per_row;

        size_t next = 1;
        if (target >= static_cast<Float64>(remaining))
            next = remaining;
        else if (target > 1.0)
            next = static_cast<size_t>(target);

        if (next > candidate)
            next = std::min(next, candidate * max_growth_per_probe);
        /// The bracket both keeps the candidate meaningful and guarantees progress: a candidate
        /// that fits raises the lower end past itself, one that overflows lowers the upper end
        /// below itself, and the check above leaves at least one row between the ends.
        next = std::clamp(next, largest_fitting + 1, smallest_overflowing - 1);

        /// A slope is only as good as the rows it was measured across. Two measurements that
        /// straddle one very wide row describe that row rather than the rows around it, and the
        /// step they propose lands next to the end of the bracket the walk came from, so the
        /// bracket shrinks by a row per probe and the batch stops far short of what the budget
        /// allows. Once both ends of the bracket are known, a proposal that falls in an outer
        /// quarter is replaced by the midpoint, which halves the bracket however wrong the
        /// slope was. A wire whose cost is close to affine is unaffected: its proposals land on
        /// the boundary itself, which is in the middle of the bracket by the time it is known.
        if (largest_fitting > 0 && smallest_overflowing <= remaining)
        {
            const size_t width = smallest_overflowing - largest_fitting;
            if (width > 3 && (next < largest_fitting + width / 4 || next > smallest_overflowing - width / 4))
                next = largest_fitting + width / 2;
        }

        candidate = next;
    }

    return std::max<size_t>(largest_fitting, 1);
}

}
