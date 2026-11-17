#include <Interpreters/capInsertBlockSizeBytesToMemoryLimit.h>

#include <Core/Settings.h>
#include <Common/Exception.h>
#include <Common/MemoryTracker.h>

#include <algorithm>

namespace DB
{

namespace Setting
{
    extern const SettingsDouble insert_squashing_max_memory_ratio;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

size_t capInsertBlockSizeBytesToMemoryLimit(size_t min_block_size_bytes, const Settings & settings)
{
    const double ratio = settings[Setting::insert_squashing_max_memory_ratio];
    /// The negated comparison also rejects NaN. A ratio above 1 is meaningless, and rejecting it keeps the
    /// product with the memory limit representable in `size_t`.
    if (!(ratio >= 0 && ratio <= 1))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "The value of the setting `insert_squashing_max_memory_ratio` must be from 0 to 1, got {}", ratio);
    if (ratio == 0)
        return min_block_size_bytes;

    const Int64 memory_limit = total_memory_tracker.getHardLimit();
    if (memory_limit <= 0)
        return min_block_size_bytes;

    /// A threshold of 0 means "no threshold", so a cap that rounds down to 0 must not turn into that.
    if (min_block_size_bytes == 0)
        return 0;
    const size_t cap = std::max<size_t>(1, static_cast<size_t>(static_cast<double>(memory_limit) * ratio));
    return std::min(min_block_size_bytes, cap);
}

}
