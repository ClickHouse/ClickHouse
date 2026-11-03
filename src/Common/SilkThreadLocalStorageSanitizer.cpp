#include <Common/SilkThreadLocalStorageSanitizer.h>

#if defined(SILK_THREAD_LOCAL_STORAGE_SANITIZER)
#    include <array>
#    include <cstdint>
#    include <string_view>
#    include <utility>

#    include <fmt/format.h>

#    include <Common/Exception.h>
#    include <Common/FiberLocal.h>
#    include <Common/SilkFiberScheduler.h>

namespace
{

struct ThreadLocalStorageFirstSeen
{
    static constexpr size_t capacity = 1000;

    size_t size = 0;
    std::array<std::pair<std::string_view, const void *>, capacity> entries;

    const void * find(std::string_view name) const noexcept
    {
        for (size_t i = 0; i < size; ++i)
            if (entries[i].first == name)
                return entries[i].second;
        return nullptr;
    }

    void insert(std::string_view name, const void * address) noexcept
    {
        chassert(size < capacity);
        entries[size] = {name, address};
        ++size;
    }
};

/// Only allocates on first access, which happens at fiber start (FiberContext::main)
/// outside of silk_thread_local_storage_sanitizer_access_hook. See silk_thread_local_storage_sanitizer_fiber_init_hook.
constinit FiberLocal<ThreadLocalStorageFirstSeen, FiberLocalSlot::THREAD_LOCAL_STORAGE_SANITIZER_FIRST_SEEN> first_seen;
constinit FiberLocal<bool, FiberLocalSlot::THREAD_LOCAL_STORAGE_SANITIZER_INSIDE> inside_access_hook;
}

extern "C" void silk_thread_local_storage_sanitizer_fiber_init_hook() noexcept
{
    first_seen.get();
}
#endif

extern "C" void silk_thread_local_storage_sanitizer_access_hook([[maybe_unused]] void * address, [[maybe_unused]] const char * name) noexcept
{
#if defined(SILK_THREAD_LOCAL_STORAGE_SANITIZER)
    if (!Silk::isInsideFiber())
        return;

    /// The failure path below logs and aborts, which touches instrumented thread-locals; guard against recursion.
    if (inside_access_hook)
        return;
    inside_access_hook = true;

    if (const void * first = first_seen->find(name))
        chassert(first == address, fmt::format(
            "Fiber accessed thread_local '{}' at two different addresses: the fiber migrated between OS threads, "
            "and raw thread_local is not fiber-safe. Consider wrapping it in FiberLocal.",
            name));
    else
        first_seen->insert(name, address);

    inside_access_hook = false;
#endif
}
