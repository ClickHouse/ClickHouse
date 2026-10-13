#pragma once

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <type_traits>

#include <base/defines.h>

#include <Common/CacheLine.h>
#include <Common/VariableContext.h>

namespace FiberLocalSlot
{
enum : size_t
{
    TRACE_CONTEXT,
    CURRENT_THREAD,
    INSIDE_SILK_FIBER,
    LOCK_MEMORY_EXCEPTION_COUNTER,
    LOCK_MEMORY_EXCEPTION_LEVEL,
    LOCK_MEMORY_EXCEPTION_BLOCK_FAULT_INJECTIONS,
    MEMORY_TRACKER_BLOCKER_LEVEL,
    MEMORY_TRACKER_UNTRACKED_ALLOCATIONS_BLOCKER_COUNTER,
#if !defined(NDEBUG)
    MEMORY_TRACKER_ALWAYS_THROW_ON_ALLOCATION,
#endif
#if defined(SILK_THREAD_LOCAL_STORAGE_SANITIZER)
    THREAD_LOCAL_STORAGE_SANITIZER_FIRST_SEEN,
    THREAD_LOCAL_STORAGE_SANITIZER_INSIDE,
#endif
    COUNT,
};

/// Slots below this are private to a stackful coroutine; the rest stay shared with its thread.
inline constexpr size_t COROUTINE_LOCAL_COUNT = CURRENT_THREAD;
}

/// Support defaults while keeping FiberLocal zero-overhead on access and
/// keeping FiberLocal and FiberLocalStorage constant-initialized.
constexpr uintptr_t fiberLocalSlotDefault(size_t slot)
{
    switch (slot)
    {
        case FiberLocalSlot::MEMORY_TRACKER_BLOCKER_LEVEL:
            return static_cast<uintptr_t>(VariableContext::Max);
        default:
            return 0;
    }
}

constexpr std::array<uintptr_t, FiberLocalSlot::COUNT> fiberLocalSlotDefaults()
{
    std::array<uintptr_t, FiberLocalSlot::COUNT> defaults{};
    for (size_t slot = 0; slot < defaults.size(); ++slot)
        defaults[slot] = fiberLocalSlotDefault(slot);
    return defaults;
}

template <typename T>
concept FiberLocalStoredInline
    = std::is_scalar_v<T>
    && sizeof(T) <= sizeof(void *)
    && alignof(T) <= alignof(void *);

/// Storage for FiberLocal slots.
///
/// (1) One instance per execution context (thread, silk fiber, or stackful coroutine).
///
/// (2) FiberLocalStoredInline variables (e.g. integers, pointers) are stored inline.
///     Accessing those is zero overhead instructions vs plain TLS.
///     Non-FiberLocalStoredInline variables are allocated on the heap and destroyed
///     on execution context exit.
///
/// (3) Constant-initialized and trivially destructible, which avoids static initialization/destruction
///     order related complexity and provides identical semantics with plain TLS variables
///     for threads. So static objects' constructors and destructors may use FiberLocal
///     just like they would use normal thread_local variables.
///
/// (4) A context switch should swap the arenas' (i.e. slots) contents. This is where we pay for
///     zero overhead on access.
///
/// (5) One type = one slot. Slots numbers are constant (see FiberLocalSlot enum).
///     So destructors are stored in a static array.
///     Example: type A is stored in slot 0, so we store ~A in slot_destructors[0]
///     once and won't ever overwrite it.
///
/// To achieve both (2) and (3):
/// 1. For threads, the class holds its instance in TLS
///    along with ThreadStorageCleaner which runs destroySlots.
/// 2. Other usages (i.e. fibers and coroutines) hold
///    std::unique_ptr<FiberLocalStorage, SlotsDestroyer> (FiberLocalStorage::Holder).
///
class FiberLocalStorage
{
public:
    struct SlotsDestroyer
    {
        void operator()(FiberLocalStorage * storage) const noexcept;
    };

    using Holder = std::unique_ptr<FiberLocalStorage, SlotsDestroyer>;

    static Holder create();

    template <typename T, size_t slot>
    static T load() noexcept
    {
        uintptr_t word = load<slot>();
        T value;
        std::memcpy(&value, &word, sizeof(T));
        return value;
    }

    template <typename T, size_t slot>
    static void store(T value) noexcept
    {
        uintptr_t word = 0;
        std::memcpy(&word, &value, sizeof(T));
        store<slot>(word);
    }

    template <typename T, size_t slot>
    static T & heapObject()
    {
        auto * object = reinterpret_cast<T *>(load<slot>());
        if (!object)
        {
            object = new T();
            store<slot>(reinterpret_cast<uintptr_t>(object));
            registerDestructor(slot, [](void * raw) { delete static_cast<T *>(raw); });
            armThreadStorageCleaner();
        }
        return *object;
    }

    static void swap(FiberLocalStorage & saved) noexcept
    {
        thread_storage.slots.swap(saved.slots);
    }

    static void swapCoroutineLocal(FiberLocalStorage & saved) noexcept
    {
        std::swap_ranges(
            thread_storage.slots.begin(),
            thread_storage.slots.begin() + FiberLocalSlot::COROUTINE_LOCAL_COUNT,
            saved.slots.begin());
    }

    void destroySlots() noexcept;

private:

    /// A fiber may resume on another OS thread, but the compiler may hoist &slots[slot].

    static constexpr size_t slot_count = FiberLocalSlot::COUNT;

    __attribute__((noinline)) static std::array<uintptr_t, slot_count> & currentSlots() noexcept
    {
        __asm__ __volatile__("" ::: "memory");
        return thread_storage.slots;
    }

    template <size_t slot>
    static uintptr_t load() noexcept
    {
        static_assert(slot < slot_count);
        uintptr_t value = 0;
#if defined(__x86_64__) && defined(__ELF__)
        __asm__ __volatile__(
            "movq %%fs:FiberLocalStorageThreadStorage@tpoff+%c1, %0"
            : "=r"(value)
            : "i"(slot * sizeof(void *))
            : "memory");
#elif defined(__aarch64__) && defined(__ELF__)
        __asm__ __volatile__(
            "mrs %0, tpidr_el0\n\t"
            "add %0, %0, :tprel_hi12:FiberLocalStorageThreadStorage+%c1\n\t"
            "ldr %0, [%0, :tprel_lo12_nc:FiberLocalStorageThreadStorage+%c1]"
            : "=&r"(value)
            : "i"(slot * sizeof(void *))
            : "memory");
#else
        value = currentSlots()[slot];
#endif
        return value;
    }

    template <size_t slot>
    static void store(uintptr_t value) noexcept
    {
#if defined(__x86_64__) && defined(__ELF__)
        __asm__ __volatile__(
            "movq %1, %%fs:FiberLocalStorageThreadStorage@tpoff+%c0"
            :
            : "i"(slot * sizeof(void *)), "r"(value)
            : "memory");
#elif defined(__aarch64__) && defined(__ELF__)
        void * address = nullptr;
        __asm__ __volatile__(
            "mrs %0, tpidr_el0\n\t"
            "add %0, %0, :tprel_hi12:FiberLocalStorageThreadStorage+%c1\n\t"
            "str %2, [%0, :tprel_lo12_nc:FiberLocalStorageThreadStorage+%c1]"
            : "=&r"(address)
            : "i"(slot * sizeof(void *)), "r"(value)
            : "memory");
#else
        currentSlots()[slot] = value;
#endif
    }

    static void registerDestructor(size_t slot, void (* destroy)(void *)) noexcept;
    static void armThreadStorageCleaner() noexcept;

    struct ThreadStorageCleaner;

    static constinit std::array<std::atomic<void (*)(void *)>, slot_count> slot_destructors;
    static thread_local constinit FiberLocalStorage thread_storage asm("FiberLocalStorageThreadStorage");

    alignas(DB::CH_CACHE_LINE_SIZE) std::array<uintptr_t, slot_count> slots = fiberLocalSlotDefaults();
    static_assert(
        sizeof(slots) <= 2 * DB::CH_CACHE_LINE_SIZE,
        "One slot is one word, and a context switch swaps the whole arena: keep it within two cache lines");
};

/// Fiber-aware thread_local variable. Zero overhead vs plain TLS on access.
/// Works in plain threads, silk fibers, and stackful coroutines.
template <typename T, size_t slot, auto default_value = uintptr_t{0}>
class FiberLocal
{
    static_assert(static_cast<uintptr_t>(default_value) == fiberLocalSlotDefault(slot), "default_value must match fiberLocalSlotDefault");

public:
    T get() const requires FiberLocalStoredInline<T> { return FiberLocalStorage::load<T, slot>(); }
    operator T() const requires FiberLocalStoredInline<T> { return get(); } /// NOLINT(google-explicit-constructor)
    T operator->() const requires (FiberLocalStoredInline<T> && std::is_pointer_v<T>) { return get(); }

    FiberLocal & operator=(T value) requires FiberLocalStoredInline<T>
    {
        FiberLocalStorage::store<T, slot>(value);
        return *this;
    }

    T & get() const requires (!FiberLocalStoredInline<T>) { return FiberLocalStorage::heapObject<T, slot>(); }
    operator T &() const requires (!FiberLocalStoredInline<T>) { return get(); } /// NOLINT(google-explicit-constructor)
    T & operator*() const requires (!FiberLocalStoredInline<T>) { return get(); }
    T * operator->() const requires (!FiberLocalStoredInline<T>) { return &get(); }
};
