#pragma once

#include <array>
#include <chrono>
#include <exception>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <tuple>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wc++26-extensions"
#include <boost/pfr/core.hpp>
#pragma clang diagnostic pop

/** Compile-time approximation of "self-contained": a type owns every byte of heap it references and can
  * be deep-copied, with no shared or borrowed indirection. System-log elements must be self-contained so
  * that all of their memory is allocated when the element is built (under the memory blocker in
  * SystemLogBase::add) and freed when the element is destroyed (by the flush thread). A shared_ptr,
  * weak_ptr, raw pointer or std::exception_ptr breaks this: its pointee is allocated by whoever produced
  * it and freed by whoever drops the last reference, leaking memory onto the wrong tracker.
  *
  * The check reflects members with boost::pfr, so it has two inherent limitations:
  *  - it can only see through AGGREGATES; a type with any user-declared constructor is opaque;
  *  - non-aggregate leaves must be recognised as self-contained by other means. Two cover most cases:
  *    trivially-copyable types (cannot own heap or hold a shared_ptr) are accepted automatically, and
  *    heap-owning value types (String, containers, ClickHouse's Field/Settings/...) are trusted either
  *    through OwningValue (std types, recursed into) or SelfContainedLeaf (opt-in, not recursed).
  *
  * It cannot catch a legal value type whose value borrows external state (aliasing) - that is what the
  * runtime memory-tracking sanitizer is for; this is only a cheap compile-time tripwire.
  */

namespace DB
{

enum class SelfContainedStatus : int
{
    Ok = 0,
    Unverifiable = 1,      /// non-aggregate, non-trivial, non-whitelisted leaf - cannot be inspected
    SharedIndirection = 2, /// found a shared_ptr / weak_ptr / raw pointer / exception_ptr
};

constexpr SelfContainedStatus worse(SelfContainedStatus a, SelfContainedStatus b)
{
    return static_cast<int>(a) >= static_cast<int>(b) ? a : b;
}

/// Types that share or borrow heap owned elsewhere - the leak vector.
template <typename> struct IsSharedIndirection : std::false_type {};
template <typename T> struct IsSharedIndirection<std::shared_ptr<T>> : std::true_type {};
template <typename T> struct IsSharedIndirection<std::weak_ptr<T>> : std::true_type {};
template <> struct IsSharedIndirection<std::exception_ptr> : std::true_type {};

/// Standard owning value types; recurse into their payload element types.
template <typename> struct OwningValue : std::false_type { using Payload = std::tuple<>; };
template <typename C, typename R, typename A> struct OwningValue<std::basic_string<C, R, A>> : std::true_type { using Payload = std::tuple<>; };
template <typename E, typename A> struct OwningValue<std::vector<E, A>> : std::true_type { using Payload = std::tuple<E>; };
template <typename K, typename V, typename C, typename A> struct OwningValue<std::map<K, V, C, A>> : std::true_type { using Payload = std::tuple<K, V>; };
template <typename K, typename V, typename H, typename E, typename A> struct OwningValue<std::unordered_map<K, V, H, E, A>> : std::true_type { using Payload = std::tuple<K, V>; };
template <typename K, typename C, typename A> struct OwningValue<std::set<K, C, A>> : std::true_type { using Payload = std::tuple<K>; };
template <typename K, typename H, typename E, typename A> struct OwningValue<std::unordered_set<K, H, E, A>> : std::true_type { using Payload = std::tuple<K>; };
template <typename E> struct OwningValue<std::optional<E>> : std::true_type { using Payload = std::tuple<E>; };
template <typename T, typename D> struct OwningValue<std::unique_ptr<T, D>> : std::true_type { using Payload = std::tuple<T>; };
template <typename E, std::size_t N> struct OwningValue<std::array<E, N>> : std::true_type { using Payload = std::tuple<E>; };
template <typename A, typename B> struct OwningValue<std::pair<A, B>> : std::true_type { using Payload = std::tuple<A, B>; };
template <typename... Es> struct OwningValue<std::tuple<Es...>> : std::true_type { using Payload = std::tuple<Es...>; };
template <typename... Es> struct OwningValue<std::variant<Es...>> : std::true_type { using Payload = std::tuple<Es...>; };
template <typename C, typename D> struct OwningValue<std::chrono::time_point<C, D>> : std::true_type { using Payload = std::tuple<>; };
template <typename R, typename P> struct OwningValue<std::chrono::duration<R, P>> : std::true_type { using Payload = std::tuple<>; };

/// Opt-in for heap-owning value types that are self-contained but cannot be reflected (e.g. ClickHouse's
/// Field, Settings, ProfileEvents::Counters::Snapshot). Specialise to std::true_type near their use. These
/// are trusted wholesale (not recursed into), so only specialise types with no shared/borrowed ownership.
template <typename> struct SelfContainedLeaf : std::false_type {};

template <typename T> consteval SelfContainedStatus selfContainedStatus();

template <typename... Ts> consteval SelfContainedStatus statusOfAll()
{
    SelfContainedStatus status = SelfContainedStatus::Ok;
    ((status = worse(status, selfContainedStatus<std::remove_cvref_t<Ts>>())), ...);
    return status;
}

template <typename Tuple> consteval SelfContainedStatus payloadStatus()
{
    return []<typename... P>(std::tuple<P...> *) { return statusOfAll<P...>(); }(static_cast<Tuple *>(nullptr));
}

template <typename T, std::size_t... I> consteval SelfContainedStatus fieldsStatus(std::index_sequence<I...>)
{
    return statusOfAll<boost::pfr::tuple_element_t<I, T>...>();
}

template <typename T> consteval SelfContainedStatus selfContainedStatus()
{
    if constexpr (std::is_pointer_v<T> || IsSharedIndirection<T>::value)
        return SelfContainedStatus::SharedIndirection;
    else if constexpr (std::is_scalar_v<T> || std::is_enum_v<T>)
        return SelfContainedStatus::Ok;
    else if constexpr (OwningValue<T>::value)
        return payloadStatus<typename OwningValue<T>::Payload>();
    else if constexpr (SelfContainedLeaf<T>::value)
        return SelfContainedStatus::Ok;
    else if constexpr (std::is_aggregate_v<T>)
        /// Recurse before the trivially-copyable shortcut so a raw pointer inside an aggregate is caught.
        return fieldsStatus<T>(std::make_index_sequence<boost::pfr::tuple_size_v<T>>{});
    else if constexpr (std::is_trivially_copyable_v<T>)
        /// Non-aggregate POD leaf (Decimal, UUID, IPvX, wide ints): cannot own heap or hold a shared_ptr.
        /// This also admits std::string_view, which does NOT own its storage: whoever stores one in a log
        /// element is responsible for the pointed-to memory outliving the element - in practice it must be
        /// static (e.g. a compile-time format string literal). We don't try to prove the pointee is static;
        /// that is not feasible at compile time.
        return SelfContainedStatus::Ok;
    else
        return SelfContainedStatus::Unverifiable;
}

template <typename T> constexpr bool is_self_contained_v = selfContainedStatus<T>() == SelfContainedStatus::Ok;

/// Hard compile-time gate for system-log elements, with a cause-specific message.
#define ASSERT_SELF_CONTAINED_LOG_ELEMENT(ELEMENT) \
    static_assert( \
        ::DB::selfContainedStatus<ELEMENT>() != ::DB::SelfContainedStatus::SharedIndirection, \
        #ELEMENT " is not self-contained: it (transitively) holds a shared_ptr, weak_ptr, raw pointer or " \
        "std::exception_ptr. System-log elements must own all their memory (it is allocated under the " \
        "memory blocker in SystemLogBase::add and freed with the element by the flush thread). Replace the " \
        "shared/borrowed member with a self-contained value (e.g. a Snapshot by value instead of a " \
        "shared_ptr, an error message string instead of an exception_ptr)."); \
    static_assert( \
        ::DB::selfContainedStatus<ELEMENT>() != ::DB::SelfContainedStatus::Unverifiable, \
        #ELEMENT " cannot be verified as self-contained. boost::pfr can only reflect aggregates, so either " \
        "make the type an aggregate (remove its user-declared constructors - the fill callback of " \
        "SystemLogBase::add builds it in place, so constructors are unnecessary), or, if it is a " \
        "heap-owning value type with no shared ownership, register it via SelfContainedLeaf<>.")

}
