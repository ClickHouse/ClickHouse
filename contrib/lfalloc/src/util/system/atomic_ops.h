#pragma once

#include <type_traits>

template <typename T>
inline TAtomic* AsAtomicPtr(T volatile* target) {
    return reinterpret_cast<TAtomic*>(target);
}

template <typename T>
inline const TAtomic* AsAtomicPtr(T const volatile* target) {
    return reinterpret_cast<const TAtomic*>(target);
}

// integral types

template <typename T>
struct TAtomicTraits {
    enum {
        Castable = std::is_integral<T>::value && sizeof(T) == sizeof(TAtomicBase) && !std::is_const<T>::value,
    };
};

template <typename T, typename TT>
using TEnableIfCastable = std::enable_if_t<TAtomicTraits<T>::Castable, TT>;

template <typename T>
inline TEnableIfCastable<T, T> AtomicGet(T const volatile& target) {
    return static_cast<T>(AtomicGet(*AsAtomicPtr(&target)));
}

template <typename T>
inline TEnableIfCastable<T, void> AtomicSet(T volatile& target, TAtomicBase value) {
    AtomicSet(*AsAtomicPtr(&target), value);
}

template <typename T>
inline TEnableIfCastable<T, T> AtomicIncrement(T volatile& target) {
    return static_cast<T>(AtomicIncrement(*AsAtomicPtr(&target)));
}

template <typename T>
inline TEnableIfCastable<T, T> AtomicGetAndIncrement(T volatile& target) {
    return static_cast<T>(AtomicGetAndIncrement(*AsAtomicPtr(&target)));
}

template <typename T>
inline TEnableIfCastable<T, T> AtomicDecrement(T volatile& target) {
    return static_cast<T>(AtomicDecrement(*AsAtomicPtr(&target)));
}

template <typename T>
inline TEnableIfCastable<T, T> AtomicGetAndDecrement(T volatile& target) {
    return static_cast<T>(AtomicGetAndDecrement(*AsAtomicPtr(&target)));
}

template <typename T>
inline TEnableIfCastable<T, T> AtomicAdd(T volatile& target, TAtomicBase value) {
    return static_cast<T>(AtomicAdd(*AsAtomicPtr(&target), value));
}

template <typename T>
inline TEnableIfCastable<T, T> AtomicGetAndAdd(T volatile& target, TAtomicBase value) {
    return static_cast<T>(AtomicGetAndAdd(*AsAtomicPtr(&target), value));
}

template <typename T>
inline TEnableIfCastable<T, T> AtomicSub(T volatile& target, TAtomicBase value) {
    return static_cast<T>(AtomicSub(*AsAtomicPtr(&target), value));
}

template <typename T>
inline TEnableIfCastable<T, T> AtomicGetAndSub(T volatile& target, TAtomicBase value) {
    return static_cast<T>(AtomicGetAndSub(*AsAtomicPtr(&target), value));
}

template <typename T>
inline TEnableIfCastable<T, T> AtomicSwap(T volatile* target, TAtomicBase exchange) {
    return static_cast<T>(AtomicSwap(AsAtomicPtr(target), exchange));
}

template <typename T>
inline TEnableIfCastable<T, bool> AtomicCas(T volatile* target, TAtomicBase exchange, TAtomicBase compare) {
    return AtomicCas(AsAtomicPtr(target), exchange, compare);
}

template <typename T>
inline TEnableIfCastable<T, T> AtomicGetAndCas(T volatile* target, TAtomicBase exchange, TAtomicBase compare) {
    return static_cast<T>(AtomicGetAndCas(AsAtomicPtr(target), exchange, compare));
}

template <typename T>
inline TEnableIfCastable<T, bool> AtomicTryLock(T volatile* target) {
    return AtomicTryLock(AsAtomicPtr(target));
}

template <typename T>
inline TEnableIfCastable<T, bool> AtomicTryAndTryLock(T volatile* target) {
    return AtomicTryAndTryLock(AsAtomicPtr(target));
}

template <typename T>
inline TEnableIfCastable<T, void> AtomicUnlock(T volatile* target) {
    AtomicUnlock(AsAtomicPtr(target));
}

template <typename T>
inline TEnableIfCastable<T, T> AtomicOr(T volatile& target, TAtomicBase value) {
    return static_cast<T>(AtomicOr(*AsAtomicPtr(&target), value));
}

template <typename T>
inline TEnableIfCastable<T, T> AtomicAnd(T volatile& target, TAtomicBase value) {
    return static_cast<T>(AtomicAnd(*AsAtomicPtr(&target), value));
}

template <typename T>
inline TEnableIfCastable<T, T> AtomicXor(T volatile& target, TAtomicBase value) {
    return static_cast<T>(AtomicXor(*AsAtomicPtr(&target), value));
}

// pointer types

template <typename T>
inline T* AtomicGet(T* const volatile& target) {
    return reinterpret_cast<T*>(AtomicGet(*AsAtomicPtr(&target)));
}

template <typename T>
inline void AtomicSet(T* volatile& target, T* value) {
    AtomicSet(*AsAtomicPtr(&target), reinterpret_cast<TAtomicBase>(value));
}

using TNullPtr = decltype(nullptr);

template <typename T>
inline void AtomicSet(T* volatile& target, TNullPtr) {
    AtomicSet(*AsAtomicPtr(&target), 0);
}

template <typename T>
inline T* AtomicSwap(T* volatile* target, T* exchange) {
    return reinterpret_cast<T*>(AtomicSwap(AsAtomicPtr(target), reinterpret_cast<TAtomicBase>(exchange)));
}

template <typename T>
inline T* AtomicSwap(T* volatile* target, TNullPtr) {
    return reinterpret_cast<T*>(AtomicSwap(AsAtomicPtr(target), 0));
}

template <typename T>
inline bool AtomicCas(T* volatile* target, T* exchange, T* compare) {
    return AtomicCas(AsAtomicPtr(target), reinterpret_cast<TAtomicBase>(exchange), reinterpret_cast<TAtomicBase>(compare));
}

template <typename T>
inline T* AtomicGetAndCas(T* volatile* target, T* exchange, T* compare) {
    return reinterpret_cast<T*>(AtomicGetAndCas(AsAtomicPtr(target), reinterpret_cast<TAtomicBase>(exchange), reinterpret_cast<TAtomicBase>(compare)));
}

template <typename T>
inline bool AtomicCas(T* volatile* target, T* exchange, TNullPtr) {
    return AtomicCas(AsAtomicPtr(target), reinterpret_cast<TAtomicBase>(exchange), 0);
}

template <typename T>
inline T* AtomicGetAndCas(T* volatile* target, T* exchange, TNullPtr) {
    return reinterpret_cast<T*>(AtomicGetAndCas(AsAtomicPtr(target), reinterpret_cast<TAtomicBase>(exchange), 0));
}

template <typename T>
inline bool AtomicCas(T* volatile* target, TNullPtr, T* compare) {
    return AtomicCas(AsAtomicPtr(target), 0, reinterpret_cast<TAtomicBase>(compare));
}

template <typename T>
inline T* AtomicGetAndCas(T* volatile* target, TNullPtr, T* compare) {
    return reinterpret_cast<T*>(AtomicGetAndCas(AsAtomicPtr(target), 0, reinterpret_cast<TAtomicBase>(compare)));
}

template <typename T>
inline bool AtomicCas(T* volatile* target, TNullPtr, TNullPtr) {
    return AtomicCas(AsAtomicPtr(target), 0, 0);
}

template <typename T>
inline T* AtomicGetAndCas(T* volatile* target, TNullPtr, TNullPtr) {
    return reinterpret_cast<T*>(AtomicGetAndCas(AsAtomicPtr(target), 0, 0));
}
