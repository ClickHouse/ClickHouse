#include <gtest/gtest.h>

#include <Common/MutexProtected.h>

#include <atomic>
#include <functional>
#include <mutex>
#include <shared_mutex>
#include <type_traits>
#include <utility>
#include <vector>

namespace
{

struct A
{
    explicit A(int i_) : i(i_) {}

    int i;
};

using ProtectedA = DB::MutexProtected<A>;
using ReadOnlyAccessor = decltype(std::declval<const ProtectedA &>().getReadOnly());
using WriteEnabledAccessor = decltype(std::declval<ProtectedA &>().getWriteEnabled());
using LockOrderedReadWriteAccessors = decltype(DB::LockOrderedAccessorPair(
    DB::readOnly(std::declval<const ProtectedA &>()), DB::writeEnabled(std::declval<ProtectedA &>())));

static_assert(std::is_same_v<
    ProtectedA,
    DB::MutexProtected<A, DB::SharedMutex, std::unique_lock, std::shared_lock>>);

template <typename Protected>
concept CanGetReadOnlyFromRvalue = requires(Protected && value)
{
    std::move(value).getReadOnly();
};

template <typename Protected>
concept CanGetWriteEnabledFromRvalue = requires(Protected && value)
{
    std::move(value).getWriteEnabled();
};

template <typename Accessor>
concept CanDereferenceRvalue = requires(Accessor && accessor)
{
    *std::move(accessor);
};

template <typename Accessor>
concept CanAccessThroughRvalue = requires(Accessor && accessor)
{
    std::move(accessor).operator->();
};

template <typename Protected>
concept CanOverrideWriteLock = requires(Protected & value)
{
    value.template getWriteEnabled<std::shared_lock>();
};

template <typename Protected>
concept CanConstructLockOrderedAccessorsFromRvalue = requires(Protected && first, Protected & second)
{
    DB::LockOrderedAccessorPair(DB::readOnly(std::move(first)), DB::writeEnabled(second));
};

template <typename Accessors>
concept CanGetAccessorFromLvalue = requires(Accessors & accessors) { accessors.template get<0>(); };

template <typename Protected>
concept CanDereferenceTemporaryLockOrderedAccessor = requires(Protected & first, Protected & second) {
    *DB::LockOrderedAccessorPair(DB::readOnly(first), DB::writeEnabled(second)).template get<0>();
};

template <typename Protected>
concept CanAccessThroughTemporaryLockOrderedAccessor = requires(Protected & first, Protected & second) {
    DB::LockOrderedAccessorPair(DB::readOnly(first), DB::writeEnabled(second)).template get<0>().operator->();
};

template <typename Protected>
concept CanGetWriteAccessToConst = requires(const Protected & first, Protected & second)
{
    DB::LockOrderedAccessorPair(DB::writeEnabled(first), DB::readOnly(second));
};

static_assert(!std::is_copy_constructible_v<ReadOnlyAccessor>);
static_assert(!std::is_copy_assignable_v<ReadOnlyAccessor>);
static_assert(!std::is_move_constructible_v<ReadOnlyAccessor>);
static_assert(!std::is_move_assignable_v<ReadOnlyAccessor>);
static_assert(!std::is_copy_constructible_v<WriteEnabledAccessor>);
static_assert(!std::is_copy_assignable_v<WriteEnabledAccessor>);
static_assert(!std::is_move_constructible_v<WriteEnabledAccessor>);
static_assert(!std::is_move_assignable_v<WriteEnabledAccessor>);
static_assert(!std::is_copy_constructible_v<LockOrderedReadWriteAccessors>);
static_assert(!std::is_copy_assignable_v<LockOrderedReadWriteAccessors>);
static_assert(!std::is_move_constructible_v<LockOrderedReadWriteAccessors>);
static_assert(!std::is_move_assignable_v<LockOrderedReadWriteAccessors>);
static_assert(std::is_same_v<decltype(*std::declval<ReadOnlyAccessor &>()), const A &>);
static_assert(std::is_same_v<decltype(*std::declval<WriteEnabledAccessor &>()), A &>);
static_assert(std::is_same_v<decltype(std::declval<LockOrderedReadWriteAccessors &&>().template get<0>()), ReadOnlyAccessor>);
static_assert(std::is_same_v<decltype(std::declval<LockOrderedReadWriteAccessors &&>().template get<1>()), WriteEnabledAccessor>);
static_assert(!CanGetReadOnlyFromRvalue<ProtectedA>);
static_assert(!CanGetReadOnlyFromRvalue<const ProtectedA>);
static_assert(!CanGetWriteEnabledFromRvalue<ProtectedA>);
static_assert(!CanDereferenceRvalue<ReadOnlyAccessor>);
static_assert(!CanDereferenceRvalue<WriteEnabledAccessor>);
static_assert(!CanAccessThroughRvalue<ReadOnlyAccessor>);
static_assert(!CanAccessThroughRvalue<WriteEnabledAccessor>);
static_assert(!CanOverrideWriteLock<ProtectedA>);
static_assert(!CanConstructLockOrderedAccessorsFromRvalue<ProtectedA>);
static_assert(!CanGetAccessorFromLvalue<LockOrderedReadWriteAccessors>);
static_assert(!CanDereferenceTemporaryLockOrderedAccessor<ProtectedA>);
static_assert(!CanAccessThroughTemporaryLockOrderedAccessor<ProtectedA>);
static_assert(!CanGetWriteAccessToConst<ProtectedA>);

enum class LockKind
{
    Shared,
    Unique,
};

struct LockCounts
{
    static void reset()
    {
        unique_locks = 0;
        shared_locks = 0;
        acquisition_order.clear();
    }

    static inline std::atomic_size_t unique_locks = 0;
    static inline std::atomic_size_t shared_locks = 0;
    static inline std::vector<LockKind> acquisition_order;
};

template <class Mutex>
class TrackingSharedLock
{
public:
    explicit TrackingSharedLock(Mutex &)
    {
        ++LockCounts::shared_locks;
        LockCounts::acquisition_order.push_back(LockKind::Shared);
    }

    TrackingSharedLock(TrackingSharedLock && other) noexcept
        : owns_lock(std::exchange(other.owns_lock, false))
    {
    }

    ~TrackingSharedLock()
    {
        if (owns_lock)
            --LockCounts::shared_locks;
    }

    TrackingSharedLock(const TrackingSharedLock &) = delete;
    TrackingSharedLock & operator=(const TrackingSharedLock &) = delete;
    TrackingSharedLock & operator=(TrackingSharedLock &&) = delete;

private:
    bool owns_lock = true;
};

template <class Mutex>
class TrackingUniqueLock
{
public:
    explicit TrackingUniqueLock(Mutex &)
    {
        ++LockCounts::unique_locks;
        LockCounts::acquisition_order.push_back(LockKind::Unique);
    }

    TrackingUniqueLock(TrackingUniqueLock && other) noexcept
        : owns_lock(std::exchange(other.owns_lock, false))
    {
    }

    ~TrackingUniqueLock()
    {
        if (owns_lock)
            --LockCounts::unique_locks;
    }

    TrackingUniqueLock(const TrackingUniqueLock &) = delete;
    TrackingUniqueLock & operator=(const TrackingUniqueLock &) = delete;
    TrackingUniqueLock & operator=(TrackingUniqueLock &&) = delete;

private:
    bool owns_lock = true;
};

using TrackingMutexProtected = DB::MutexProtected<A, DB::SharedMutex, TrackingUniqueLock, TrackingSharedLock>;
using TrackingIntMutexProtected = DB::MutexProtected<int, DB::SharedMutex, TrackingUniqueLock, TrackingSharedLock>;

}

TEST(MutexProtected, GetReadOnly)
{
    int i = 0;
    DB::MutexProtected<A> a{A{5}};

    {
        auto roa = a.getReadOnly();
        i = roa->i;
    }

    EXPECT_EQ(i, 5);
}

TEST(MutexProtected, GetWriteEnabled)
{
    int i = 0;
    DB::MutexProtected<A> a{A{5}};

    {
        auto rwa = a.getWriteEnabled();
        i = ++rwa->i;
    }

    EXPECT_EQ(i, 6);
}

TEST(MutexProtected, GetReadOnlyAcquiresAndReleasesSharedLock)
{
    LockCounts::reset();
    TrackingMutexProtected a{A{5}};

    {
        auto roa = a.getReadOnly();
        EXPECT_EQ(roa->i, 5);
        EXPECT_EQ(LockCounts::shared_locks, 1);
        EXPECT_EQ(LockCounts::unique_locks, 0);
    }

    EXPECT_EQ(LockCounts::shared_locks, 0);
    EXPECT_EQ(LockCounts::unique_locks, 0);
}

TEST(MutexProtected, GetWriteEnabledAcquiresAndReleasesExclusiveLock)
{
    LockCounts::reset();
    TrackingMutexProtected a{A{5}};

    {
        auto rwa = a.getWriteEnabled();
        EXPECT_EQ(rwa->i, 5);
        EXPECT_EQ(LockCounts::shared_locks, 0);
        EXPECT_EQ(LockCounts::unique_locks, 1);
    }

    EXPECT_EQ(LockCounts::shared_locks, 0);
    EXPECT_EQ(LockCounts::unique_locks, 0);
}

TEST(MutexProtected, LockOrderedAccessorPairAcquiresInAddressOrder)
{
    LockCounts::reset();
    TrackingMutexProtected source{A{5}};
    TrackingMutexProtected destination{A{7}};
    const bool source_is_first = std::less<const void *>{}(&source, &destination);

    {
        auto [source_accessor, destination_accessor]
            = DB::LockOrderedAccessorPair(DB::readOnly(source), DB::writeEnabled(destination));
        static_assert(std::is_same_v<decltype(source_accessor), TrackingMutexProtected::ReadOnlyAccessor>);
        static_assert(std::is_same_v<decltype(destination_accessor), TrackingMutexProtected::WriteEnabledAccessor>);
        static_assert(std::is_same_v<decltype((source_accessor)), TrackingMutexProtected::ReadOnlyAccessor &>);
        static_assert(std::is_same_v<decltype((destination_accessor)), TrackingMutexProtected::WriteEnabledAccessor &>);
        ASSERT_EQ(LockCounts::acquisition_order.size(), 2);
        EXPECT_EQ(LockCounts::acquisition_order[0], source_is_first ? LockKind::Shared : LockKind::Unique);
        EXPECT_EQ(LockCounts::acquisition_order[1], source_is_first ? LockKind::Unique : LockKind::Shared);
        EXPECT_EQ(source_accessor->i, 5);
        ++destination_accessor->i;
        EXPECT_EQ(LockCounts::shared_locks, 1);
        EXPECT_EQ(LockCounts::unique_locks, 1);
    }

    EXPECT_EQ(LockCounts::shared_locks, 0);
    EXPECT_EQ(LockCounts::unique_locks, 0);

    LockCounts::reset();
    {
        auto [destination_accessor, source_accessor]
            = DB::LockOrderedAccessorPair(DB::readOnly(destination), DB::writeEnabled(source));
        ASSERT_EQ(LockCounts::acquisition_order.size(), 2);
        EXPECT_EQ(LockCounts::acquisition_order[0], source_is_first ? LockKind::Unique : LockKind::Shared);
        EXPECT_EQ(LockCounts::acquisition_order[1], source_is_first ? LockKind::Shared : LockKind::Unique);
        EXPECT_EQ(destination_accessor->i, 8);
        ++source_accessor->i;
    }

    EXPECT_EQ(LockCounts::shared_locks, 0);
    EXPECT_EQ(LockCounts::unique_locks, 0);
}

TEST(MutexProtected, LockOrderedAccessorPairSupportsDifferentValueTypes)
{
    LockCounts::reset();
    TrackingMutexProtected source{A{5}};
    TrackingIntMutexProtected destination{7};
    const bool source_is_first = std::less<const void *>{}(&source, &destination);

    {
        auto [source_accessor, destination_accessor]
            = DB::LockOrderedAccessorPair(DB::readOnly(source), DB::writeEnabled(destination));
        static_assert(std::is_same_v<decltype(source_accessor), TrackingMutexProtected::ReadOnlyAccessor>);
        static_assert(std::is_same_v<decltype(destination_accessor), TrackingIntMutexProtected::WriteEnabledAccessor>);
        ASSERT_EQ(LockCounts::acquisition_order.size(), 2);
        EXPECT_EQ(LockCounts::acquisition_order[0], source_is_first ? LockKind::Shared : LockKind::Unique);
        EXPECT_EQ(LockCounts::acquisition_order[1], source_is_first ? LockKind::Unique : LockKind::Shared);
        EXPECT_EQ(source_accessor->i, 5);
        ++*destination_accessor;
    }

    EXPECT_EQ(LockCounts::shared_locks, 0);
    EXPECT_EQ(LockCounts::unique_locks, 0);
    auto destination_value = destination.getReadOnly();
    EXPECT_EQ(*destination_value, 8);
}

TEST(MutexProtected, ExtractedAccessorOwnsTransferredLock)
{
    LockCounts::reset();
    TrackingMutexProtected source{A{5}};
    TrackingMutexProtected destination{A{7}};

    {
        auto && source_accessor
            = DB::LockOrderedAccessorPair(DB::readOnly(source), DB::writeEnabled(destination)).get<0>();
        EXPECT_EQ(source_accessor->i, 5);
        EXPECT_EQ(LockCounts::shared_locks, 1);
        EXPECT_EQ(LockCounts::unique_locks, 0);
    }

    EXPECT_EQ(LockCounts::shared_locks, 0);
    EXPECT_EQ(LockCounts::unique_locks, 0);
}

TEST(MutexProtected, LockOrderedAccessorPairSupportsConfigurableAccessModes)
{
    TrackingMutexProtected first{A{5}};
    TrackingMutexProtected second{A{7}};
    const bool first_is_first = std::less<const void *>{}(&first, &second);

    LockCounts::reset();
    {
        auto [first_accessor, second_accessor]
            = DB::LockOrderedAccessorPair(DB::readOnly(first), DB::readOnly(second));
        EXPECT_EQ(LockCounts::acquisition_order, (std::vector{LockKind::Shared, LockKind::Shared}));
        EXPECT_EQ(first_accessor->i, 5);
        EXPECT_EQ(second_accessor->i, 7);
    }

    LockCounts::reset();
    {
        auto [first_accessor, second_accessor]
            = DB::LockOrderedAccessorPair(DB::writeEnabled(first), DB::readOnly(second));
        ASSERT_EQ(LockCounts::acquisition_order.size(), 2);
        EXPECT_EQ(LockCounts::acquisition_order[0], first_is_first ? LockKind::Unique : LockKind::Shared);
        EXPECT_EQ(LockCounts::acquisition_order[1], first_is_first ? LockKind::Shared : LockKind::Unique);
        ++first_accessor->i;
        EXPECT_EQ(second_accessor->i, 7);
    }

    LockCounts::reset();
    {
        auto [first_accessor, second_accessor]
            = DB::LockOrderedAccessorPair(DB::writeEnabled(first), DB::writeEnabled(second));
        EXPECT_EQ(LockCounts::acquisition_order, (std::vector{LockKind::Unique, LockKind::Unique}));
        ++first_accessor->i;
        ++second_accessor->i;
    }

    auto first_value = first.getReadOnly();
    auto second_value = second.getReadOnly();
    EXPECT_EQ(first_value->i, 7);
    EXPECT_EQ(second_value->i, 8);
}

TEST(MutexProtected, LockOrderedAccessorPairRejectsSameObject)
{
    DB::MutexProtected<A> value{A{5}};
    EXPECT_THROW(
        static_cast<void>(DB::LockOrderedAccessorPair(DB::readOnly(value), DB::writeEnabled(value))),
        std::invalid_argument);
}

TEST(MutexProtected, SupportsExclusiveOnlyMutex)
{
    using StdMutexProtected = DB::MutexProtected<A, std::mutex, std::unique_lock, std::unique_lock>;
    StdMutexProtected a{A{5}};

    {
        auto rwa = a.getWriteEnabled();
        ++rwa->i;
    }

    {
        auto roa = a.getReadOnly();
        EXPECT_EQ(roa->i, 6);
    }
}
