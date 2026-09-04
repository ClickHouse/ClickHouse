#include <gtest/gtest.h>

#include <Common/MutexProtected.h>

#include <memory>
#include <mutex>
#include <stdexcept>
#include <type_traits>
#include <utility>

namespace
{

struct A
{
    explicit A(int i_) : i(i_) {}

    int i;
};

using ReadOnlyAccessor = decltype(std::declval<const DB::MutexProtected<A> &>().getReadOnly());
using WriteEnabledAccessor = decltype(std::declval<DB::MutexProtected<A> &>().getWriteEnabled());

static_assert(!std::is_copy_constructible_v<ReadOnlyAccessor>);
static_assert(!std::is_copy_assignable_v<ReadOnlyAccessor>);
static_assert(!std::is_move_constructible_v<ReadOnlyAccessor>);
static_assert(!std::is_move_assignable_v<ReadOnlyAccessor>);
static_assert(!std::is_copy_constructible_v<WriteEnabledAccessor>);
static_assert(!std::is_copy_assignable_v<WriteEnabledAccessor>);
static_assert(!std::is_move_constructible_v<WriteEnabledAccessor>);
static_assert(!std::is_move_assignable_v<WriteEnabledAccessor>);

template <class Mutex>
class NonBlockingSharedLock
{
public:
    explicit NonBlockingSharedLock(Mutex & mutex_)
        : mutex(mutex_)
    {
        if (!mutex.try_lock_shared())
            throw std::runtime_error("Cannot acquire shared lock");
    }

    ~NonBlockingSharedLock() noexcept
    {
        mutex.unlock_shared();
    }

    NonBlockingSharedLock(const NonBlockingSharedLock &) = delete;
    NonBlockingSharedLock & operator=(const NonBlockingSharedLock &) = delete;

private:
    Mutex & mutex;
};

template <class Mutex>
class NonBlockingUniqueLock
{
public:
    explicit NonBlockingUniqueLock(Mutex & mutex_)
        : mutex(mutex_)
    {
        if (!mutex.try_lock())
            throw std::runtime_error("Cannot acquire unique lock");
    }

    ~NonBlockingUniqueLock() noexcept
    {
        mutex.unlock();
    }

    NonBlockingUniqueLock(const NonBlockingUniqueLock &) = delete;
    NonBlockingUniqueLock & operator=(const NonBlockingUniqueLock &) = delete;

private:
    Mutex & mutex;
};

}

TEST(MutexProtected, AccessReadOnly)
{
    int i = 0;
    DB::MutexProtected<A> a{A{5}};

    a.accessReadOnly([&](const A * roa) { i = roa->i; });

    EXPECT_EQ(i, 5);
}

TEST(MutexProtected, AccessWriteEnabled)
{
    int i = 0;
    DB::MutexProtected<A> a{A{5}};

    a.accessWriteEnabled([&](A * rwa) { i = ++rwa->i; });

    EXPECT_EQ(i, 6);
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

TEST(MutexProtected, AccessReadOnlyWithNonDefaultLockGuard)
{
    int i = 0;
    DB::MutexProtected<A> a{A{5}};

    a.accessReadOnly<NonBlockingSharedLock>([&](const A * roa) { i = roa->i; });

    EXPECT_EQ(i, 5);
}

TEST(MutexProtected, AccessReadOnlyAcceptsMoveOnlyLvalueFunctor)
{
    DB::MutexProtected<A> a{A{5}};
    auto functor = [value = std::make_unique<int>(0)](const A * roa)
    {
        *value = roa->i;
        return *value;
    };

    EXPECT_EQ(a.accessReadOnly(functor), 5);
}

TEST(MutexProtected, AccessWriteEnabledWithNonDefaultLockGuard)
{
    int i = 0;
    DB::MutexProtected<A> a{A{5}};

    a.accessWriteEnabled<NonBlockingUniqueLock>([&](A * rwa) { i = ++rwa->i; });

    EXPECT_EQ(i, 6);
}

TEST(MutexProtected, GetReadOnlyWithNonDefaultLockGuard)
{
    int i = 0;
    DB::MutexProtected<A> a{A{5}};

    {
        auto roa = a.getReadOnly<NonBlockingSharedLock>();
        i = roa->i;
    }

    EXPECT_EQ(i, 5);
}

TEST(MutexProtected, GetWriteEnabledWithNonDefaultLockGuard)
{
    int i = 0;
    DB::MutexProtected<A> a{A{5}};

    {
        auto rwa = a.getWriteEnabled<NonBlockingUniqueLock>();
        i = ++rwa->i;
    }

    EXPECT_EQ(i, 6);
}

TEST(MutexProtected, AccessReadOnlyAcquiresSharedLock)
{
    bool read_only_access_lambda_evaluated = false;
    bool shared_lock_acquired = false;
    bool shared_lock_acquirable = true;
    bool unique_lock_lambda_evaluated = false;
    bool shared_lock_lambda_evaluated = false;
    DB::MutexProtected<A> a{A{5}};

    a.accessReadOnly([&](const A *)
    {
        read_only_access_lambda_evaluated = true;
        try
        {
            a.accessReadOnly<NonBlockingUniqueLock>(
                [&](const A *) { unique_lock_lambda_evaluated = true; });
        }
        catch (const std::runtime_error &)
        {
            shared_lock_acquired = true;
        }

        try
        {
            a.accessReadOnly<NonBlockingSharedLock>(
                [&](const A *) { shared_lock_lambda_evaluated = true; });
        }
        catch (const std::runtime_error &)
        {
            shared_lock_acquirable = false;
        }
    });

    EXPECT_TRUE(read_only_access_lambda_evaluated);
    EXPECT_TRUE(shared_lock_acquired);
    EXPECT_TRUE(shared_lock_acquirable);
    EXPECT_FALSE(unique_lock_lambda_evaluated);
    EXPECT_TRUE(shared_lock_lambda_evaluated);
}

TEST(MutexProtected, AccessWriteEnabledAcquiresExclusiveLock)
{
    bool write_enabled_access_lambda_evaluated = false;
    bool unique_lock_acquired = false;
    bool shared_lock_acquirable = true;
    bool unique_lock_lambda_evaluated = false;
    bool shared_lock_lambda_evaluated = false;
    DB::MutexProtected<A> a{A{5}};

    a.accessWriteEnabled([&](A *)
    {
        write_enabled_access_lambda_evaluated = true;
        try
        {
            a.accessReadOnly<NonBlockingUniqueLock>(
                [&](const A *) { unique_lock_lambda_evaluated = true; });
        }
        catch (const std::runtime_error &)
        {
            unique_lock_acquired = true;
        }

        try
        {
            a.accessReadOnly<NonBlockingSharedLock>(
                [&](const A *) { shared_lock_lambda_evaluated = true; });
        }
        catch (const std::runtime_error &)
        {
            shared_lock_acquirable = false;
        }
    });

    EXPECT_TRUE(write_enabled_access_lambda_evaluated);
    EXPECT_TRUE(unique_lock_acquired);
    EXPECT_FALSE(shared_lock_acquirable);
    EXPECT_FALSE(unique_lock_lambda_evaluated);
    EXPECT_FALSE(shared_lock_lambda_evaluated);
}

TEST(MutexProtected, GetReadOnlyAcquiresSharedLock)
{
    bool read_only_access_code_reached = false;
    bool shared_lock_acquired = false;
    bool shared_lock_acquirable = true;
    bool unique_lock_lambda_evaluated = false;
    bool shared_lock_lambda_evaluated = false;
    DB::MutexProtected<A> a{A{5}};

    {
        auto roa = a.getReadOnly();
        read_only_access_code_reached = true;
        EXPECT_EQ(roa->i, 5);

        try
        {
            a.accessReadOnly<NonBlockingUniqueLock>(
                [&](const A *) { unique_lock_lambda_evaluated = true; });
        }
        catch (const std::runtime_error &)
        {
            shared_lock_acquired = true;
        }

        try
        {
            a.accessReadOnly<NonBlockingSharedLock>(
                [&](const A *) { shared_lock_lambda_evaluated = true; });
        }
        catch (const std::runtime_error &)
        {
            shared_lock_acquirable = false;
        }
    }

    EXPECT_TRUE(read_only_access_code_reached);
    EXPECT_TRUE(shared_lock_acquired);
    EXPECT_TRUE(shared_lock_acquirable);
    EXPECT_FALSE(unique_lock_lambda_evaluated);
    EXPECT_TRUE(shared_lock_lambda_evaluated);
}

TEST(MutexProtected, GetWriteEnabledAcquiresExclusiveLock)
{
    bool write_enabled_access_code_reached = false;
    bool unique_lock_acquired = false;
    bool shared_lock_acquirable = true;
    bool unique_lock_lambda_evaluated = false;
    bool shared_lock_lambda_evaluated = false;
    DB::MutexProtected<A> a{A{5}};

    {
        auto rwa = a.getWriteEnabled();
        write_enabled_access_code_reached = true;
        EXPECT_EQ(rwa->i, 5);

        try
        {
            a.accessReadOnly<NonBlockingUniqueLock>(
                [&](const A *) { unique_lock_lambda_evaluated = true; });
        }
        catch (const std::runtime_error &)
        {
            unique_lock_acquired = true;
        }

        try
        {
            a.accessReadOnly<NonBlockingSharedLock>(
                [&](const A *) { shared_lock_lambda_evaluated = true; });
        }
        catch (const std::runtime_error &)
        {
            shared_lock_acquirable = false;
        }
    }

    EXPECT_TRUE(write_enabled_access_code_reached);
    EXPECT_TRUE(unique_lock_acquired);
    EXPECT_FALSE(shared_lock_acquirable);
    EXPECT_FALSE(unique_lock_lambda_evaluated);
    EXPECT_FALSE(shared_lock_lambda_evaluated);
}

TEST(MutexProtected, AccessReadOnlyWithNonDefaultLockGuardAcquiresLock)
{
    bool read_only_access_lambda_evaluated = false;
    bool unique_lock_acquired = false;
    bool shared_lock_acquirable = true;
    bool unique_lock_lambda_evaluated = false;
    bool shared_lock_lambda_evaluated = false;
    DB::MutexProtected<A> a{A{5}};

    a.accessReadOnly<NonBlockingUniqueLock>([&](const A *)
    {
        read_only_access_lambda_evaluated = true;
        try
        {
            a.accessReadOnly<NonBlockingUniqueLock>(
                [&](const A *) { unique_lock_lambda_evaluated = true; });
        }
        catch (const std::runtime_error &)
        {
            unique_lock_acquired = true;
        }

        try
        {
            a.accessReadOnly<NonBlockingSharedLock>(
                [&](const A *) { shared_lock_lambda_evaluated = true; });
        }
        catch (const std::runtime_error &)
        {
            shared_lock_acquirable = false;
        }
    });

    EXPECT_TRUE(read_only_access_lambda_evaluated);
    EXPECT_TRUE(unique_lock_acquired);
    EXPECT_FALSE(shared_lock_acquirable);
    EXPECT_FALSE(unique_lock_lambda_evaluated);
    EXPECT_FALSE(shared_lock_lambda_evaluated);
}

TEST(MutexProtected, AccessWriteEnabledWithNonDefaultLockGuardAcquiresLock)
{
    bool write_enabled_access_lambda_evaluated = false;
    bool unique_lock_acquired = false;
    bool shared_lock_acquirable = true;
    bool unique_lock_lambda_evaluated = false;
    bool shared_lock_lambda_evaluated = false;
    DB::MutexProtected<A> a{A{5}};

    a.accessWriteEnabled<NonBlockingUniqueLock>([&](A *)
    {
        write_enabled_access_lambda_evaluated = true;
        try
        {
            a.accessReadOnly<NonBlockingUniqueLock>(
                [&](const A *) { unique_lock_lambda_evaluated = true; });
        }
        catch (const std::runtime_error &)
        {
            unique_lock_acquired = true;
        }

        try
        {
            a.accessReadOnly<NonBlockingSharedLock>(
                [&](const A *) { shared_lock_lambda_evaluated = true; });
        }
        catch (const std::runtime_error &)
        {
            shared_lock_acquirable = false;
        }
    });

    EXPECT_TRUE(write_enabled_access_lambda_evaluated);
    EXPECT_TRUE(unique_lock_acquired);
    EXPECT_FALSE(shared_lock_acquirable);
    EXPECT_FALSE(unique_lock_lambda_evaluated);
    EXPECT_FALSE(shared_lock_lambda_evaluated);
}

TEST(MutexProtected, GetReadOnlyWithNonDefaultLockGuardAcquiresLock)
{
    bool read_only_access_code_reached = false;
    bool unique_lock_acquired = false;
    bool shared_lock_acquirable = true;
    bool unique_lock_lambda_evaluated = false;
    bool shared_lock_lambda_evaluated = false;
    DB::MutexProtected<A> a{A{5}};

    {
        auto roa = a.getReadOnly<NonBlockingUniqueLock>();
        read_only_access_code_reached = true;
        EXPECT_EQ(roa->i, 5);

        try
        {
            a.accessReadOnly<NonBlockingUniqueLock>(
                [&](const A *) { unique_lock_lambda_evaluated = true; });
        }
        catch (const std::runtime_error &)
        {
            unique_lock_acquired = true;
        }

        try
        {
            a.accessReadOnly<NonBlockingSharedLock>(
                [&](const A *) { shared_lock_lambda_evaluated = true; });
        }
        catch (const std::runtime_error &)
        {
            shared_lock_acquirable = false;
        }
    }

    EXPECT_TRUE(read_only_access_code_reached);
    EXPECT_TRUE(unique_lock_acquired);
    EXPECT_FALSE(shared_lock_acquirable);
    EXPECT_FALSE(unique_lock_lambda_evaluated);
    EXPECT_FALSE(shared_lock_lambda_evaluated);
}

TEST(MutexProtected, GetWriteEnabledWithNonDefaultLockGuardAcquiresLock)
{
    bool write_enabled_access_code_reached = false;
    bool unique_lock_acquired = false;
    bool shared_lock_acquirable = true;
    bool unique_lock_lambda_evaluated = false;
    bool shared_lock_lambda_evaluated = false;
    DB::MutexProtected<A> a{A{5}};

    {
        auto rwa = a.getWriteEnabled<NonBlockingUniqueLock>();
        write_enabled_access_code_reached = true;
        EXPECT_EQ(rwa->i, 5);

        try
        {
            a.accessReadOnly<NonBlockingUniqueLock>(
                [&](const A *) { unique_lock_lambda_evaluated = true; });
        }
        catch (const std::runtime_error &)
        {
            unique_lock_acquired = true;
        }

        try
        {
            a.accessReadOnly<NonBlockingSharedLock>(
                [&](const A *) { shared_lock_lambda_evaluated = true; });
        }
        catch (const std::runtime_error &)
        {
            shared_lock_acquirable = false;
        }
    }

    EXPECT_TRUE(write_enabled_access_code_reached);
    EXPECT_TRUE(unique_lock_acquired);
    EXPECT_FALSE(shared_lock_acquirable);
    EXPECT_FALSE(unique_lock_lambda_evaluated);
    EXPECT_FALSE(shared_lock_lambda_evaluated);
}

TEST(MutexProtected, AccessWriteEnabledWithStdMutexAcquiresLock)
{
    bool write_enabled_access_lambda_evaluated = false;
    bool unique_lock_acquired = false;
    bool unique_lock_acquirable = true;
    bool unique_lock_lambda_evaluated = false;
    bool second_unique_lock_lambda_evaluated = false;
    DB::MutexProtected<A, std::mutex> a{A{5}};

    a.accessWriteEnabled<NonBlockingUniqueLock>([&](A *)
    {
        write_enabled_access_lambda_evaluated = true;
        try
        {
            a.accessReadOnly<NonBlockingUniqueLock>(
                [&](const A *) { unique_lock_lambda_evaluated = true; });
        }
        catch (const std::runtime_error &)
        {
            unique_lock_acquired = true;
        }

        try
        {
            a.accessReadOnly<NonBlockingUniqueLock>(
                [&](const A *) { second_unique_lock_lambda_evaluated = true; });
        }
        catch (const std::runtime_error &)
        {
            unique_lock_acquirable = false;
        }
    });

    EXPECT_TRUE(write_enabled_access_lambda_evaluated);
    EXPECT_TRUE(unique_lock_acquired);
    EXPECT_FALSE(unique_lock_acquirable);
    EXPECT_FALSE(unique_lock_lambda_evaluated);
    EXPECT_FALSE(second_unique_lock_lambda_evaluated);
}

TEST(MutexProtected, GetWriteEnabledWithStdMutexAcquiresLock)
{
    bool write_enabled_access_code_reached = false;
    bool unique_lock_acquired = false;
    bool unique_lock_acquirable = true;
    bool unique_lock_lambda_evaluated = false;
    bool second_unique_lock_lambda_evaluated = false;
    DB::MutexProtected<A, std::mutex> a{A{5}};

    {
        auto rwa = a.getWriteEnabled<NonBlockingUniqueLock>();
        write_enabled_access_code_reached = true;
        EXPECT_EQ(rwa->i, 5);

        try
        {
            a.accessReadOnly<NonBlockingUniqueLock>(
                [&](const A *) { unique_lock_lambda_evaluated = true; });
        }
        catch (const std::runtime_error &)
        {
            unique_lock_acquired = true;
        }

        try
        {
            a.accessReadOnly<NonBlockingUniqueLock>(
                [&](const A *) { second_unique_lock_lambda_evaluated = true; });
        }
        catch (const std::runtime_error &)
        {
            unique_lock_acquirable = false;
        }
    }

    EXPECT_TRUE(write_enabled_access_code_reached);
    EXPECT_TRUE(unique_lock_acquired);
    EXPECT_FALSE(unique_lock_acquirable);
    EXPECT_FALSE(unique_lock_lambda_evaluated);
    EXPECT_FALSE(second_unique_lock_lambda_evaluated);
}
