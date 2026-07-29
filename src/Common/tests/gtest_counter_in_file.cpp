#include <gtest/gtest.h>

#include <sys/mman.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <new>
#include <limits>
#include <stdexcept>
#include <string>

/// CounterInFile.h uses DB::readIntText / writeIntText / readChar / writeChar
/// but does not include their declarations (it relies on transitive includes
/// from its production caller). Pull them in so this standalone TU compiles.
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/CounterInFile.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>

namespace ProfileEvents
{
    extern const Event WriteBufferFromFileDescriptorWrite;
}

namespace fs = std::filesystem;

namespace
{

/// Unique scratch path per test so parallel runs do not collide.
fs::path makeScratchDir(const std::string & name)
{
    fs::path dir = fs::temp_directory_path() / ("gtest_counter_in_file_" + name + "_" + std::to_string(::getpid()));
    fs::remove_all(dir);
    fs::create_directories(dir);
    return dir;
}

/// True when `add` accepts a recovery lower bound and its lazy provider. The
/// checks that name those parameters are gated on this, so the file still
/// compiles against a tree that predates them.
template <typename Counter>
concept HasRecoveryBound = requires (Counter & counter)
{
    counter.add(Int64{1}, true, Int64{0}, std::function<Int64()>{});
};

}

/// create_if_need == false on a MISSING file must throw and must NOT create the
/// file. A failed probe through this shared helper must leave no counter state
/// behind (regression guard for the non-creating contract).
TEST(CounterInFile, MissingFileNoCreateThrowsAndLeavesNoFile)
{
    fs::path dir = makeScratchDir("missing_no_create");
    fs::path path = dir / "counter.txt";
    ASSERT_FALSE(fs::exists(path));

    CounterInFile counter(path.string());
    EXPECT_ANY_THROW(counter.add(1, /*create_if_need=*/false));
    EXPECT_FALSE(fs::exists(path)) << "probe with create_if_need=false must not create the file";

    fs::remove_all(dir);
}

/// create_if_need == false on an existing EMPTY file must throw (and the file
/// stays empty - the probe must not turn it into persistent counter state).
TEST(CounterInFile, EmptyFileNoCreateThrows)
{
    fs::path dir = makeScratchDir("empty_no_create");
    fs::path path = dir / "counter.txt";
    { std::ofstream ofs(path); } // create a zero-length file
    ASSERT_TRUE(fs::exists(path));
    ASSERT_EQ(fs::file_size(path), 0u);

    CounterInFile counter(path.string());
    EXPECT_ANY_THROW(counter.add(1, /*create_if_need=*/false));
    EXPECT_EQ(fs::file_size(path), 0u) << "rejected empty file must stay empty";

    fs::remove_all(dir);
}

/// create_if_need == true must self-heal a MISSING file: treat it as zero,
/// create it, and return the incremented value.
TEST(CounterInFile, MissingFileCreateRecovers)
{
    fs::path dir = makeScratchDir("missing_create");
    fs::path path = dir / "counter.txt";
    ASSERT_FALSE(fs::exists(path));

    CounterInFile counter(path.string());
    EXPECT_EQ(counter.add(1, /*create_if_need=*/true), 1);
    EXPECT_TRUE(fs::exists(path));
    EXPECT_EQ(counter.add(1, /*create_if_need=*/true), 2);

    fs::remove_all(dir);
}

/// create_if_need == true must self-heal an existing EMPTY file rather than
/// failing forever: this is the permanent-FREEZE-failure symptom of the issue.
/// The file state is decided from its size under the lock, so a zero-length file
/// left behind by an interrupted writer is treated like a missing one.
TEST(CounterInFile, EmptyFileCreateRecovers)
{
    fs::path dir = makeScratchDir("empty_create");
    fs::path path = dir / "counter.txt";
    { std::ofstream ofs(path); }
    ASSERT_TRUE(fs::exists(path));
    ASSERT_EQ(fs::file_size(path), 0u);

    CounterInFile counter(path.string());
    EXPECT_EQ(counter.add(1, /*create_if_need=*/true), 1);
    EXPECT_GT(fs::file_size(path), 0u);

    fs::remove_all(dir);
}

/// Concurrent first creators must get distinct values, or two backups share one
/// identifier. Fork because `flock` is per process; the barrier and the rounds are
/// what make the losing interleaving likely enough to catch.
TEST(CounterInFile, ConcurrentFirstCreatorsGetDistinctValues)
{
    fs::path dir = makeScratchDir("concurrent_first_create");
    fs::path path = dir / "counter.txt";
    ASSERT_FALSE(fs::exists(path));

    constexpr unsigned rounds = 24;

    /// Shared with the child across fork, so both sides can meet before each round.
    /// Monotonic: every participant adds one, so round r ends at 2 * (r + 1).
    void * shared = ::mmap(nullptr, sizeof(std::atomic<unsigned>), PROT_READ | PROT_WRITE,
                           MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    ASSERT_NE(shared, MAP_FAILED);
    auto * arrived = new (shared) std::atomic<unsigned>(0);

    auto meet = [&](unsigned round)
    {
        const unsigned target = 2 * (round + 1);
        arrived->fetch_add(1, std::memory_order_acq_rel);
        /// Bounded, so a died peer cannot hang the suite.
        for (unsigned spins = 0; arrived->load(std::memory_order_acquire) < target && spins < 20'000'000; ++spins)
            ::sched_yield();
    };

    int fds[2];
    ASSERT_EQ(::pipe(fds), 0);

    const pid_t pid = ::fork();
    ASSERT_NE(pid, -1);
    if (pid == 0)
    {
        ::close(fds[0]);
        for (unsigned round = 0; round < rounds; ++round)
        {
            meet(round);
            Int64 value = -1;
            try
            {
                CounterInFile counter(path.string());
                value = counter.add(1, /*create_if_need=*/true);
            }
            catch (...)
            {
                /// A forked child cannot report gtest failures, so the cause must be printed.
                value = -1;
                std::cerr << "child round " << round << ": " << DB::getCurrentExceptionMessage(true) << "\n";
            }
            if (::write(fds[1], &value, sizeof(value)) != static_cast<ssize_t>(sizeof(value)))
                break;
        }
        ::close(fds[1]);
        ::_exit(0);
    }

    ::close(fds[1]);

    for (unsigned round = 0; round < rounds; ++round)
    {
        /// Safe to reset here: the previous round's child value was already read, so the
        /// child is past its own call.
        fs::remove(path);

        meet(round);
        Int64 here = -1;
        std::string here_error;
        try
        {
            CounterInFile counter(path.string());
            here = counter.add(1, /*create_if_need=*/true);
        }
        catch (...)
        {
            here = -1;
            here_error = DB::getCurrentExceptionMessage(true);
        }

        Int64 there = -1;
        ASSERT_EQ(::read(fds[0], &there, sizeof(there)), static_cast<ssize_t>(sizeof(there)))
            << "the child did not report a value in round " << round;

        ASSERT_GT(here, 0) << "round " << round << ": " << here_error;
        ASSERT_GT(there, 0) << "round " << round << ": the child reported no value, see its stderr";
        ASSERT_NE(here, there)
            << "round " << round << ": concurrent first creators were both handed " << here
            << ", so two backups would share one identifier";
        /// And nothing was lost: the two allocations are exactly the first two.
        ASSERT_EQ(std::min(here, there), 1) << "round " << round;
        ASSERT_EQ(std::max(here, there), 2) << "round " << round;
    }

    ::close(fds[0]);
    int status = 0;
    ASSERT_EQ(::waitpid(pid, &status, 0), pid);
    ::munmap(shared, sizeof(std::atomic<unsigned>));

    fs::remove_all(dir);
}

/// The empty-file recovery must not disturb the ordinary path: a counter that
/// holds a value is advanced from it, never reset, and a zero delta only reads.
TEST(CounterInFile, HealthyFileIsAdvancedNotReset)
{
    fs::path dir = makeScratchDir("healthy");
    fs::path path = dir / "counter.txt";
    { std::ofstream ofs(path); ofs << "5\n"; }

    CounterInFile counter(path.string());
    EXPECT_EQ(counter.add(1, /*create_if_need=*/true), 6);
    EXPECT_EQ(counter.add(1, /*create_if_need=*/true), 7);
    EXPECT_EQ(counter.add(0, /*create_if_need=*/true), 7) << "a zero delta must not change the counter";

    fs::remove_all(dir);
}

/// The widest record must reach the file in ONE write, or an interrupted writer
/// leaves digits that read back as a smaller valid number. Count the syscalls the
/// counter's own writer issues: the final contents are the same either way, so they
/// cannot tell the two buffer sizes apart.
TEST(CounterInFile, WidestValueAndNewlineTakeOneWrite)
{
    fs::path dir = makeScratchDir("widest_one_write");
    fs::path path = dir / "counter.txt";

    /// The minimum is the widest value, one character longer than the maximum
    /// because of its sign, so a buffer sized for the maximum is still one byte short.
    const Int64 widest = std::numeric_limits<Int64>::min(); // 20 characters

    /// Start one above the minimum and step down onto it, so the counter itself
    /// writes the widest record rather than only reading one written here.
    {
        std::ofstream ofs(path);
        ofs << (widest + 1) << "\n";
    }

    /// WriteBufferFromFileDescriptor::nextImpl counts one event per ::write, so the
    /// delta over a single add() is the number of syscalls that record took.
    const ProfileEvents::Count writes_before
        = ProfileEvents::global_counters[ProfileEvents::WriteBufferFromFileDescriptorWrite];

    CounterInFile counter(path.string());
    EXPECT_EQ(counter.add(-1, /*create_if_need=*/true), widest)
        << "the counter must write and read back the widest value";

    const ProfileEvents::Count writes
        = ProfileEvents::global_counters[ProfileEvents::WriteBufferFromFileDescriptorWrite] - writes_before;

    /// With a 16-byte buffer this is 2.
    EXPECT_EQ(writes, 1u)
        << "the widest value and its newline must reach the file in one write, or an "
           "interrupted writer can leave a truncated value that reads back as a smaller number";

    /// And the record itself is complete, so the counter round-trips.
    std::string contents;
    {
        std::ifstream ifs(path);
        std::getline(ifs, contents);
    }
    EXPECT_EQ(contents, std::to_string(widest))
        << "the counter left a truncated record on disk";

    CounterInFile reopened(path.string());
    EXPECT_EQ(reopened.add(0, /*create_if_need=*/true), widest);

    fs::remove_all(dir);
}

/// A hand-written counter need not end in a newline (the class tells operators to
/// create the file manually), so it must be advanced rather than reset.
TEST(CounterInFile, ManuallyWrittenValueWithoutNewlineIsUsedAsIs)
{
    fs::path dir = makeScratchDir("manual_no_newline");
    fs::path path = dir / "counter.txt";
    { std::ofstream ofs(path); ofs << "5"; } // no trailing newline

    CounterInFile counter(path.string());
    if constexpr (HasRecoveryBound<CounterInFile>)
        /// A recovery bound must be ignored here: the file is healthy, just terse.
        EXPECT_EQ(counter.add(1, /*create_if_need=*/true, /*min_initial_value=*/41), 6)
            << "a newline-less hand-written value must be advanced, not recovered";
    else
        EXPECT_EQ(counter.add(1, /*create_if_need=*/true), 6);

    fs::remove_all(dir);
}

/// A counter parked at the Int64 maximum has no next value: advancing it must
/// throw rather than overflow into a negative number, and must leave the stored
/// value untouched so the state stays diagnosable.
TEST(CounterInFile, ExhaustedCounterRefusesToOverflow)
{
    fs::path dir = makeScratchDir("exhausted");
    fs::path path = dir / "counter.txt";
    { std::ofstream ofs(path); ofs << std::numeric_limits<Int64>::max() << "\n"; }

    CounterInFile counter(path.string());
    EXPECT_ANY_THROW(counter.add(1, /*create_if_need=*/true));

    // A zero delta only reads, so it still reports the maximum.
    EXPECT_EQ(counter.add(0, /*create_if_need=*/true), std::numeric_limits<Int64>::max());

    fs::remove_all(dir);
}

/// min_initial_value sets a lower bound only when the file was missing/empty;
/// it must NOT clamp a healthy existing counter.
template <typename Counter>
void checkMinInitialValueAppliesOnlyOnRecovery(const fs::path & dir)
{
    if constexpr (!HasRecoveryBound<Counter>)
        GTEST_SKIP() << "add() takes no recovery lower bound in this tree";
    else
    {
        // Recovery from empty: starting point is min_initial_value, result is +delta.
        {
            fs::path path = dir / "recover.txt";
            { std::ofstream ofs(path); }
            Counter counter(path.string());
            EXPECT_EQ(counter.add(1, /*create_if_need=*/true, /*min_initial_value=*/41), 42);
        }

        // Recovery from a MISSING file, not just an empty one. Both states reach the
        // same branch (`O_CREAT` then a zero size under the lock), but only the
        // empty one was covered, so a regression that recovered from zero when the
        // file is absent would have been invisible - and beside an existing
        // `shadow/<N>` that means handing out an identifier again.
        {
            fs::path path = dir / "recover_missing.txt";
            ASSERT_FALSE(fs::exists(path));
            Counter counter(path.string());
            EXPECT_EQ(counter.add(1, /*create_if_need=*/true, /*min_initial_value=*/41), 42);
        }

        // Healthy counter is used as-is and is never clamped down or up.
        {
            fs::path path = dir / "healthy.txt";
            { std::ofstream ofs(path); ofs << "5\n"; }
            Counter counter(path.string());
            EXPECT_EQ(counter.add(1, /*create_if_need=*/true, /*min_initial_value=*/100), 6);
        }
    }
}

TEST(CounterInFile, MinInitialValueAppliesOnlyOnRecovery)
{
    fs::path dir = makeScratchDir("min_initial");
    checkMinInitialValueAppliesOnlyOnRecovery<CounterInFile>(dir);
    fs::remove_all(dir);
}

/// The provider supplies an additional lower bound on recovery, combined with
/// the static min_initial_value via max(). It is evaluated only on the recovery
/// path (missing/empty file), never on a healthy counter.
template <typename Counter>
void checkMinInitialValueProviderEvaluatedOnlyOnRecovery(const fs::path & dir)
{
    if constexpr (!HasRecoveryBound<Counter>)
        GTEST_SKIP() << "add() takes no recovery lower bound in this tree";
    else
    {
        // Recovery from empty: provider's value wins over the smaller static bound.
        {
            fs::path path = dir / "recover_provider.txt";
            { std::ofstream ofs(path); }
            Counter counter(path.string());
            bool called = false;
            Int64 res = counter.add(
                1, /*create_if_need=*/true, /*min_initial_value=*/7,
                [&]() -> Int64 { called = true; return 41; });
            EXPECT_TRUE(called) << "provider must be evaluated on the recovery path";
            EXPECT_EQ(res, 42) << "max(7, 41) + 1";
        }

        // The same recovery, from a MISSING file rather than an empty one.
        {
            fs::path path = dir / "recover_provider_missing.txt";
            ASSERT_FALSE(fs::exists(path));
            Counter counter(path.string());
            bool called = false;
            Int64 res = counter.add(
                1, /*create_if_need=*/true, /*min_initial_value=*/7,
                [&]() -> Int64 { called = true; return 41; });
            EXPECT_TRUE(called) << "provider must be evaluated when the file is missing";
            EXPECT_EQ(res, 42) << "max(7, 41) + 1";
        }

        // Recovery from empty: the larger static bound wins over the provider.
        {
            fs::path path = dir / "recover_static.txt";
            { std::ofstream ofs(path); }
            Counter counter(path.string());
            Int64 res = counter.add(
                1, /*create_if_need=*/true, /*min_initial_value=*/100,
                [&]() -> Int64 { return 5; });
            EXPECT_EQ(res, 101) << "max(100, 5) + 1";
        }

        // Healthy counter: the provider must NOT be evaluated (the bound is unused),
        // and the existing value is returned untouched.
        {
            fs::path path = dir / "healthy_provider.txt";
            { std::ofstream ofs(path); ofs << "5\n"; }
            Counter counter(path.string());
            bool called = false;
            Int64 res = counter.add(
                1, /*create_if_need=*/true, /*min_initial_value=*/0,
                [&]() -> Int64 { called = true; return 1000; });
            EXPECT_FALSE(called) << "provider must not be evaluated for a healthy counter";
            EXPECT_EQ(res, 6);
        }
    }
}

TEST(CounterInFile, MinInitialValueProviderEvaluatedOnlyOnRecovery)
{
    fs::path dir = makeScratchDir("min_initial_provider");
    checkMinInitialValueProviderEvaluatedOnlyOnRecovery<CounterInFile>(dir);
    fs::remove_all(dir);
}

/// A throw from the provider aborts the call (the FREEZE fails) and the counter
/// file is not written: a recovery that cannot establish a safe lower bound
/// must not silently advance the counter.
template <typename Counter>
void checkMinInitialValueProviderThrowAbortsWithoutWrite(const fs::path & dir)
{
    if constexpr (!HasRecoveryBound<Counter>)
        GTEST_SKIP() << "add() takes no recovery lower bound in this tree";
    else
    {
        fs::path path = dir / "counter.txt";
        { std::ofstream ofs(path); }
        ASSERT_TRUE(fs::exists(path));
        ASSERT_EQ(fs::file_size(path), 0u);

        Counter counter(path.string());
        EXPECT_ANY_THROW(counter.add(
            1, /*create_if_need=*/true, /*min_initial_value=*/0,
            [&]() -> Int64 { throw std::runtime_error("scan failed"); }));

        // The counter file must remain empty: the aborted recovery wrote nothing.
        EXPECT_EQ(fs::file_size(path), 0u) << "counter must not advance when the provider throws";
    }
}

TEST(CounterInFile, MinInitialValueProviderThrowAbortsWithoutWrite)
{
    fs::path dir = makeScratchDir("min_initial_provider_throw");
    checkMinInitialValueProviderThrowAbortsWithoutWrite<CounterInFile>(dir);
    fs::remove_all(dir);
}
