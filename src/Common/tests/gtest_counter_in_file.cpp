#include <gtest/gtest.h>

#include <unistd.h>

#include <filesystem>
#include <fstream>
#include <stdexcept>

/// CounterInFile.h uses DB::readIntText / writeIntText / readChar / writeChar
/// but does not include their declarations (it relies on transitive includes
/// from its production caller). Pull them in so this standalone TU compiles.
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/CounterInFile.h>

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
/// create it, and return the incremented value (core of the issue #105719 fix).
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
/// failing forever (the permanent-FREEZE-failure symptom from issue #105719).
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

/// min_initial_value sets a lower bound only when the file was missing/empty;
/// it must NOT clamp a healthy existing counter.
TEST(CounterInFile, MinInitialValueAppliesOnlyOnRecovery)
{
    fs::path dir = makeScratchDir("min_initial");

    // Recovery from empty: starting point is min_initial_value, result is +delta.
    {
        fs::path path = dir / "recover.txt";
        { std::ofstream ofs(path); }
        CounterInFile counter(path.string());
        EXPECT_EQ(counter.add(1, /*create_if_need=*/true, /*min_initial_value=*/41), 42);
    }

    // Healthy counter is used as-is and is never clamped down or up.
    {
        fs::path path = dir / "healthy.txt";
        { std::ofstream ofs(path); ofs << "5\n"; }
        CounterInFile counter(path.string());
        EXPECT_EQ(counter.add(1, /*create_if_need=*/true, /*min_initial_value=*/100), 6);
    }

    fs::remove_all(dir);
}

/// The provider supplies an additional lower bound on recovery, combined with
/// the static min_initial_value via max(). It is evaluated only on the recovery
/// path (missing/empty file), never on a healthy counter.
TEST(CounterInFile, MinInitialValueProviderEvaluatedOnlyOnRecovery)
{
    fs::path dir = makeScratchDir("min_initial_provider");

    // Recovery from empty: provider's value wins over the smaller static bound.
    {
        fs::path path = dir / "recover_provider.txt";
        { std::ofstream ofs(path); }
        CounterInFile counter(path.string());
        bool called = false;
        Int64 res = counter.add(
            1, /*create_if_need=*/true, /*min_initial_value=*/7,
            [&]() -> Int64 { called = true; return 41; });
        EXPECT_TRUE(called) << "provider must be evaluated on the recovery path";
        EXPECT_EQ(res, 42) << "max(7, 41) + 1";
    }

    // Recovery from empty: the larger static bound wins over the provider.
    {
        fs::path path = dir / "recover_static.txt";
        { std::ofstream ofs(path); }
        CounterInFile counter(path.string());
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
        CounterInFile counter(path.string());
        bool called = false;
        Int64 res = counter.add(
            1, /*create_if_need=*/true, /*min_initial_value=*/0,
            [&]() -> Int64 { called = true; return 1000; });
        EXPECT_FALSE(called) << "provider must not be evaluated for a healthy counter";
        EXPECT_EQ(res, 6);
    }

    fs::remove_all(dir);
}

/// A throw from the provider aborts the call (the FREEZE fails) and the counter
/// file is not written: a recovery that cannot establish a safe lower bound
/// must not silently advance the counter.
TEST(CounterInFile, MinInitialValueProviderThrowAbortsWithoutWrite)
{
    fs::path dir = makeScratchDir("min_initial_provider_throw");
    fs::path path = dir / "counter.txt";
    { std::ofstream ofs(path); }
    ASSERT_TRUE(fs::exists(path));
    ASSERT_EQ(fs::file_size(path), 0u);

    CounterInFile counter(path.string());
    EXPECT_ANY_THROW(counter.add(
        1, /*create_if_need=*/true, /*min_initial_value=*/0,
        [&]() -> Int64 { throw std::runtime_error("scan failed"); }));

    // The counter file must remain empty: the aborted recovery wrote nothing.
    EXPECT_EQ(fs::file_size(path), 0u) << "counter must not advance when the provider throws";

    fs::remove_all(dir);
}
