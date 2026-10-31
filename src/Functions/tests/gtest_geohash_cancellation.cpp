#include <gtest/gtest.h>

#include <Functions/CancellationBudget.h>
#include <Functions/GeoHash.h>

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <functional>
#include <stdexcept>
#include <vector>


namespace
{

/// The base32 alphabet geohashes are written in holds no NUL, so in a buffer filled with '\0' the count of
/// leading non-NUL bytes is the number of bytes the expansion has written so far.
size_t bytesWritten(const std::vector<char> & out)
{
    return static_cast<size_t>(std::find(out.begin(), out.end(), '\0') - out.begin());
}

/// A box of 300 x 300 grid cells at the finest precision: more than one budget interval, and small enough
/// to expand in milliseconds.
DB::GeohashesInBoxPreparedArgs prepare90000()
{
    return DB::geohashesInBoxPrepare(
        0.0, 0.0, 300.0 * 360.0 / std::pow(2.0, 30), 300.0 * 180.0 / std::pow(2.0, 30), static_cast<uint8_t>(12));
}

}

TEST(GeohashesInBoxCancellation, ChecksCancellationDuringExpansion)
{
    const DB::GeohashesInBoxPreparedArgs args = prepare90000();
    ASSERT_EQ(args.items_count, 90000u);

    std::vector<char> out(args.items_count * args.precision, '\0');

    size_t checks = 0;
    size_t bytes_at_check = 0;
    const std::function<void()> check = [&]
    {
        ++checks;
        bytes_at_check = bytesWritten(out);
        throw std::runtime_error("cancelled");
    };
    DB::CancellationBudget budget(check);

    EXPECT_THROW(DB::geohashesInBox(args, out.data(), budget), std::runtime_error);
    EXPECT_EQ(checks, 1u);
    /// The check runs before the encode whose charge triggered it, so the interval's last encode is the one
    /// that did not happen. Written symbolically: the interval width is not part of what is being asserted.
    EXPECT_EQ(bytes_at_check, (DB::CancellationBudget::units_per_check - 1) * args.precision);
    EXPECT_LT(bytes_at_check, args.items_count * args.precision);
}

TEST(GeohashesInBoxCancellation, InvalidArgumentsExitChargesOneUnit)
{
    /// Reversed bounds prepare an empty box, whose zero precision takes the guard exit.
    const DB::GeohashesInBoxPreparedArgs args = DB::geohashesInBoxPrepare(1.0, 1.0, 0.0, 0.0, static_cast<uint8_t>(4));
    ASSERT_EQ(args.precision, 0u);

    size_t checks = 0;
    const std::function<void()> check = [&] { ++checks; };
    DB::CancellationBudget budget(check);

    std::vector<char> out(64, '\0');
    size_t unexpected_results = 0;
    for (size_t i = 0; i < DB::CancellationBudget::units_per_check; ++i)
        if (DB::geohashesInBox(args, out.data(), budget) != 0)
            ++unexpected_results;

    EXPECT_EQ(unexpected_results, 0u);
    EXPECT_EQ(checks, 1u);
}

TEST(GeohashesInBoxCancellation, ZeroItemFallbackChargesOneUnit)
{
    /// A point box holds zero grid cells on both axes, so the loops emit nothing and the fallback runs.
    const DB::GeohashesInBoxPreparedArgs args = DB::geohashesInBoxPrepare(0.0, 0.0, 0.0, 0.0, static_cast<uint8_t>(12));
    ASSERT_EQ(args.items_count, 1u);
    ASSERT_EQ(args.longitude_items, 0u);
    ASSERT_EQ(args.latitude_items, 0u);

    size_t checks = 0;
    const std::function<void()> check = [&] { ++checks; };
    DB::CancellationBudget budget(check);

    std::vector<char> out(args.precision, '\0');
    size_t unexpected_results = 0;
    for (size_t i = 0; i < DB::CancellationBudget::units_per_check; ++i)
        if (DB::geohashesInBox(args, out.data(), budget) != 1)
            ++unexpected_results;

    EXPECT_EQ(unexpected_results, 0u);
    EXPECT_EQ(checks, 1u);
}

TEST(GeohashesInBoxCancellation, EmptyCheckIsANoOp)
{
    const DB::GeohashesInBoxPreparedArgs args = prepare90000();
    ASSERT_EQ(args.items_count, 90000u);

    /// There is no query to observe when a background merge or a `clickhouse-local` invocation with no
    /// process list element evaluates the function.
    const std::function<void()> no_check;
    DB::CancellationBudget budget(no_check);

    std::vector<char> out(args.items_count * args.precision, '\0');
    EXPECT_EQ(DB::geohashesInBox(args, out.data(), budget), args.items_count);
    EXPECT_EQ(bytesWritten(out), args.items_count * args.precision);
}
