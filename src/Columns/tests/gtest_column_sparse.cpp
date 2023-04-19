#include <Columns/ColumnSparse.h>
#include <Columns/ColumnsNumber.h>

#include <Common/randomSeed.h>
#include <pcg_random.hpp>
#include <gtest/gtest.h>

#include <algorithm>
#include <numeric>

#include <Common/FieldVisitors.h>

using namespace DB;
static pcg64 rng(randomSeed());

std::pair<MutableColumnPtr, MutableColumnPtr> createColumns(size_t n, size_t k)
{
    auto values = ColumnVector<UInt64>::create();
    auto offsets = ColumnVector<UInt64>::create();
    auto full = ColumnVector<UInt64>::create();

    auto & values_data = values->getData();
    auto & offsets_data = offsets->getData();
    auto & full_data = full->getData();

    values_data.push_back(0);

    for (size_t i = 0; i < n; ++i)
    {
        bool not_zero = rng() % k == 0;
        size_t value = not_zero ? rng() % 1000000 : 0;
        full_data.push_back(value);

        if (not_zero)
        {
            values_data.push_back(value);
            offsets_data.push_back(i);
        }
    }

    auto sparse = ColumnSparse::create(std::move(values), std::move(offsets), n);
    return std::make_pair(std::move(sparse), std::move(full));
}

bool checkEquals(const IColumn & lhs, const IColumn & rhs)
{
    if (lhs.size() != rhs.size())
        return false;

    for (size_t i = 0; i < lhs.size(); ++i)
        if (lhs.compareAt(i, i, rhs, 0) != 0)
            return false;

    return true;
}

// Can't use ErrorCodes, because of 'using namespace DB'.
constexpr int error_code = 12345;

constexpr size_t T = 5000;
constexpr size_t MAX_ROWS = 10000;
constexpr size_t sparse_ratios[] = {1, 2, 5, 10, 32, 50, 64, 100, 256, 500, 1000, 5000, 10000};
constexpr size_t K = sizeof(sparse_ratios) / sizeof(sparse_ratios[0]);

#define DUMP_COLUMN(column) std::cerr << #column << ": " << (column)->dumpStructure() << "\n"

TEST(ColumnSparse, InsertRangeFrom)
{
    auto test_case = [&](size_t n1, size_t k1, size_t n2, size_t k2, size_t from, size_t len)
    {
        auto [sparse_dst, full_dst] = createColumns(n1, k1);
        auto [sparse_src, full_src] = createColumns(n2, k2);

        sparse_dst->insertRangeFrom(*sparse_src, from, len);
        full_dst->insertRangeFrom(*full_src, from, len);

        if (!checkEquals(*sparse_dst->convertToFullColumnIfSparse(), *full_dst))
        {
            DUMP_COLUMN(sparse_src);
            DUMP_COLUMN(full_src);
            DUMP_COLUMN(sparse_dst);
            DUMP_COLUMN(full_dst);
            throw Exception(error_code, "Columns are unequal");
        }
    };

    try
    {
        for (size_t i = 0; i < T; ++i)
        {
            size_t n1 = rng() % MAX_ROWS + 1;
            size_t k1 = sparse_ratios[rng() % K];

            size_t n2 = rng() % MAX_ROWS + 1;
            size_t k2 = sparse_ratios[rng() % K];

            size_t from = rng() % n2;
            size_t to = rng() % n2;

            if (from > to)
                std::swap(from, to);

            test_case(n1, k1, n2, k2, from, to - from);
        }
    }
    catch (const Exception & e)
    {
        FAIL() << e.displayText();
    }
}

TEST(ColumnSparse, PopBack)
{
    auto test_case = [&](size_t n, size_t k, size_t m)
    {
        auto [sparse_dst, full_dst] = createColumns(n, k);

        sparse_dst->popBack(m);
        full_dst->popBack(m);

        if (!checkEquals(*sparse_dst->convertToFullColumnIfSparse(), *full_dst))
        {
            DUMP_COLUMN(sparse_dst);
            DUMP_COLUMN(full_dst);
            throw Exception(error_code, "Columns are unequal");
        }
    };

    try
    {
        for (size_t i = 0; i < T; ++i)
        {
            size_t n = rng() % MAX_ROWS + 1;
            size_t k = sparse_ratios[rng() % K];
            size_t m = rng() % n;

            test_case(n, k, m);
        }
    }
    catch (const Exception & e)
    {
        FAIL() << e.displayText();
    }
}

TEST(ColumnSparse, Filter)
{
    auto test_case = [&](size_t n, size_t k, size_t m)
    {
        auto [sparse_src, full_src] = createColumns(n, k);

        PaddedPODArray<UInt8> filt(n);
        for (size_t i = 0; i < n; ++i)
            filt[i] = rng() % m == 0;

        auto sparse_dst = sparse_src->filter(filt, -1);
        auto full_dst = full_src->filter(filt, -1);

        if (!checkEquals(*sparse_dst->convertToFullColumnIfSparse(), *full_dst))
        {
            DUMP_COLUMN(sparse_src);
            DUMP_COLUMN(full_src);
            DUMP_COLUMN(sparse_dst);
            DUMP_COLUMN(full_dst);
            throw Exception(error_code, "Columns are unequal");
        }
    };

    try
    {
        for (size_t i = 0; i < T; ++i)
        {
            size_t n = rng() % MAX_ROWS + 1;
            size_t k = sparse_ratios[rng() % K];
            size_t m = sparse_ratios[rng() % K];

            test_case(n, k, m);
        }
    }
    catch (const Exception & e)
    {
        FAIL() << e.displayText();
    }
}

TEST(ColumnSparse, Permute)
{
    auto test_case = [&](size_t n, size_t k, size_t limit)
    {
        auto [sparse_src, full_src] = createColumns(n, k);

        IColumn::Permutation perm(n);
        std::iota(perm.begin(), perm.end(), 0);
        std::shuffle(perm.begin(), perm.end(), rng);

        auto sparse_dst = sparse_src->permute(perm, limit);
        auto full_dst = full_src->permute(perm, limit);

        if (limit)
        {
            sparse_dst = sparse_dst->cut(0, limit);
            full_dst = full_dst->cut(0, limit);
        }

        if (!checkEquals(*sparse_dst->convertToFullColumnIfSparse(), *full_dst))
        {
            DUMP_COLUMN(sparse_src);
            DUMP_COLUMN(full_src);
            DUMP_COLUMN(sparse_dst);
            DUMP_COLUMN(full_dst);
            throw Exception(error_code, "Columns are unequal");
        }
    };

    try
    {
        for (size_t i = 0; i < T; ++i)
        {
            size_t n = rng() % MAX_ROWS + 1;
            size_t k = sparse_ratios[rng() % K];
            size_t limit = rng() % 2 ? 0 : rng() % n;

            test_case(n, k, limit);
        }
    }
    catch (const Exception & e)
    {
        FAIL() << e.displayText();
    }
}

TEST(ColumnSparse, CompareColumn)
{
    auto test_case = [&](size_t n1, size_t k1, size_t n2, size_t k2, size_t row_num)
    {
        auto [sparse_src1, full_src1] = createColumns(n1, k1);
        auto [sparse_src2, full_src2] = createColumns(n2, k2);

        PaddedPODArray<Int8> comp_sparse;
        PaddedPODArray<Int8> comp_full;

        sparse_src1->compareColumn(*sparse_src2, row_num, nullptr, comp_sparse, 1, 1);
        full_src1->compareColumn(*full_src2, row_num, nullptr, comp_full, 1, 1);

        if (comp_sparse != comp_full)
        {
            DUMP_COLUMN(sparse_src1);
            DUMP_COLUMN(full_src1);
            DUMP_COLUMN(sparse_src2);
            DUMP_COLUMN(full_src2);
            throw Exception(error_code, "Compare results are unequal");
        }
    };

    try
    {
        for (size_t i = 0; i < T; ++i)
        {
            size_t n1 = rng() % MAX_ROWS + 1;
            size_t k1 = sparse_ratios[rng() % K];

            size_t n2 = rng() % MAX_ROWS + 1;
            size_t k2 = sparse_ratios[rng() % K];

            size_t row_num = rng() % n2;

            test_case(n1, k1, n2, k2, row_num);
        }
    }
    catch (const Exception & e)
    {
        FAIL() << e.displayText();
    }
}

TEST(ColumnSparse, GetPermutation)
{
    auto test_case = [&](size_t n, size_t k, size_t limit, bool reverse)
    {
        auto [sparse_src, full_src] = createColumns(n, k);

        IColumn::Permutation perm_sparse;
        IColumn::Permutation perm_full;

        IColumn::PermutationSortDirection direction = reverse ? IColumn::PermutationSortDirection::Descending : IColumn::PermutationSortDirection::Ascending;
        IColumn::PermutationSortStability stability = IColumn::PermutationSortStability::Unstable;

        sparse_src->getPermutation(direction, stability, limit, 1, perm_sparse);
        full_src->getPermutation(direction, stability, limit, 1, perm_full);

        auto sparse_sorted = sparse_src->permute(perm_sparse, limit);
        auto full_sorted = full_src->permute(perm_full, limit);

        if (limit)
        {
            sparse_sorted = sparse_sorted->cut(0, limit);
            full_sorted = full_sorted->cut(0, limit);
        }

        if (!checkEquals(*sparse_sorted->convertToFullColumnIfSparse(), *full_sorted))
        {
            DUMP_COLUMN(sparse_src);
            DUMP_COLUMN(full_src);
            DUMP_COLUMN(sparse_sorted);
            DUMP_COLUMN(full_sorted);
            throw Exception(error_code, "Sorted columns are unequal");
        }
    };

    try
    {
        for (size_t i = 0; i < T; ++i)
        {
            size_t n = rng() % MAX_ROWS + 1;
            size_t k = sparse_ratios[rng() % K];

            size_t limit = rng() % 2 ? 0 : rng() % n;
            bool reverse = rng() % 2;

            test_case(n, k, limit, reverse);
        }
    }
    catch (const Exception & e)
    {
        FAIL() << e.displayText();
    }
}

#undef DUMP_COLUMN
#undef DUMP_NON_DEFAULTS
