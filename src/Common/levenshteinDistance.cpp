#include <Common/PODArray.h>
#include <Common/iota.h>
#include <Common/levenshteinDistance.h>

namespace DB
{

size_t levenshteinDistance(const String & lhs, const String & rhs)
{
    size_t m = lhs.size();
    size_t n = rhs.size();

    PODArrayWithStackMemory<size_t, 64> row(n + 1);

    iota(row.data() + 1, n, size_t(1));

    for (size_t j = 1; j <= m; ++j)
    {
        row[0] = j;
        size_t prev = j - 1;
        for (size_t i = 1; i <= n; ++i)
        {
            size_t old = row[i];
            row[i] = std::min(prev + (std::tolower(lhs[j - 1]) != std::tolower(rhs[i - 1])),
                              std::min(row[i - 1], row[i]) + 1);
            prev = old;
        }
    }
    return row[n];
}

}
