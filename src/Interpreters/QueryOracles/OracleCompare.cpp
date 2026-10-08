#include <Interpreters/QueryOracles/OracleCompare.h>

#include <fmt/format.h>

namespace DB
{

std::string OracleCompare::diffSummary(const Rows & a, const Rows & b, size_t max_diff)
{
    /// Symmetric-diff walk over two sorted sequences: `< row` is present only in `a`, `> row`
    /// only in `b`. Assumes both inputs are sorted (SortedBag/SortedSet), which every equality
    /// oracle produces via OracleExec.
    std::string out;
    size_t shown = 0;
    size_t ai = 0;
    size_t bi = 0;
    while ((ai < a.size() || bi < b.size()) && shown < max_diff)
    {
        if (ai < a.size() && (bi >= b.size() || a[ai] < b[bi]))
        {
            out += fmt::format("  < {}\n", a[ai]);
            ++ai;
            ++shown;
        }
        else if (bi < b.size() && (ai >= a.size() || b[bi] < a[ai]))
        {
            out += fmt::format("  > {}\n", b[bi]);
            ++bi;
            ++shown;
        }
        else
        {
            ++ai;
            ++bi;
        }
    }
    return out;
}

}
