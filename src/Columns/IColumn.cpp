#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <Columns/IColumn.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnConst.h>
#include <Core/Field.h>


namespace DB
{

String IColumn::dumpStructure() const
{
    WriteBufferFromOwnString res;
    res << getFamilyName() << "(size = " << size();

    ColumnCallback callback = [&](ColumnPtr & subcolumn)
    {
        res << ", " << subcolumn->dumpStructure();
    };

    const_cast<IColumn*>(this)->forEachSubcolumn(callback);

    res << ")";
    return res.str();
}

void IColumn::insertFrom(const IColumn & src, size_t n)
{
    insert(src[n]);
}

bool isColumnNullable(const IColumn & column)
{
    return checkColumn<ColumnNullable>(column);
}

bool isColumnConst(const IColumn & column)
{
    return checkColumn<ColumnConst>(column);
}

void IColumn::updatePermutationImpl(
    size_t limit,
    Permutation & res,
    EqualRanges & equal_ranges,
    ComparePredicate less,
    ComparePredicate equals,
    Sort full_sort,
    PartialSort partial_sort) const
{
    if (equal_ranges.empty())
        return;

    if (limit >= size() || limit > equal_ranges.back().second)
        limit = 0;

    EqualRanges new_ranges;

    size_t number_of_ranges = equal_ranges.size();
    if (limit)
        --number_of_ranges;

    for (size_t i = 0; i < number_of_ranges; ++i)
    {
        const auto & [first, last] = equal_ranges[i];
        full_sort(res.begin() + first, res.begin() + last, less);

        size_t new_first = first;
        for (size_t j = first + 1; j < last; ++j)
        {
            if (!equals(res[j], res[new_first]))
            {
                if (j - new_first > 1)
                    new_ranges.emplace_back(new_first, j);

                new_first = j;
            }
        }

        if (last - new_first > 1)
            new_ranges.emplace_back(new_first, last);
    }

    if (limit)
    {
        const auto & [first, last] = equal_ranges.back();

        if (limit < first || limit > last)
        {
            equal_ranges = std::move(new_ranges);
            return;
        }

        /// Since then we are working inside the interval.
        partial_sort(res.begin() + first, res.begin() + limit, res.begin() + last, less);

        size_t new_first = first;
        for (size_t j = first + 1; j < limit; ++j)
        {
            if (!equals(res[j], res[new_first]))
            {
                if (j - new_first > 1)
                    new_ranges.emplace_back(new_first, j);
                new_first = j;
            }
        }

        size_t new_last = limit;
        for (size_t j = limit; j < last; ++j)
        {
            if (equals(res[j], res[new_first]))
            {
                std::swap(res[j], res[new_last]);
                ++new_last;
            }
        }

        if (new_last - new_first > 1)
            new_ranges.emplace_back(new_first, new_last);
    }

    equal_ranges = std::move(new_ranges);
}

}
