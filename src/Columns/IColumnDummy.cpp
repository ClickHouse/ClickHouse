#include <Columns/ColumnsCommon.h>
#include <Columns/IColumnDummy.h>
#include <Core/Field.h>
#include <Common/Arena.h>
#include <Common/iota.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
    extern const int NOT_IMPLEMENTED;
}


Field IColumnDummy::operator[](size_t) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot get value from {}", getName());
}

void IColumnDummy::get(size_t, Field &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot get value from {}", getName());
}

void IColumnDummy::insert(const Field &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot insert element into {}", getName());
}

bool IColumnDummy::isDefaultAt(size_t) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "isDefaultAt is not implemented for {}", getName());
}

StringRef IColumnDummy::serializeValueIntoArena(size_t /*n*/, Arena & arena, char const *& begin) const
{
    /// Has to put one useless byte into Arena, because serialization into zero number of bytes is ambiguous.
    char * res = arena.allocContinue(1, begin);
    *res = 0;
    return { res, 1 };
}

const char * IColumnDummy::deserializeAndInsertFromArena(const char * pos)
{
    ++s;
    return pos + 1;
}

const char * IColumnDummy::skipSerializedInArena(const char * pos) const
{
    return pos;
}

ColumnPtr IColumnDummy::filter(const Filter & filt, ssize_t /*result_size_hint*/) const
{
    size_t bytes = countBytesInFilter(filt);
    return cloneDummy(bytes);
}

void IColumnDummy::expand(const IColumn::Filter & mask, bool)
{
    s = mask.size();
}

ColumnPtr IColumnDummy::permute(const Permutation & perm, size_t limit) const
{
    if (s != perm.size())
        throw Exception(ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH, "Size of permutation doesn't match size of column.");

    return cloneDummy(limit ? std::min(s, limit) : s);
}

ColumnPtr IColumnDummy::index(const IColumn & indexes, size_t limit) const
{
    if (indexes.size() < limit)
        throw Exception(ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH, "Size of indexes is less than required.");

    return cloneDummy(limit ? limit : s);
}

void IColumnDummy::getPermutation(IColumn::PermutationSortDirection /*direction*/, IColumn::PermutationSortStability /*stability*/,
                size_t /*limit*/, int /*nan_direction_hint*/, Permutation & res) const
{
    res.resize(s);
    iota(res.data(), s, IColumn::Permutation::value_type(0));
}

ColumnPtr IColumnDummy::replicate(const Offsets & offsets) const
{
    if (s != offsets.size())
        throw Exception(ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH, "Size of offsets doesn't match size of column.");

    return cloneDummy(offsets.back());
}

MutableColumns IColumnDummy::scatter(ColumnIndex num_columns, const Selector & selector) const
{
    if (s != selector.size())
        throw Exception(ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH, "Size of selector doesn't match size of column.");

    std::vector<size_t> counts(num_columns);
    for (auto idx : selector)
        ++counts[idx];

    MutableColumns res(num_columns);
    for (size_t i = 0; i < num_columns; ++i)
        res[i] = cloneResized(counts[i]);

    return res;
}

double IColumnDummy::getRatioOfDefaultRows(double) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method getRatioOfDefaultRows is not supported for {}", getName());
}

UInt64 IColumnDummy::getNumberOfDefaultRows() const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method getNumberOfDefaultRows is not supported for {}", getName());
}

void IColumnDummy::getIndicesOfNonDefaultRows(Offsets &, size_t, size_t) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method getIndicesOfNonDefaultRows is not supported for {}", getName());
}

void IColumnDummy::gather(ColumnGathererStream &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method gather is not supported for {}", getName());
}

}
