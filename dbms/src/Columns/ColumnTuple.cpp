#include <Columns/ColumnTuple.h>
#include <ext/map.h>
#include <ext/range.h>
#include <DataStreams/ColumnGathererStream.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int CANNOT_INSERT_VALUE_OF_DIFFERENT_SIZE_INTO_TUPLE;
}


ColumnTuple::ColumnTuple(Block data_) : data(data_)
{
    size_t size = data.columns();
    columns.resize(size);
    for (size_t i = 0; i < size; ++i)
        columns[i] = data.getByPosition(i).column;
}

ColumnPtr ColumnTuple::cloneEmpty() const
{
    return std::make_shared<ColumnTuple>(data.cloneEmpty());
}

Field ColumnTuple::operator[](size_t n) const
{
    return Tuple{ext::map<TupleBackend>(columns, [n] (const auto & column) { return (*column)[n]; })};
}

void ColumnTuple::get(size_t n, Field & res) const
{
    const size_t size = columns.size();
    res = Tuple(TupleBackend(size));
    TupleBackend & res_arr = DB::get<Tuple &>(res).t;
    for (const auto i : ext::range(0, size))
        columns[i]->get(n, res_arr[i]);
}

StringRef ColumnTuple::getDataAt(size_t n) const
{
    throw Exception("Method getDataAt is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
}

void ColumnTuple::insertData(const char * pos, size_t length)
{
    throw Exception("Method insertData is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
}

void ColumnTuple::insert(const Field & x)
{
    const TupleBackend & tuple = DB::get<const Tuple &>(x).t;

    const size_t size = columns.size();
    if (tuple.size() != size)
        throw Exception("Cannot insert value of different size into tuple", ErrorCodes::CANNOT_INSERT_VALUE_OF_DIFFERENT_SIZE_INTO_TUPLE);

    for (size_t i = 0; i < size; ++i)
        columns[i]->insert(tuple[i]);
}

void ColumnTuple::insertFrom(const IColumn & src_, size_t n)
{
    const ColumnTuple & src = static_cast<const ColumnTuple &>(src_);

    size_t size = columns.size();
    if (src.columns.size() != size)
        throw Exception("Cannot insert value of different size into tuple", ErrorCodes::CANNOT_INSERT_VALUE_OF_DIFFERENT_SIZE_INTO_TUPLE);

    for (size_t i = 0; i < size; ++i)
        columns[i]->insertFrom(*src.columns[i], n);
}

void ColumnTuple::insertDefault()
{
    for (auto & column : columns)
        column->insertDefault();
}

void ColumnTuple::popBack(size_t n)
{
    for (auto & column : columns)
        column->popBack(n);
}

StringRef ColumnTuple::serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const
{
    size_t values_size = 0;
    for (auto & column : columns)
        values_size += column->serializeValueIntoArena(n, arena, begin).size;

    return StringRef(begin, values_size);
}

const char * ColumnTuple::deserializeAndInsertFromArena(const char * pos)
{
    for (auto & column : columns)
        pos = column->deserializeAndInsertFromArena(pos);

    return pos;
}

void ColumnTuple::updateHashWithValue(size_t n, SipHash & hash) const
{
    for (auto & column : columns)
        column->updateHashWithValue(n, hash);
}

void ColumnTuple::insertRangeFrom(const IColumn & src, size_t start, size_t length)
{
    for (size_t i = 0; i < columns.size(); ++i)
        data.getByPosition(i).column->insertRangeFrom(
            *static_cast<const ColumnTuple &>(src).data.getByPosition(i).column.get(),
            start, length);
}

ColumnPtr ColumnTuple::filter(const Filter & filt, ssize_t result_size_hint) const
{
    Block res_block = data.cloneEmpty();

    for (size_t i = 0; i < columns.size(); ++i)
        res_block.getByPosition(i).column = data.getByPosition(i).column->filter(filt, result_size_hint);

    return std::make_shared<ColumnTuple>(res_block);
}

ColumnPtr ColumnTuple::permute(const Permutation & perm, size_t limit) const
{
    Block res_block = data.cloneEmpty();

    for (size_t i = 0; i < columns.size(); ++i)
        res_block.getByPosition(i).column = data.getByPosition(i).column->permute(perm, limit);

    return std::make_shared<ColumnTuple>(res_block);
}

ColumnPtr ColumnTuple::replicate(const Offsets_t & offsets) const
{
    Block res_block = data.cloneEmpty();

    for (size_t i = 0; i < columns.size(); ++i)
        res_block.getByPosition(i).column = data.getByPosition(i).column->replicate(offsets);

    return std::make_shared<ColumnTuple>(res_block);
}

Columns ColumnTuple::scatter(ColumnIndex num_columns, const Selector & selector) const
{
    size_t num_tuple_elements = columns.size();
    std::vector<Columns> scattered_tuple_elements(num_tuple_elements);

    for (size_t tuple_element_idx = 0; tuple_element_idx < num_tuple_elements; ++tuple_element_idx)
        scattered_tuple_elements[tuple_element_idx] = data.getByPosition(tuple_element_idx).column->scatter(num_columns, selector);

    Columns res(num_columns);

    for (size_t scattered_idx = 0; scattered_idx < num_columns; ++scattered_idx)
    {
        Block res_block = data.cloneEmpty();
        for (size_t tuple_element_idx = 0; tuple_element_idx < num_tuple_elements; ++tuple_element_idx)
            res_block.getByPosition(tuple_element_idx).column = scattered_tuple_elements[tuple_element_idx][scattered_idx];
        res[scattered_idx] = std::make_shared<ColumnTuple>(res_block);
    }

    return res;
}

int ColumnTuple::compareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const
{
    size_t size = columns.size();
    for (size_t i = 0; i < size; ++i)
        if (int res = columns[i]->compareAt(n, m, *static_cast<const ColumnTuple &>(rhs).columns[i], nan_direction_hint))
            return res;

    return 0;
}

template <bool positive>
struct ColumnTuple::Less
{
    ConstColumnPlainPtrs plain_columns;
    int nan_direction_hint;

    Less(const Columns & columns, int nan_direction_hint_)
        : nan_direction_hint(nan_direction_hint_)
    {
        for (const auto & column : columns)
            plain_columns.push_back(column.get());
    }

    bool operator() (size_t a, size_t b) const
    {
        for (ConstColumnPlainPtrs::const_iterator it = plain_columns.begin(); it != plain_columns.end(); ++it)
        {
            int res = (*it)->compareAt(a, b, **it, nan_direction_hint);
            if (res < 0)
                return positive;
            else if (res > 0)
                return !positive;
        }
        return false;
    }
};

void ColumnTuple::getPermutation(bool reverse, size_t limit, int nan_direction_hint, Permutation & res) const
{
    size_t rows = size();
    res.resize(rows);
    for (size_t i = 0; i < rows; ++i)
        res[i] = i;

    if (limit >= rows)
        limit = 0;

    if (limit)
    {
        if (reverse)
            std::partial_sort(res.begin(), res.begin() + limit, res.end(), Less<false>(columns, nan_direction_hint));
        else
            std::partial_sort(res.begin(), res.begin() + limit, res.end(), Less<true>(columns, nan_direction_hint));
    }
    else
    {
        if (reverse)
            std::sort(res.begin(), res.end(), Less<false>(columns, nan_direction_hint));
        else
            std::sort(res.begin(), res.end(), Less<true>(columns, nan_direction_hint));
    }
}

void ColumnTuple::gather(ColumnGathererStream & gatherer)
{
    gatherer.gather(*this);
}

void ColumnTuple::reserve(size_t n)
{
    for (auto & column : columns)
        column->reserve(n);
}

size_t ColumnTuple::byteSize() const
{
    size_t res = 0;
    for (const auto & column : columns)
        res += column->byteSize();
    return res;
}

size_t ColumnTuple::allocatedSize() const
{
    size_t res = 0;
    for (const auto & column : columns)
        res += column->allocatedSize();
    return res;
}

ColumnPtr ColumnTuple::convertToFullColumnIfConst() const
{
    Block materialized = data;
    for (size_t i = 0, size = materialized.columns(); i < size; ++i)
        if (auto converted = materialized.getByPosition(i).column->convertToFullColumnIfConst())
            materialized.getByPosition(i).column = converted;

    return std::make_shared<ColumnTuple>(materialized);
}

void ColumnTuple::getExtremes(Field & min, Field & max) const
{
    const size_t tuple_size = columns.size();

    min = Tuple(TupleBackend(tuple_size));
    max = Tuple(TupleBackend(tuple_size));

    auto & min_backend = min.get<Tuple &>().t;
    auto & max_backend = max.get<Tuple &>().t;

    for (const auto i : ext::range(0, tuple_size))
        columns[i]->getExtremes(min_backend[i], max_backend[i]);
}


}
