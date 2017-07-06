#include <Columns/ColumnAggregateFunction.h>
#include <AggregateFunctions/AggregateFunctionState.h>
#include <DataStreams/ColumnGathererStream.h>
#include <Common/SipHash.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int PARAMETER_OUT_OF_BOUND;
    extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
}


ColumnAggregateFunction::~ColumnAggregateFunction()
{
    if (!func->hasTrivialDestructor() && !src)
        for (auto val : data)
            func->destroy(val);
}

void ColumnAggregateFunction::addArena(ArenaPtr arena_)
{
    arenas.push_back(arena_);
}

ColumnPtr ColumnAggregateFunction::convertToValues() const
{
    const IAggregateFunction * function = func.get();
    ColumnPtr res = function->getReturnType()->createColumn();

    /** If the aggregate function returns an unfinalized/unfinished state,
        * then you just need to copy pointers to it and also shared ownership of data.
        *
        * Also replace the aggregate function with the nested function.
        * That is, if this column is the states of the aggregate function `aggState`,
        * then we return the same column, but with the states of the aggregate function `agg`.
        * These are the same states, changing only the function to which they correspond.
        *
        * Further is quite difficult to understand.
        * Example when this happens:
        *
        * SELECT k, finalizeAggregation(quantileTimingState(0.5)(x)) FROM ... GROUP BY k WITH TOTALS
        *
        * This calculates the aggregate function `quantileTimingState`.
        * Its return type AggregateFunction(quantileTiming(0.5), UInt64)`.
        * Due to the presence of WITH TOTALS, during aggregation the states of this aggregate function will be stored
        *  in the ColumnAggregateFunction column of type
        *  AggregateFunction(quantileTimingState(0.5), UInt64).
        * Then, in `TotalsHavingBlockInputStream`, it will be called `convertToValues` method,
        *  to get the "ready" values.
        * But it just converts a column of type
        *   `AggregateFunction(quantileTimingState(0.5), UInt64)`
        * into `AggregateFunction(quantileTiming(0.5), UInt64)`
        * - in the same states.
        *
        * Then `finalizeAggregation` function will be calculated, which will call `convertToValues` already on the result.
        * And this converts a column of type
        *   AggregateFunction(quantileTiming(0.5), UInt64)
        * into UInt16 - already finished result of `quantileTiming`.
        */
    if (const AggregateFunctionState * function_state = typeid_cast<const AggregateFunctionState *>(function))
    {
        std::shared_ptr<ColumnAggregateFunction> res = std::make_shared<ColumnAggregateFunction>(*this);
        res->set(function_state->getNestedFunction());
        res->getData().assign(getData().begin(), getData().end());
        return res;
    }

    IColumn & column = *res;
    res->reserve(getData().size());

    for (auto val : getData())
        function->insertResultInto(val, column);

    return res;
}


void ColumnAggregateFunction::insertRangeFrom(const IColumn & from, size_t start, size_t length)
{
    const ColumnAggregateFunction & from_concrete = static_cast<const ColumnAggregateFunction &>(from);

    if (start + length > from_concrete.getData().size())
        throw Exception("Parameters start = " + toString(start) + ", length = " + toString(length)
                + " are out of bound in ColumnAggregateFunction::insertRangeFrom method"
                  " (data.size() = "
                + toString(from_concrete.getData().size())
                + ").",
            ErrorCodes::PARAMETER_OUT_OF_BOUND);

    if (!empty() && src.get() != &from_concrete)
    {
        /// Must create new states of aggregate function and take ownership of it,
        ///  because ownership of states of aggregate function cannot be shared for individual rows,
        ///  (only as a whole).

        size_t end = start + length;
        for (size_t i = start; i < end; ++i)
            insertFrom(from, i);
    }
    else
    {
        /// Keep shared ownership of aggregation states.
        src = from_concrete.shared_from_this();

        auto & data = getData();
        size_t old_size = data.size();
        data.resize(old_size + length);
        memcpy(&data[old_size], &from_concrete.getData()[start], length * sizeof(data[0]));
    }
}


ColumnPtr ColumnAggregateFunction::filter(const Filter & filter, ssize_t result_size_hint) const
{
    size_t size = getData().size();
    if (size != filter.size())
        throw Exception("Size of filter doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    std::shared_ptr<ColumnAggregateFunction> res = std::make_shared<ColumnAggregateFunction>(*this);

    if (size == 0)
        return res;

    auto & res_data = res->getData();

    if (result_size_hint)
        res_data.reserve(result_size_hint > 0 ? result_size_hint : size);

    for (size_t i = 0; i < size; ++i)
        if (filter[i])
            res_data.push_back(getData()[i]);

    /// To save RAM in case of too strong filtering.
    if (res_data.size() * 2 < res_data.capacity())
        res_data = Container_t(res_data.cbegin(), res_data.cend());

    return res;
}


ColumnPtr ColumnAggregateFunction::permute(const Permutation & perm, size_t limit) const
{
    size_t size = getData().size();

    if (limit == 0)
        limit = size;
    else
        limit = std::min(size, limit);

    if (perm.size() < limit)
        throw Exception("Size of permutation is less than required.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    std::shared_ptr<ColumnAggregateFunction> res = std::make_shared<ColumnAggregateFunction>(*this);

    res->getData().resize(limit);
    for (size_t i = 0; i < limit; ++i)
        res->getData()[i] = getData()[perm[i]];

    return res;
}

/// Is required to support operations with Set
void ColumnAggregateFunction::updateHashWithValue(size_t n, SipHash & hash) const
{
    String buf;
    {
        WriteBufferFromString wbuf(buf);
        func->serialize(getData()[n], wbuf);
    }
    hash.update(buf.c_str(), buf.size());
}

/// NOTE: Highly overestimates size of a column if it was produced in AggregatingBlockInputStream (it contains size of other columns)
size_t ColumnAggregateFunction::byteSize() const
{
    size_t res = getData().size() * sizeof(getData()[0]);

    for (const auto & arena : arenas)
        res += arena.get()->size();

    return res;
}


/// Like byteSize(), highly overestimates size
size_t ColumnAggregateFunction::allocatedSize() const
{
    size_t res = getData().allocated_size() * sizeof(getData()[0]);

    for (const auto & arena : arenas)
        res += arena.get()->size();

    return res;
}

ColumnPtr ColumnAggregateFunction::cloneEmpty() const
{
    return std::make_shared<ColumnAggregateFunction>(func, Arenas(1, std::make_shared<Arena>()));
}

Field ColumnAggregateFunction::operator[](size_t n) const
{
    Field field = String();
    {
        WriteBufferFromString buffer(field.get<String &>());
        func->serialize(getData()[n], buffer);
    }
    return field;
}

void ColumnAggregateFunction::get(size_t n, Field & res) const
{
    res = String();
    {
        WriteBufferFromString buffer(res.get<String &>());
        func->serialize(getData()[n], buffer);
    }
}

StringRef ColumnAggregateFunction::getDataAt(size_t n) const
{
    return StringRef(reinterpret_cast<const char *>(&getData()[n]), sizeof(getData()[n]));
}

void ColumnAggregateFunction::insertData(const char * pos, size_t length)
{
    getData().push_back(*reinterpret_cast<const AggregateDataPtr *>(pos));
}

void ColumnAggregateFunction::insertFrom(const IColumn & src, size_t n)
{
    /// Must create new state of aggregate function and take ownership of it,
    ///  because ownership of states of aggregate function cannot be shared for individual rows,
    ///  (only as a whole, see comment above).
    insertDefault();
    insertMergeFrom(src, n);
}

void ColumnAggregateFunction::insertFrom(ConstAggregateDataPtr place)
{
    insertDefault();
    insertMergeFrom(place);
}

void ColumnAggregateFunction::insertMergeFrom(ConstAggregateDataPtr place)
{
    func->merge(getData().back(), place, &createOrGetArena());
}

void ColumnAggregateFunction::insertMergeFrom(const IColumn & src, size_t n)
{
    insertMergeFrom(static_cast<const ColumnAggregateFunction &>(src).getData()[n]);
}

Arena & ColumnAggregateFunction::createOrGetArena()
{
    if (unlikely(arenas.empty()))
        arenas.emplace_back(std::make_shared<Arena>());
    return *arenas.back().get();
}

void ColumnAggregateFunction::insert(const Field & x)
{
    IAggregateFunction * function = func.get();

    Arena & arena = createOrGetArena();

    getData().push_back(arena.alloc(function->sizeOfData()));
    function->create(getData().back());
    ReadBufferFromString read_buffer(x.get<const String &>());
    function->deserialize(getData().back(), read_buffer, &arena);
}

void ColumnAggregateFunction::insertDefault()
{
    IAggregateFunction * function = func.get();

    Arena & arena = createOrGetArena();

    getData().push_back(arena.alloc(function->sizeOfData()));
    function->create(getData().back());
}

StringRef ColumnAggregateFunction::serializeValueIntoArena(size_t n, Arena & arena, const char *& begin) const
{
    throw Exception("Method serializeValueIntoArena is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
}

const char * ColumnAggregateFunction::deserializeAndInsertFromArena(const char * pos)
{
    throw Exception("Method deserializeAndInsertFromArena is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
}

void ColumnAggregateFunction::popBack(size_t n)
{
    size_t size = data.size();
    size_t new_size = size - n;

    if (!src)
        for (size_t i = new_size; i < size; ++i)
            func->destroy(data[i]);

    data.resize_assume_reserved(new_size);
}

ColumnPtr ColumnAggregateFunction::replicate(const IColumn::Offsets_t & offsets) const
{
    throw Exception("Method replicate is not supported for ColumnAggregateFunction.", ErrorCodes::NOT_IMPLEMENTED);
}

Columns ColumnAggregateFunction::scatter(IColumn::ColumnIndex num_columns, const IColumn::Selector & selector) const
{
    /// Columns with scattered values will point to this column as the owner of values.
    Columns columns(num_columns);
    for (auto & column : columns)
        column = std::make_shared<ColumnAggregateFunction>(*this);

    size_t num_rows = size();

    {
        size_t reserve_size = num_rows / num_columns * 1.1; /// 1.1 is just a guess. Better to use n-sigma rule.

        if (reserve_size > 1)
            for (auto & column : columns)
                column->reserve(reserve_size);
    }

    for (size_t i = 0; i < num_rows; ++i)
        static_cast<ColumnAggregateFunction &>(*columns[selector[i]]).data.push_back(data[i]);

    return columns;
}

void ColumnAggregateFunction::getPermutation(bool reverse, size_t limit, int nan_direction_hint, IColumn::Permutation & res) const
{
    size_t s = getData().size();
    res.resize(s);
    for (size_t i = 0; i < s; ++i)
        res[i] = i;
}

void ColumnAggregateFunction::gather(ColumnGathererStream & gatherer)
{
    gatherer.gather(*this);
}

void ColumnAggregateFunction::getExtremes(Field & min, Field & max) const
{
    throw Exception("Method getExtremes is not supported for ColumnAggregateFunction.", ErrorCodes::NOT_IMPLEMENTED);
}

}
