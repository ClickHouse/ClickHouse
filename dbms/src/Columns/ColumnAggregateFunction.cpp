#include <Columns/ColumnAggregateFunction.h>
#include <AggregateFunctions/AggregateFunctionState.h>
#include <DataStreams/ColumnGathererStream.h>
#include <IO/WriteBufferFromArena.h>
#include <Common/SipHash.h>
#include <Common/AlignedBuffer.h>
#include <Common/typeid_cast.h>
#include <Common/Arena.h>
#include <Columns/ColumnsCommon.h>


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

MutableColumnPtr ColumnAggregateFunction::convertToValues() const
{
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
    if (const AggregateFunctionState * function_state = typeid_cast<const AggregateFunctionState *>(func.get()))
    {
        auto res = createView();
        res->set(function_state->getNestedFunction());
        res->data.assign(data.begin(), data.end());
        return res;
    }

    MutableColumnPtr res = func->getReturnType()->createColumn();
    res->reserve(data.size());

    for (auto val : data)
        func->insertResultInto(val, *res);

    return res;
}


void ColumnAggregateFunction::ensureOwnership()
{
    if (src)
    {
        /// We must copy all data from src and take ownership.
        size_t size = data.size();

        Arena & arena = createOrGetArena();
        size_t size_of_state = func->sizeOfData();
        size_t align_of_state = func->alignOfData();

        size_t rollback_pos = 0;
        try
        {
            for (size_t i = 0; i < size; ++i)
            {
                ConstAggregateDataPtr old_place = data[i];
                data[i] = arena.alignedAlloc(size_of_state, align_of_state);
                func->create(data[i]);
                ++rollback_pos;
                func->merge(data[i], old_place, &arena);
            }
        }
        catch (...)
        {
            /// If we failed to take ownership, destroy all temporary data.

            if (!func->hasTrivialDestructor())
                for (size_t i = 0; i < rollback_pos; ++i)
                    func->destroy(data[i]);

            throw;
        }

        /// Now we own all data.
        src.reset();
    }
}


void ColumnAggregateFunction::insertRangeFrom(const IColumn & from, size_t start, size_t length)
{
    const ColumnAggregateFunction & from_concrete = static_cast<const ColumnAggregateFunction &>(from);

    if (start + length > from_concrete.data.size())
        throw Exception("Parameters start = " + toString(start) + ", length = " + toString(length)
                + " are out of bound in ColumnAggregateFunction::insertRangeFrom method"
                  " (data.size() = "
                + toString(from_concrete.data.size())
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
        src = from_concrete.getPtr();

        size_t old_size = data.size();
        data.resize(old_size + length);
        memcpy(&data[old_size], &from_concrete.data[start], length * sizeof(data[0]));
    }
}


ColumnPtr ColumnAggregateFunction::filter(const Filter & filter, ssize_t result_size_hint) const
{
    size_t size = data.size();
    if (size != filter.size())
        throw Exception("Size of filter doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    if (size == 0)
        return cloneEmpty();

    auto res = createView();
    auto & res_data = res->data;

    if (result_size_hint)
        res_data.reserve(result_size_hint > 0 ? result_size_hint : size);

    for (size_t i = 0; i < size; ++i)
        if (filter[i])
            res_data.push_back(data[i]);

    /// To save RAM in case of too strong filtering.
    if (res_data.size() * 2 < res_data.capacity())
        res_data = Container(res_data.cbegin(), res_data.cend());

    return res;
}


ColumnPtr ColumnAggregateFunction::permute(const Permutation & perm, size_t limit) const
{
    size_t size = data.size();

    if (limit == 0)
        limit = size;
    else
        limit = std::min(size, limit);

    if (perm.size() < limit)
        throw Exception("Size of permutation is less than required.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    auto res = createView();

    res->data.resize(limit);
    for (size_t i = 0; i < limit; ++i)
        res->data[i] = data[perm[i]];

    return res;
}

ColumnPtr ColumnAggregateFunction::index(const IColumn & indexes, size_t limit) const
{
    return selectIndexImpl(*this, indexes, limit);
}

template <typename Type>
ColumnPtr ColumnAggregateFunction::indexImpl(const PaddedPODArray<Type> & indexes, size_t limit) const
{
    auto res = createView();

    res->data.resize(limit);
    for (size_t i = 0; i < limit; ++i)
        res->data[i] = data[indexes[i]];

    return res;
}

INSTANTIATE_INDEX_IMPL(ColumnAggregateFunction)

/// Is required to support operations with Set
void ColumnAggregateFunction::updateHashWithValue(size_t n, SipHash & hash) const
{
    WriteBufferFromOwnString wbuf;
    func->serialize(data[n], wbuf);
    hash.update(wbuf.str().c_str(), wbuf.str().size());
}

/// NOTE: Highly overestimates size of a column if it was produced in AggregatingBlockInputStream (it contains size of other columns)
size_t ColumnAggregateFunction::byteSize() const
{
    size_t res = data.size() * sizeof(data[0]);

    for (const auto & arena : arenas)
        res += arena->size();

    return res;
}


/// Like byteSize(), highly overestimates size
size_t ColumnAggregateFunction::allocatedBytes() const
{
    size_t res = data.allocated_bytes();

    for (const auto & arena : arenas)
        res += arena->size();

    return res;
}

MutableColumnPtr ColumnAggregateFunction::cloneEmpty() const
{
    return create(func, Arenas(1, std::make_shared<Arena>()));
}

Field ColumnAggregateFunction::operator[](size_t n) const
{
    Field field = String();
    {
        WriteBufferFromString buffer(field.get<String &>());
        func->serialize(data[n], buffer);
    }
    return field;
}

void ColumnAggregateFunction::get(size_t n, Field & res) const
{
    res = String();
    {
        WriteBufferFromString buffer(res.get<String &>());
        func->serialize(data[n], buffer);
    }
}

StringRef ColumnAggregateFunction::getDataAt(size_t n) const
{
    return StringRef(reinterpret_cast<const char *>(&data[n]), sizeof(data[n]));
}

void ColumnAggregateFunction::insertData(const char * pos, size_t /*length*/)
{
    ensureOwnership();
    data.push_back(*reinterpret_cast<const AggregateDataPtr *>(pos));
}

void ColumnAggregateFunction::insertFrom(const IColumn & from, size_t n)
{
    /// Must create new state of aggregate function and take ownership of it,
    ///  because ownership of states of aggregate function cannot be shared for individual rows,
    ///  (only as a whole, see comment above).
    ensureOwnership();
    insertDefault();
    insertMergeFrom(from, n);
}

void ColumnAggregateFunction::insertFrom(ConstAggregateDataPtr place)
{
    ensureOwnership();
    insertDefault();
    insertMergeFrom(place);
}

void ColumnAggregateFunction::insertMergeFrom(ConstAggregateDataPtr place)
{
    func->merge(data.back(), place, &createOrGetArena());
}

void ColumnAggregateFunction::insertMergeFrom(const IColumn & from, size_t n)
{
    insertMergeFrom(static_cast<const ColumnAggregateFunction &>(from).data[n]);
}

Arena & ColumnAggregateFunction::createOrGetArena()
{
    if (unlikely(arenas.empty()))
        arenas.emplace_back(std::make_shared<Arena>());
    return *arenas.back().get();
}


static void pushBackAndCreateState(ColumnAggregateFunction::Container & data, Arena & arena, IAggregateFunction * func)
{
    data.push_back(arena.alignedAlloc(func->sizeOfData(), func->alignOfData()));
    try
    {
        func->create(data.back());
    }
    catch (...)
    {
        data.pop_back();
        throw;
    }
}


void ColumnAggregateFunction::insert(const Field & x)
{
    ensureOwnership();
    Arena & arena = createOrGetArena();
    pushBackAndCreateState(data, arena, func.get());
    ReadBufferFromString read_buffer(x.get<const String &>());
    func->deserialize(data.back(), read_buffer, &arena);
}

void ColumnAggregateFunction::insertDefault()
{
    ensureOwnership();
    Arena & arena = createOrGetArena();
    pushBackAndCreateState(data, arena, func.get());
}

StringRef ColumnAggregateFunction::serializeValueIntoArena(size_t n, Arena & dst, const char *& begin) const
{
    WriteBufferFromArena out(dst, begin);
    func->serialize(data[n], out);
    return out.finish();
}

const char * ColumnAggregateFunction::deserializeAndInsertFromArena(const char * src_arena)
{
    ensureOwnership();

    /** Parameter "src_arena" points to Arena, from which we will deserialize the state.
      * And "dst_arena" is another Arena, that aggregate function state will use to store its data.
      */
    Arena & dst_arena = createOrGetArena();
    pushBackAndCreateState(data, dst_arena, func.get());

    /** We will read from src_arena.
      * There is no limit for reading - it is assumed, that we can read all that we need after src_arena pointer.
      * Buf ReadBufferFromMemory requires some bound. We will use arbitary big enough number, that will not overflow pointer.
      * NOTE Technically, this is not compatible with C++ standard,
      *  as we cannot legally compare pointers after last element + 1 of some valid memory region.
      *  Probably this will not work under UBSan.
      */
    ReadBufferFromMemory read_buffer(src_arena, std::numeric_limits<char *>::max() - src_arena);
    func->deserialize(data.back(), read_buffer, &dst_arena);

    return read_buffer.position();
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

ColumnPtr ColumnAggregateFunction::replicate(const IColumn::Offsets & offsets) const
{
    size_t size = data.size();
    if (size != offsets.size())
        throw Exception("Size of offsets doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    if (size == 0)
        return cloneEmpty();

    auto res = createView();
    auto & res_data = res->data;
    res_data.reserve(offsets.back());

    IColumn::Offset prev_offset = 0;
    for (size_t i = 0; i < size; ++i)
    {
        size_t size_to_replicate = offsets[i] - prev_offset;
        prev_offset = offsets[i];

        for (size_t j = 0; j < size_to_replicate; ++j)
            res_data.push_back(data[i]);
    }

    return res;
}

MutableColumns ColumnAggregateFunction::scatter(IColumn::ColumnIndex num_columns, const IColumn::Selector & selector) const
{
    /// Columns with scattered values will point to this column as the owner of values.
    MutableColumns columns(num_columns);
    for (auto & column : columns)
        column = createView();

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

void ColumnAggregateFunction::getPermutation(bool /*reverse*/, size_t /*limit*/, int /*nan_direction_hint*/, IColumn::Permutation & res) const
{
    size_t s = data.size();
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
    /// Place serialized default values into min/max.

    AlignedBuffer place_buffer(func->sizeOfData(), func->alignOfData());
    AggregateDataPtr place = place_buffer.data();

    String serialized;

    func->create(place);
    try
    {
        WriteBufferFromString buffer(serialized);
        func->serialize(place, buffer);
    }
    catch (...)
    {
        func->destroy(place);
        throw;
    }
    func->destroy(place);

    min = serialized;
    max = serialized;
}

}
