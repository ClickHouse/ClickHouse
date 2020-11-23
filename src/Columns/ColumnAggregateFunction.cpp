#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnsCommon.h>
#include <Common/assert_cast.h>
#include <DataStreams/ColumnGathererStream.h>
#include <IO/WriteBufferFromArena.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <Common/FieldVisitors.h>
#include <Common/SipHash.h>
#include <Common/AlignedBuffer.h>
#include <Common/typeid_cast.h>
#include <Common/Arena.h>
#include <Common/WeakHash.h>
#include <Common/HashTable/Hash.h>

#include <AggregateFunctions/AggregateFunctionMLMethod.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int PARAMETER_OUT_OF_BOUND;
    extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


static std::string getTypeString(const AggregateFunctionPtr & func)
{
    WriteBufferFromOwnString stream;
    stream << "AggregateFunction(" << func->getName();
    const auto & parameters = func->getParameters();
    const auto & argument_types = func->getArgumentTypes();

    if (!parameters.empty())
    {
        stream << '(';
        for (size_t i = 0; i < parameters.size(); ++i)
        {
            if (i)
                stream << ", ";
            stream << applyVisitor(FieldVisitorToString(), parameters[i]);
        }
        stream << ')';
    }

    for (const auto & argument_type : argument_types)
        stream << ", " << argument_type->getName();

    stream << ')';
    return stream.str();
}


ColumnAggregateFunction::ColumnAggregateFunction(const AggregateFunctionPtr & func_)
    : func(func_), type_string(getTypeString(func))
{
}

ColumnAggregateFunction::ColumnAggregateFunction(const AggregateFunctionPtr & func_, const ConstArenas & arenas_)
    : foreign_arenas(arenas_), func(func_), type_string(getTypeString(func))
{

}

void ColumnAggregateFunction::set(const AggregateFunctionPtr & func_)
{
    func = func_;
    type_string = getTypeString(func);
}


ColumnAggregateFunction::~ColumnAggregateFunction()
{
    if (!func->hasTrivialDestructor() && !src)
        for (auto * val : data)
            func->destroy(val);
}

void ColumnAggregateFunction::addArena(ConstArenaPtr arena_)
{
    foreign_arenas.push_back(arena_);
}

namespace
{

ConstArenas concatArenas(const ConstArenas & array, ConstArenaPtr arena)
{
    ConstArenas result = array;
    if (arena)
        result.push_back(std::move(arena));

    return result;
}

}

MutableColumnPtr ColumnAggregateFunction::convertToValues(MutableColumnPtr column)
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
      * Then, in `TotalsHavingTransform`, it will be called `convertToValues` method,
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
    auto & column_aggregate_func = assert_cast<ColumnAggregateFunction &>(*column);
    auto & func = column_aggregate_func.func;
    auto & data = column_aggregate_func.data;

    /// insertResultInto may invalidate states, so we must unshare ownership of them
    column_aggregate_func.ensureOwnership();

    MutableColumnPtr res = func->getReturnType()->createColumn();
    res->reserve(data.size());

    /// If there are references to states in final column, we must hold their ownership
    /// by holding arenas and source.

    auto callback = [&](auto & subcolumn)
    {
        if (auto * aggregate_subcolumn = typeid_cast<ColumnAggregateFunction *>(subcolumn.get()))
        {
            aggregate_subcolumn->foreign_arenas = concatArenas(column_aggregate_func.foreign_arenas, column_aggregate_func.my_arena);
            aggregate_subcolumn->src = column_aggregate_func.getPtr();
        }
    };

    callback(res);
    res->forEachSubcolumn(callback);

    for (auto * val : data)
        func->insertResultInto(val, *res, &column_aggregate_func.createOrGetArena());

    return res;
}

MutableColumnPtr ColumnAggregateFunction::predictValues(const ColumnsWithTypeAndName & arguments, const Context & context) const
{
    MutableColumnPtr res = func->getReturnTypeToPredict()->createColumn();
    res->reserve(data.size());

    auto * machine_learning_function = func.get();
    if (machine_learning_function)
    {
        if (data.size() == 1)
        {
            /// Case for const column. Predict using single model.
            machine_learning_function->predictValues(data[0], *res, arguments, 0, arguments.front().column->size(), context);
        }
        else
        {
            /// Case for non-constant column. Use different aggregate function for each row.
            size_t row_num = 0;
            for (auto * val : data)
            {
                machine_learning_function->predictValues(val, *res, arguments, row_num, 1, context);
                ++row_num;
            }
        }
    }
    else
    {
        throw Exception("Illegal aggregate function is passed",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }
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


bool ColumnAggregateFunction::structureEquals(const IColumn & to) const
{
    const auto * to_concrete = typeid_cast<const ColumnAggregateFunction *>(&to);
    if (!to_concrete)
        return false;

    /// AggregateFunctions must be the same.

    const IAggregateFunction & func_this = *func;
    const IAggregateFunction & func_to = *to_concrete->func;

    return typeid(func_this) == typeid(func_to);
}


void ColumnAggregateFunction::insertRangeFrom(const IColumn & from, size_t start, size_t length)
{
    const ColumnAggregateFunction & from_concrete = assert_cast<const ColumnAggregateFunction &>(from);

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
        memcpy(data.data() + old_size, &from_concrete.data[start], length * sizeof(data[0]));
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

void ColumnAggregateFunction::updateWeakHash32(WeakHash32 & hash) const
{
    auto s = data.size();
    if (hash.getData().size() != data.size())
        throw Exception("Size of WeakHash32 does not match size of column: column size is " + std::to_string(s) +
                        ", hash size is " + std::to_string(hash.getData().size()), ErrorCodes::LOGICAL_ERROR);

    auto & hash_data = hash.getData();

    std::vector<UInt8> v;
    for (size_t i = 0; i < s; ++i)
    {
        WriteBufferFromVector<std::vector<UInt8>> wbuf(v);
        func->serialize(data[i], wbuf);
        wbuf.finalize();
        hash_data[i] = ::updateWeakHash32(v.data(), v.size(), hash_data[i]);
    }
}

void ColumnAggregateFunction::updateHashFast(SipHash & hash) const
{
    /// Fallback to per-element hashing, as there is no faster way
    for (size_t i = 0; i < size(); ++i)
        updateHashWithValue(i, hash);
}

/// The returned size is less than real size. The reason is that some parts of
/// aggregate function data may be allocated on shared arenas. These arenas are
/// used for several blocks, and also may be updated concurrently from other
/// threads, so we can't know the size of these data.
size_t ColumnAggregateFunction::byteSize() const
{
    return data.size() * sizeof(data[0])
            + (my_arena ? my_arena->size() : 0);
}

/// Like in byteSize(), the size is underestimated.
size_t ColumnAggregateFunction::allocatedBytes() const
{
    return data.allocated_bytes()
            + (my_arena ? my_arena->size() : 0);
}

void ColumnAggregateFunction::protect()
{
    data.protect();
}

MutableColumnPtr ColumnAggregateFunction::cloneEmpty() const
{
    return create(func);
}

Field ColumnAggregateFunction::operator[](size_t n) const
{
    Field field = AggregateFunctionStateData();
    field.get<AggregateFunctionStateData &>().name = type_string;
    {
        WriteBufferFromString buffer(field.get<AggregateFunctionStateData &>().data);
        func->serialize(data[n], buffer);
    }
    return field;
}

void ColumnAggregateFunction::get(size_t n, Field & res) const
{
    res = AggregateFunctionStateData();
    res.get<AggregateFunctionStateData &>().name = type_string;
    {
        WriteBufferFromString buffer(res.get<AggregateFunctionStateData &>().data);
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
    insertMergeFrom(assert_cast<const ColumnAggregateFunction &>(from).data[n]);
}

Arena & ColumnAggregateFunction::createOrGetArena()
{
    if (unlikely(!my_arena))
        my_arena = std::make_shared<Arena>();

    return *my_arena.get();
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
    if (x.getType() != Field::Types::AggregateFunctionState)
        throw Exception(String("Inserting field of type ") + x.getTypeName() + " into ColumnAggregateFunction. "
                        "Expected " + Field::Types::toString(Field::Types::AggregateFunctionState), ErrorCodes::LOGICAL_ERROR);

    const auto & field_name = x.get<const AggregateFunctionStateData &>().name;
    if (type_string != field_name)
        throw Exception("Cannot insert filed with type " + field_name + " into column with type " + type_string,
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    ensureOwnership();
    Arena & arena = createOrGetArena();
    pushBackAndCreateState(data, arena, func.get());
    ReadBufferFromString read_buffer(x.get<const AggregateFunctionStateData &>().data);
    func->deserialize(data.back(), read_buffer, &arena);
}

void ColumnAggregateFunction::insertDefault()
{
    ensureOwnership();
    Arena & arena = createOrGetArena();
    pushBackAndCreateState(data, arena, func.get());
}

StringRef ColumnAggregateFunction::serializeValueIntoArena(size_t n, Arena & arena, const char *& begin) const
{
    WriteBufferFromArena out(arena, begin);
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
      * Buf ReadBufferFromMemory requires some bound. We will use arbitrary big enough number, that will not overflow pointer.
      * NOTE Technically, this is not compatible with C++ standard,
      *  as we cannot legally compare pointers after last element + 1 of some valid memory region.
      *  Probably this will not work under UBSan.
      */
    ReadBufferFromMemory read_buffer(src_arena, std::numeric_limits<char *>::max() - src_arena - 1);
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
        size_t reserve_size = double(num_rows) / num_columns * 1.1; /// 1.1 is just a guess. Better to use n-sigma rule.

        if (reserve_size > 1)
            for (auto & column : columns)
                column->reserve(reserve_size);
    }

    for (size_t i = 0; i < num_rows; ++i)
        assert_cast<ColumnAggregateFunction &>(*columns[selector[i]]).data.push_back(data[i]);

    return columns;
}

void ColumnAggregateFunction::getPermutation(bool /*reverse*/, size_t /*limit*/, int /*nan_direction_hint*/, IColumn::Permutation & res) const
{
    size_t s = data.size();
    res.resize(s);
    for (size_t i = 0; i < s; ++i)
        res[i] = i;
}

void ColumnAggregateFunction::updatePermutation(bool, size_t, int, Permutation &, EqualRanges&) const {}

void ColumnAggregateFunction::gather(ColumnGathererStream & gatherer)
{
    gatherer.gather(*this);
}

void ColumnAggregateFunction::getExtremes(Field & min, Field & max) const
{
    /// Place serialized default values into min/max.

    AlignedBuffer place_buffer(func->sizeOfData(), func->alignOfData());
    AggregateDataPtr place = place_buffer.data();

    AggregateFunctionStateData serialized;
    serialized.name = type_string;

    func->create(place);
    try
    {
        WriteBufferFromString buffer(serialized.data);
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

ColumnAggregateFunction::MutablePtr ColumnAggregateFunction::createView() const
{
    auto res = create(func, concatArenas(foreign_arenas, my_arena));
    res->src = getPtr();
    return res;
}

ColumnAggregateFunction::ColumnAggregateFunction(const ColumnAggregateFunction & src_)
    : COWHelper<IColumn, ColumnAggregateFunction>(src_),
    foreign_arenas(concatArenas(src_.foreign_arenas, src_.my_arena)),
    func(src_.func), src(src_.getPtr()), data(src_.data.begin(), src_.data.end())
{
}

}
