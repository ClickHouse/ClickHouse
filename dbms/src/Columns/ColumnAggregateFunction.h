#pragma once

#include <Common/Arena.h>

#include <AggregateFunctions/IAggregateFunction.h>

#include <Columns/IColumn.h>

#include <Core/Field.h>

#include <IO/ReadBufferFromString.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>


namespace DB
{


/** Column of states of aggregate functions.
  * Presented as an array of pointers to the states of aggregate functions (data).
  * The states themselves are stored in one of the pools (arenas).
  *
  * It can be in two variants:
  *
  * 1. Own its values - that is, be responsible for destroying them.
  * The column consists of the values "assigned to it" after the aggregation is performed (see Aggregator, convertToBlocks function),
  *  or from values created by itself (see `insert` method).
  * In this case, `src` will be `nullptr`, and the column itself will be destroyed (call `IAggregateFunction::destroy`)
  *  states of aggregate functions in the destructor.
  *
  * 2. Do not own its values, but use values taken from another ColumnAggregateFunction column.
  * For example, this is a column obtained by permutation/filtering or other transformations from another column.
  * In this case, `src` will be `shared ptr` to the source column. Destruction of values will be handled by this source column.
  *
  * This solution is somewhat limited:
  * - the variant in which the column contains a part of "it's own" and a part of "another's" values is not supported;
  * - the option of having multiple source columns is not supported, which may be necessary for a more optimal merge of the two columns.
  *
  * These restrictions can be removed if you add an array of flags or even refcount,
  *  specifying which individual values should be destroyed and which ones should not.
  * Clearly, this method would have a substantially non-zero price.
  */
class ColumnAggregateFunction final : public COWPtrHelper<IColumn, ColumnAggregateFunction>
{
public:
    using Container = PaddedPODArray<AggregateDataPtr>;

private:
    friend class COWPtrHelper<IColumn, ColumnAggregateFunction>;

    /// Memory pools. Aggregate states are allocated from them.
    Arenas arenas;

    /// Used for destroying states and for finalization of values.
    AggregateFunctionPtr func;

    /// Source column. Used (holds source from destruction),
    ///  if this column has been constructed from another and uses all or part of its values.
    ColumnPtr src;

    /// Array of pointers to aggregation states, that are placed in arenas.
    Container data;

    ColumnAggregateFunction() {}

    /// Create a new column that has another column as a source.
    MutablePtr createView() const
    {
        MutablePtr res = create(func, arenas);
        res->src = getPtr();
        return res;
    }

    ColumnAggregateFunction(const AggregateFunctionPtr & func_)
        : func(func_)
    {
    }

    ColumnAggregateFunction(const AggregateFunctionPtr & func_, const Arenas & arenas_)
        : arenas(arenas_), func(func_)
    {
    }

    ColumnAggregateFunction(const ColumnAggregateFunction & src_)
        : arenas(src_.arenas), func(src_.func), src(src_.getPtr()), data(src_.data.begin(), src_.data.end())
    {
    }

public:
    ~ColumnAggregateFunction();

    void set(const AggregateFunctionPtr & func_)
    {
        func = func_;
    }

    AggregateFunctionPtr getAggregateFunction() { return func; }
    AggregateFunctionPtr getAggregateFunction() const { return func; }

    /// Take shared ownership of Arena, that holds memory for states of aggregate functions.
    void addArena(ArenaPtr arena_);

    /** Transform column with states of aggregate functions to column with final result values.
      */
    MutableColumnPtr convertToValues() const;

    std::string getName() const override { return "AggregateFunction(" + func->getName() + ")"; }
    const char * getFamilyName() const override { return "AggregateFunction"; }

    size_t size() const override
    {
        return getData().size();
    }

    MutableColumnPtr cloneEmpty() const override;

    Field operator[](size_t n) const override;

    void get(size_t n, Field & res) const override;

    StringRef getDataAt(size_t n) const override;

    void insertData(const char * pos, size_t length) override;

    void insertFrom(const IColumn & src, size_t n) override;

    void insertFrom(ConstAggregateDataPtr place);

    /// Merge state at last row with specified state in another column.
    void insertMergeFrom(ConstAggregateDataPtr place);

    void insertMergeFrom(const IColumn & src, size_t n);

    Arena & createOrGetArena();

    void insert(const Field & x) override;

    void insertDefault() override;

    StringRef serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const override;

    const char * deserializeAndInsertFromArena(const char * pos) override;

    void updateHashWithValue(size_t n, SipHash & hash) const override;

    size_t byteSize() const override;

    size_t allocatedBytes() const override;

    void insertRangeFrom(const IColumn & from, size_t start, size_t length) override;

    void popBack(size_t n) override;

    MutableColumnPtr filter(const Filter & filter, ssize_t result_size_hint) const override;

    MutableColumnPtr permute(const Permutation & perm, size_t limit) const override;

    MutableColumnPtr replicate(const Offsets & offsets) const override;

    MutableColumns scatter(ColumnIndex num_columns, const Selector & selector) const override;

    void gather(ColumnGathererStream & gatherer_stream) override;

    int compareAt(size_t, size_t, const IColumn &, int) const override
    {
        return 0;
    }

    void getPermutation(bool reverse, size_t limit, int nan_direction_hint, Permutation & res) const override;

    /** More efficient manipulation methods */
    Container & getData()
    {
        return data;
    }

    const Container & getData() const
    {
        return data;
    }

    void getExtremes(Field & min, Field & max) const override;
};


}
