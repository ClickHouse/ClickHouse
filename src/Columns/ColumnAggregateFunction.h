#pragma once

#include <AggregateFunctions/IAggregateFunction.h>

#include <Columns/IColumn.h>
#include <Common/PODArray.h>

#include <Core/Field.h>

#include <IO/ReadBufferFromString.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>

#include <Functions/FunctionHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

class Arena;
using ArenaPtr = std::shared_ptr<Arena>;
using ConstArenaPtr = std::shared_ptr<const Arena>;
using ConstArenas = std::vector<ConstArenaPtr>;


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
class ColumnAggregateFunction final : public COWHelper<IColumn, ColumnAggregateFunction>
{
public:
    using Container = PaddedPODArray<AggregateDataPtr>;

private:
    friend class COWHelper<IColumn, ColumnAggregateFunction>;

    /// Arenas used by function states that are created elsewhere. We own these
    /// arenas in the sense of extending their lifetime, but do not modify them.
    /// Even reading these arenas is unsafe, because they may be shared with
    /// other data blocks and modified by other threads concurrently.
    ConstArenas foreign_arenas;

    /// Arena for allocating the internals of function states created by current
    /// column (e.g., when inserting new states).
    ArenaPtr my_arena;

    /// Used for destroying states and for finalization of values.
    AggregateFunctionPtr func;

    /// Source column. Used (holds source from destruction),
    ///  if this column has been constructed from another and uses all or part of its values.
    ColumnPtr src;

    /// Array of pointers to aggregation states, that are placed in arenas.
    Container data;

    /// Name of the type to distinguish different aggregation states.
    String type_string;

    ColumnAggregateFunction() = default;

    /// Create a new column that has another column as a source.
    MutablePtr createView() const;

    /// If we have another column as a source (owner of data), copy all data to ourself and reset source.
    /// This is needed before inserting new elements, because we must own these elements (to destroy them in destructor),
    ///  but ownership of different elements cannot be mixed by different columns.
    void ensureOwnership();

    ColumnAggregateFunction(const AggregateFunctionPtr & func_);

    ColumnAggregateFunction(const AggregateFunctionPtr & func_,
                            const ConstArenas & arenas_);

    ColumnAggregateFunction(const ColumnAggregateFunction & src_);

public:
    ~ColumnAggregateFunction() override;

    void set(const AggregateFunctionPtr & func_);

    AggregateFunctionPtr getAggregateFunction() { return func; }
    AggregateFunctionPtr getAggregateFunction() const { return func; }

    /// Take shared ownership of Arena, that holds memory for states of aggregate functions.
    void addArena(ConstArenaPtr arena_);

    /// Transform column with states of aggregate functions to column with final result values.
    /// It expects ColumnAggregateFunction as an argument, this column will be destroyed.
    /// This method is made static and receive MutableColumnPtr object to explicitly destroy it.
    static MutableColumnPtr convertToValues(MutableColumnPtr column);

    std::string getName() const override { return "AggregateFunction(" + func->getName() + ")"; }
    const char * getFamilyName() const override { return "AggregateFunction"; }
    TypeIndex getDataType() const override { return TypeIndex::AggregateFunction; }

    MutableColumnPtr predictValues(const ColumnsWithTypeAndName & arguments, ContextPtr context) const;

    size_t size() const override
    {
        return getData().size();
    }

    MutableColumnPtr cloneEmpty() const override;

    Field operator[](size_t n) const override;

    void get(size_t n, Field & res) const override;

    StringRef getDataAt(size_t n) const override;

    void insertData(const char * pos, size_t length) override;

    void insertFrom(const IColumn & from, size_t n) override;

    void insertFrom(ConstAggregateDataPtr place);

    /// Merge state at last row with specified state in another column.
    void insertMergeFrom(ConstAggregateDataPtr place);

    void insertMergeFrom(const IColumn & from, size_t n);

    Arena & createOrGetArena();

    void insert(const Field & x) override;

    void insertDefault() override;

    StringRef serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const override;

    const char * deserializeAndInsertFromArena(const char * src_arena) override;

    const char * skipSerializedInArena(const char *) const override;

    void updateHashWithValue(size_t n, SipHash & hash) const override;

    void updateWeakHash32(WeakHash32 & hash) const override;

    void updateHashFast(SipHash & hash) const override;

    size_t byteSize() const override;

    size_t byteSizeAt(size_t n) const override;

    size_t allocatedBytes() const override;

    void protect() override;

    void insertRangeFrom(const IColumn & from, size_t start, size_t length) override;

    void popBack(size_t n) override;

    ColumnPtr filter(const Filter & filter, ssize_t result_size_hint) const override;

    ColumnPtr permute(const Permutation & perm, size_t limit) const override;

    ColumnPtr index(const IColumn & indexes, size_t limit) const override;

    template <typename Type>
    ColumnPtr indexImpl(const PaddedPODArray<Type> & indexes, size_t limit) const;

    ColumnPtr replicate(const Offsets & offsets) const override;

    MutableColumns scatter(ColumnIndex num_columns, const Selector & selector) const override;

    void gather(ColumnGathererStream & gatherer_stream) override;

    int compareAt(size_t, size_t, const IColumn &, int) const override
    {
        return 0;
    }

    void compareColumn(const IColumn &, size_t, PaddedPODArray<UInt64> *, PaddedPODArray<Int8> &, int, int) const override
    {
        throw Exception("Method compareColumn is not supported for ColumnAggregateFunction", ErrorCodes::NOT_IMPLEMENTED);
    }

    bool hasEqualValues() const override
    {
        throw Exception("Method hasEqualValues is not supported for ColumnAggregateFunction", ErrorCodes::NOT_IMPLEMENTED);
    }

    void getPermutation(bool reverse, size_t limit, int nan_direction_hint, Permutation & res) const override;
    void updatePermutation(bool reverse, size_t limit, int, Permutation & res, EqualRanges & equal_range) const override;

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

    bool structureEquals(const IColumn &) const override;

    MutableColumnPtr cloneResized(size_t size) const override;
};
}
