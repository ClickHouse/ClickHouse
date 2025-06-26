#pragma once

#include <Columns/IColumn_fwd.h>
#include <Core/TypeId.h>
#include <base/StringRef.h>
#include <Common/COW.h>
#include <Common/PODArray_fwd.h>
#include <Common/typeid_cast.h>

#include "config.h"

class SipHash;
class Collator;

namespace llvm
{
    class LLVMContext;
    class Value;
    class IRBuilderBase;
}

namespace DB
{

class Arena;
class ColumnGathererStream;
class Field;
class WeakHash32;
class ColumnConst;
class IDataType;
class Block;
using DataTypePtr = std::shared_ptr<const IDataType>;
using IColumnPermutation = PaddedPODArray<size_t>;
using IColumnFilter = PaddedPODArray<UInt8>;

/// A range of column values between row indexes `from` and `to`. The name "equal range" is due to table sorting as its main use case: With
/// a PRIMARY KEY (c_pk1, c_pk2, ...), the first PK column is fully sorted. The second PK column is sorted within equal-value runs of the
/// first PK column, and so on. The number of runs (ranges) per column increases from one primary key column to the next. An "equal range"
/// is a run in a previous column, within the values of the current column can be sorted.
struct EqualRange
{
    size_t from;   /// inclusive
    size_t to;     /// exclusive
    EqualRange(size_t from_, size_t to_) : from(from_), to(to_) { chassert(from <= to); }
    size_t size() const { return to - from; }
};

/// A checkpoint that contains size of column and all its subcolumns.
/// It can be used to rollback column to the previous state, for example
/// after failed parsing when column may be in inconsistent state.
struct ColumnCheckpoint
{
    size_t size;

    explicit ColumnCheckpoint(size_t size_) : size(size_) {}
    virtual ~ColumnCheckpoint() = default;
};

struct ColumnCheckpointWithNested : public ColumnCheckpoint
{
    ColumnCheckpointWithNested(size_t size_, ColumnCheckpointPtr nested_)
        : ColumnCheckpoint(size_), nested(std::move(nested_))
    {
    }

    ColumnCheckpointPtr nested;
};

struct ColumnCheckpointWithMultipleNested : public ColumnCheckpoint
{
    ColumnCheckpointWithMultipleNested(size_t size_, ColumnCheckpoints nested_)
        : ColumnCheckpoint(size_), nested(std::move(nested_))
    {
    }

    ColumnCheckpoints nested;
};

/// Declares interface to store columns in memory.
class IColumn : public COW<IColumn>
{
private:
    friend class COW<IColumn>;

    /// Creates the same column with the same data.
    /// This is internal method to use from COW.
    /// It performs shallow copy with copy-ctor and not useful from outside.
    /// If you want to copy column for modification, look at 'mutate' method.
    [[nodiscard]] virtual MutablePtr clone() const = 0;

public:
    /// Name of a Column. It is used in info messages.
    [[nodiscard]] virtual std::string getName() const { return getFamilyName(); }

    /// Name of a Column kind, without parameters (example: FixedString, Array).
    [[nodiscard]] virtual const char * getFamilyName() const = 0;

    /// Type of data that column contains. It's an underlying type: UInt16 for Date, UInt32 for DateTime, so on.
    [[nodiscard]] virtual TypeIndex getDataType() const = 0;

    /** If column isn't constant, returns itself.
      * If column is constant, transforms constant to full column (if column type allows such transform) and return it.
      */
    [[nodiscard]] virtual Ptr convertToFullColumnIfConst() const { return getPtr(); }

    /// If column isn't ColumnLowCardinality, return itself.
    /// If column is ColumnLowCardinality, transforms it to full column.
    [[nodiscard]] virtual Ptr convertToFullColumnIfLowCardinality() const { return getPtr(); }

    /// If column isn't ColumnSparse, return itself.
    /// If column is ColumnSparse, transforms it to full column.
    [[nodiscard]] virtual Ptr convertToFullColumnIfSparse() const { return getPtr(); }

    [[nodiscard]] Ptr convertToFullIfNeeded() const
    {
        return convertToFullColumnIfSparse()->convertToFullColumnIfConst()->convertToFullColumnIfLowCardinality();
    }

    /// Creates empty column with the same type.
    [[nodiscard]] virtual MutablePtr cloneEmpty() const { return cloneResized(0); }

    /// Creates column with the same type and specified size.
    /// If size is less than current size, then data is cut.
    /// If size is greater, then default values are appended.
    [[nodiscard]] virtual MutablePtr cloneResized(size_t /*size*/) const;

    /// Returns number of values in column.
    [[nodiscard]] virtual size_t size() const = 0;

    /// There are no values in columns.
    [[nodiscard]] bool empty() const { return size() == 0; }

    /// Returns value of n-th element in universal Field representation.
    /// Is used in rare cases, since creation of Field instance is expensive usually.
    [[nodiscard]] virtual Field operator[](size_t n) const = 0;

    /// Like the previous one, but avoids extra copying if Field is in a container, for example.
    virtual void get(size_t n, Field & res) const = 0;

    virtual std::pair<String, DataTypePtr> getValueNameAndType(size_t) const = 0;

    /// If possible, returns pointer to memory chunk which contains n-th element (if it isn't possible, throws an exception)
    /// Is used to optimize some computations (in aggregation, for example).
    [[nodiscard]] virtual StringRef getDataAt(size_t n) const = 0;

    /// If column stores integers, it returns n-th element transformed to UInt64 using static_cast.
    /// If column stores floating point numbers, bits of n-th elements are copied to lower bits of UInt64, the remaining bits are zeros.
    /// Is used to optimize some computations (in aggregation, for example).
    [[nodiscard]] virtual UInt64 get64(size_t /*n*/) const;

    /// If column stores native numeric type, it returns n-th element cast to Float64
    /// Is used in regression methods to cast each features into uniform type
    [[nodiscard]] virtual Float64 getFloat64(size_t /*n*/) const;

    [[nodiscard]] virtual Float32 getFloat32(size_t /*n*/) const;

    /** If column is numeric, return value of n-th element, cast to UInt64.
      * For NULL values of Nullable column it is allowed to return arbitrary value.
      * Otherwise throw an exception.
      */
    [[nodiscard]] virtual UInt64 getUInt(size_t /*n*/) const;

    [[nodiscard]] virtual Int64 getInt(size_t /*n*/) const;

    [[nodiscard]] virtual bool isDefaultAt(size_t n) const = 0;
    [[nodiscard]] virtual bool isNullAt(size_t /*n*/) const { return false; }

    /** If column is numeric, return value of n-th element, cast to bool.
      * For NULL values of Nullable column returns false.
      * Otherwise throw an exception.
      */
    [[nodiscard]] virtual bool getBool(size_t /*n*/) const;

    /// Removes all elements outside of specified range.
    /// Is used in LIMIT operation, for example.
    [[nodiscard]] virtual Ptr cut(size_t start, size_t length) const
    {
        MutablePtr res = cloneEmpty();
        res->insertRangeFrom(*this, start, length);
        return res;
    }

    /// Appends new value at the end of column (column's size is increased by 1).
    /// Is used to transform raw strings to Blocks (for example, inside input format parsers)
    virtual void insert(const Field & x) = 0;

    /// Appends new value at the end of the column if it has appropriate type and
    /// returns true if insert is successful and false otherwise.
    virtual bool tryInsert(const Field & x) = 0;

    /// Appends n-th element from other column with the same type.
    /// Is used in merge-sort and merges. It could be implemented in inherited classes more optimally than default implementation.
#if !defined(DEBUG_OR_SANITIZER_BUILD)
    virtual void insertFrom(const IColumn & src, size_t n);
#else
    void insertFrom(const IColumn & src, size_t n)
    {
        assertTypeEquality(src);
        doInsertFrom(src, n);
    }
#endif

    /// Appends range of elements from other column with the same type.
    /// Could be used to concatenate columns.
#if !defined(DEBUG_OR_SANITIZER_BUILD)
    virtual void insertRangeFrom(const IColumn & src, size_t start, size_t length) = 0;
#else
    void insertRangeFrom(const IColumn & src, size_t start, size_t length)
    {
        assertTypeEquality(src);
        doInsertRangeFrom(src, start, length);
    }
#endif

    /// Appends one element from other column with the same type multiple times.
#if !defined(DEBUG_OR_SANITIZER_BUILD)
    virtual void insertManyFrom(const IColumn & src, size_t position, size_t length)
    {
        for (size_t i = 0; i < length; ++i)
            insertFrom(src, position);
    }
#else
    void insertManyFrom(const IColumn & src, size_t position, size_t length)
    {
        assertTypeEquality(src);
        doInsertManyFrom(src, position, length);
    }
#endif

    /// Appends one field multiple times. Can be optimized in inherited classes.
    virtual void insertMany(const Field & field, size_t length)
    {
        for (size_t i = 0; i < length; ++i)
            insert(field);
    }

    /// Appends data located in specified memory chunk if it is possible (throws an exception if it cannot be implemented).
    /// Is used to optimize some computations (in aggregation, for example).
    /// Parameter length could be ignored if column values have fixed size.
    /// All data will be inserted as single element
    virtual void insertData(const char * pos, size_t length) = 0;

    /// Appends "default value".
    /// Is used when there are need to increase column size, but inserting value doesn't make sense.
    /// For example, ColumnNullable(Nested) absolutely ignores values of nested column if it is marked as NULL.
    virtual void insertDefault() = 0;

    /// Appends "default value" multiple times.
    virtual void insertManyDefaults(size_t length)
    {
        for (size_t i = 0; i < length; ++i)
            insertDefault();
    }

    /** Removes last n elements.
      * Is used to support exception-safety of several operations.
      *  For example, sometimes insertion should be reverted if we catch an exception during operation processing.
      * If column has less than n elements or n == 0 - undefined behavior.
      */
    virtual void popBack(size_t n) = 0;

    /** Serializes n-th element. Serialized element should be placed continuously inside Arena's memory.
      * Serialized value can be deserialized to reconstruct original object. Is used in aggregation.
      * The method is similar to getDataAt(), but can work when element's value cannot be mapped to existing continuous memory chunk,
      *  For example, to obtain unambiguous representation of Array of strings, strings data should be interleaved with their sizes.
      * Parameter begin should be used with Arena::allocContinue.
      */
    virtual StringRef serializeValueIntoArena(size_t /* n */, Arena & /* arena */, char const *& /* begin */) const;

    /// Same as above but serialize into already allocated continuous memory.
    /// Return pointer to the end of the serialization data.
    virtual char * serializeValueIntoMemory(size_t /* n */, char * /* memory */) const;

    /// Nullable variant to avoid calling virtualized method inside ColumnNullable.
    virtual StringRef
    serializeValueIntoArenaWithNull(size_t /* n */, Arena & /* arena */, char const *& /* begin */, const UInt8 * /* is_null */) const;

    virtual char * serializeValueIntoMemoryWithNull(size_t /* n */, char * /* memory */, const UInt8 * /* is_null */) const;

    /// Calculate all the sizes of serialized data in column, then added to `sizes`.
    /// If `is_null` is not nullptr, also take null bit into account.
    /// This is currently used to facilitate the allocation of memory for an entire continuous row
    /// in a single step. For more details, refer to the HashMethodSerialized implementation.
    virtual void collectSerializedValueSizes(PaddedPODArray<UInt64> & /* sizes */, const UInt8 * /* is_null */) const;

    /// Deserializes a value that was serialized using IColumn::serializeValueIntoArena method.
    /// Returns pointer to the position after the read data.
    [[nodiscard]] virtual const char * deserializeAndInsertFromArena(const char * pos) = 0;

    /// Skip previously serialized value that was serialized using IColumn::serializeValueIntoArena method.
    /// Returns a pointer to the position after the deserialized data.
    [[nodiscard]] virtual const char * skipSerializedInArena(const char *) const = 0;

    /// Update state of hash function with value of n-th element.
    /// On subsequent calls of this method for sequence of column values of arbitrary types,
    ///  passed bytes to hash must identify sequence of values unambiguously.
    virtual void updateHashWithValue(size_t n, SipHash & hash) const = 0;

    /// Get hash function value. Hash is calculated for each element.
    /// It's a fast weak hash function. Mainly need to scatter data between threads.
    /// WeakHash32 must have the same size as column.
    virtual WeakHash32 getWeakHash32() const = 0;

    /// Update state of hash with all column.
    virtual void updateHashFast(SipHash & hash) const = 0;

    /** Removes elements that don't match the filter.
      * Is used in WHERE and HAVING operations.
      * If result_size_hint > 0, then makes advance reserve(result_size_hint) for the result column;
      * if 0, then don't makes reserve(),
      * otherwise (i.e. < 0), makes reserve() using size of source column.
      */
    using Filter = IColumnFilter;
    [[nodiscard]] virtual Ptr filter(const Filter & filt, ssize_t result_size_hint) const = 0;

    /** Expand column by mask inplace. After expanding column will
      * satisfy the following: if we filter it by given mask, we will
      * get initial column. Values with indexes i: mask[i] = 0
      * shouldn't be used after expanding.
      * If inverted is true, inverted mask will be used.
      */
    virtual void expand(const Filter & /*mask*/, bool /*inverted*/) = 0;

    /// Permutes elements using specified permutation. Is used in sorting.
    /// limit - if it isn't 0, puts only first limit elements in the result.
    using Permutation = IColumnPermutation;
    [[nodiscard]] virtual Ptr permute(const Permutation & perm, size_t limit) const = 0;

    /// Creates new column with values column[indexes[:limit]]. If limit is 0, all indexes are used.
    /// Indexes must be one of the ColumnUInt. For default implementation, see selectIndexImpl from ColumnsCommon.h
    [[nodiscard]] virtual Ptr index(const IColumn & indexes, size_t limit) const = 0;

    /** Compares (*this)[n] and rhs[m]. Column rhs should have the same type.
      * Returns negative number, 0, or positive number (*this)[n] is less, equal, greater than rhs[m] respectively.
      * Is used in sorting.
      *
      * If one of element's value is NaN or NULLs, then:
      * - if nan_direction_hint == -1, NaN and NULLs are considered as least than everything other;
      * - if nan_direction_hint ==  1, NaN and NULLs are considered as greatest than everything other.
      * For example, if nan_direction_hint == -1 is used by descending sorting, NaNs will be at the end.
      *
      * For non Nullable and non floating point types, nan_direction_hint is ignored.
      */
#if !defined(DEBUG_OR_SANITIZER_BUILD)
    [[nodiscard]] virtual int compareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const = 0;
#else
    [[nodiscard]] int compareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const
    {
        assertTypeEquality(rhs);
        return doCompareAt(n, m, rhs, nan_direction_hint);
    }
#endif

#if USE_EMBEDDED_COMPILER

    [[nodiscard]] virtual bool isComparatorCompilable() const { return false; }

    [[nodiscard]] virtual llvm::Value * compileComparator(
        llvm::IRBuilderBase & /*builder*/, llvm::Value * /*lhs*/, llvm::Value * /*rhs*/, llvm::Value * /*nan_direction_hint*/) const;

#endif

    /// Equivalent to compareAt, but collator is used to compare values.
    [[nodiscard]] virtual int compareAtWithCollation(size_t, size_t, const IColumn &, int, const Collator &) const;

    /// Compare the whole column with single value from rhs column.
    /// If row_indexes is nullptr, it's ignored. Otherwise, it is a set of rows to compare.
    /// compare_results[i] will be equal to compareAt(row_indexes[i], rhs_row_num, rhs, nan_direction_hint) * direction
    /// row_indexes (if not ignored) will contain row numbers for which compare result is 0
    /// see compareImpl for default implementation.
    virtual void compareColumn(const IColumn & rhs, size_t rhs_row_num,
                               PaddedPODArray<UInt64> * row_indexes, PaddedPODArray<Int8> & compare_results,
                               int direction, int nan_direction_hint) const = 0;

    /// Check if all elements in the column have equal values. Return true if column is empty.
    [[nodiscard]] virtual bool hasEqualValues() const = 0;

    enum class PermutationSortDirection : uint8_t
    {
        Ascending = 0,
        Descending
    };

    enum class PermutationSortStability : uint8_t
    {
        Unstable = 0,
        Stable
    };

    /** Returns a permutation that sorts elements of this column,
      *  i.e. perm[i]-th element of source column should be i-th element of sorted column.
      * direction - permutation direction.
      * stability - stability of result permutation.
      * limit - if isn't 0, then only first limit elements of the result column could be sorted.
      * nan_direction_hint - see above.
      */
    virtual void getPermutation(PermutationSortDirection direction, PermutationSortStability stability,
                            size_t limit, int nan_direction_hint, Permutation & res) const = 0;

    /*in updatePermutation we pass the current permutation and the intervals at which it should be sorted
     * Then for each interval separately (except for the last one, if there is a limit)
     * We sort it based on data about the current column, and find all the intervals within this
     * interval that had the same values in this column. we can't tell about these values in what order they
     * should have been, we form a new array with intervals that need to be sorted
     * If there is a limit, then for the last interval we do partial sorting and all that is described above,
     * but in addition we still find all the elements equal to the largest sorted, they will also need to be sorted.
     */
    virtual void updatePermutation(PermutationSortDirection direction, PermutationSortStability stability,
                            size_t limit, int nan_direction_hint, Permutation & res, EqualRanges & equal_ranges) const = 0;

    /** Equivalent to getPermutation and updatePermutation but collator is used to compare values.
      * Supported for String, LowCardinality(String), Nullable(String) and for Array and Tuple, containing them.
      */
    virtual void getPermutationWithCollation(
        const Collator & /*collator*/,
        PermutationSortDirection /*direction*/,
        PermutationSortStability /*stability*/,
        size_t /*limit*/,
        int /*nan_direction_hint*/,
        Permutation & /*res*/) const;

    virtual void updatePermutationWithCollation(
        const Collator & /*collator*/,
        PermutationSortDirection /*direction*/,
        PermutationSortStability /*stability*/,
        size_t /*limit*/,
        int /*nan_direction_hint*/,
        Permutation & /*res*/,
        EqualRanges & /*equal_ranges*/) const;

    /// Estimate the cardinality (number of unique values) of the values in 'equal_range' after permutation, formally: |{ column[permutation[r]] : r in equal_range }|.
    virtual size_t estimateCardinalityInPermutedRange(const Permutation & permutation, const EqualRange & equal_range) const;

    /** Copies each element according offsets parameter.
      * (i-th element should be copied offsets[i] - offsets[i - 1] times.)
      * It is necessary in ARRAY JOIN operation.
      */
    using Offset = UInt64;
    using Offsets = PaddedPODArray<Offset>;
    [[nodiscard]] virtual Ptr replicate(const Offsets & offsets) const = 0;

    /** Split column to smaller columns. Each value goes to column index, selected by corresponding element of 'selector'.
      * Selector must contain values from 0 to num_columns - 1.
      * For default implementation, see scatterImpl.
      */
    using ColumnIndex = UInt64;
    using Selector = PaddedPODArray<ColumnIndex>;
    [[nodiscard]] virtual std::vector<MutablePtr> scatter(ColumnIndex num_columns, const Selector & selector) const = 0;

    /// Insert data from several other columns according to source mask (used in vertical merge).
    /// For now it is a helper to de-virtualize calls to insert*() functions inside gather loop
    /// (descendants should call gatherer_stream.gather(*this) to implement this function.)
    /// TODO: interface decoupled from ColumnGathererStream that allows non-generic specializations.
    virtual void gather(ColumnGathererStream & gatherer_stream) = 0;

    /** Computes minimum and maximum element of the column.
      * In addition to numeric types, the function is completely implemented for Date and DateTime.
      * For strings and arrays function should return default value.
      *  (except for constant columns; they should return value of the constant).
      * If column is empty function should return default value.
      */
    virtual void getExtremes(Field & min, Field & max) const = 0;

    /// Reserves memory for specified amount of elements. If reservation isn't possible, does nothing.
    /// It affects performance only (not correctness).
    virtual void reserve(size_t /*n*/) {}

    /// Returns the number of elements allocated in reserve.
    virtual size_t capacity() const { return size(); }

    /// Reserve memory before squashing all specified source columns into this column.
    virtual void prepareForSquashing(const std::vector<Ptr> & source_columns, size_t factor)
    {
        size_t new_size = size();
        for (const auto & source_column : source_columns)
            new_size += source_column->size();
        reserve(new_size * factor);
    }

    /// Requests the removal of unused capacity.
    /// It is a non-binding request to reduce the capacity of the underlying container to its size.
    virtual void shrinkToFit() {}

    /// If we have another column as a source (owner of data), copy all data to ourself and reset source.
    virtual void ensureOwnership() {}

    /// Size of column data in memory (may be approximate) - for profiling. Zero, if could not be determined.
    [[nodiscard]] virtual size_t byteSize() const = 0;

    /// Size of single value in memory (for accounting purposes)
    [[nodiscard]] virtual size_t byteSizeAt(size_t /*n*/) const = 0;

    /// Size of memory, allocated for column.
    /// This is greater or equals to byteSize due to memory reservation in containers.
    /// Zero, if could not be determined.
    [[nodiscard]] virtual size_t allocatedBytes() const = 0;

    /// Make memory region readonly with mprotect if it is large enough.
    /// The operation is slow and performed only for debug builds.
    virtual void protect() {}

    /// Returns checkpoint of current state of column.
    virtual ColumnCheckpointPtr getCheckpoint() const { return std::make_shared<ColumnCheckpoint>(size()); }

    /// Updates the checkpoint with current state. It is used to avoid extra allocations in 'getCheckpoint'.
    virtual void updateCheckpoint(ColumnCheckpoint & checkpoint) const { checkpoint.size = size(); }

    /// Rollbacks column to the checkpoint.
    /// Unlike 'popBack' this method should work correctly even if column has invalid state.
    /// Sizes of columns in checkpoint must be less or equal than current size.
    virtual void rollback(const ColumnCheckpoint & checkpoint) { popBack(size() - checkpoint.size); }

    /// If the column contains subcolumns (such as Array, Nullable, etc), do callback on them.
    /// Shallow: doesn't do recursive calls; don't do call for itself.

    using MutableColumnCallback = std::function<void(WrappedPtr &)>;
    virtual void forEachMutableSubcolumn(MutableColumnCallback) {}

    /// Note: If you implement forEachSubcolumn(Recursively), you must also implement forEachMutableSubcolumn(Recursively), and vice versa
    using ColumnCallback = std::function<void(const WrappedPtr &)>;
    virtual void forEachSubcolumn(ColumnCallback) const {}

    /// Similar to forEachSubcolumn but it also do recursive calls.
    /// In recursive calls it's prohibited to replace pointers
    /// to subcolumns, so we use another callback function.

    using RecursiveMutableColumnCallback = std::function<void(IColumn &)>;
    virtual void forEachMutableSubcolumnRecursively(RecursiveMutableColumnCallback) {}

    /// Default implementation calls the mutable overload using const_cast.
    using RecursiveColumnCallback = std::function<void(const IColumn &)>;
    virtual void forEachSubcolumnRecursively(RecursiveColumnCallback) const {}

    /// Columns have equal structure.
    /// If true - you can use "compareAt", "insertFrom", etc. methods.
    [[nodiscard]] virtual bool structureEquals(const IColumn &) const;

    /// Returns ratio of values in column, that are equal to default value of column.
    /// Checks only @sample_ratio ratio of rows.
    [[nodiscard]] virtual double getRatioOfDefaultRows(double sample_ratio = 1.0) const = 0; /// NOLINT

    /// Returns number of values in column, that are equal to default value of column.
    [[nodiscard]] virtual UInt64 getNumberOfDefaultRows() const = 0;

    /// Returns indices of values in column, that not equal to default value of column.
    virtual void getIndicesOfNonDefaultRows(Offsets & indices, size_t from, size_t limit) const = 0;

    /// Returns column with @total_size elements.
    /// In result column values from current column are at positions from @offsets.
    /// Other values are filled by value from @column_with_default_value.
    /// @shift means how much rows to skip from the beginning of current column.
    /// Used to create full column from sparse.
    [[nodiscard]] virtual Ptr createWithOffsets(const Offsets & offsets, const ColumnConst & column_with_default_value, size_t total_rows, size_t shift) const;

    /// Compress column in memory to some representation that allows to decompress it back.
    /// Return itself if compression is not applicable for this column type.
    /// The flag `force_compression` indicates that compression should be performed even if it's not efficient (if only compression factor < 1).
    [[nodiscard]] virtual Ptr compress([[maybe_unused]] bool force_compression) const
    {
        /// No compression by default.
        return getPtr();
    }

    /// If it's CompressedColumn, decompress it and return.
    /// Otherwise return itself.
    [[nodiscard]] virtual Ptr decompress() const
    {
        return getPtr();
    }

    /// Fills column values from RowRefList
    /// If row_refs_are_ranges is true, then each RowRefList has one element with >=1 consecutive rows
    virtual void fillFromRowRefs(const DataTypePtr & type, size_t source_column_index_in_block, const PaddedPODArray<UInt64> & row_refs, bool row_refs_are_ranges);

    /// Fills column values from list of blocks and row numbers
    /// `blocks` and `row_nums` must have same size
    virtual void fillFromBlocksAndRowNumbers(const DataTypePtr & type, size_t source_column_index_in_block, const std::vector<const Columns *> & columns, const std::vector<UInt32> & row_nums);

    /// Some columns may require finalization before using of other operations.
    virtual void finalize() {}
    virtual bool isFinalized() const { return true; }

    MutablePtr cloneFinalized() const
    {
        auto finalized = IColumn::mutate(getPtr());
        finalized->finalize();
        return finalized;
    }

    [[nodiscard]] static MutablePtr mutate(Ptr ptr)
    {
        MutablePtr res = ptr->shallowMutate(); /// Now use_count is 2.
        ptr.reset(); /// Reset use_count to 1.
        res->forEachMutableSubcolumn([](WrappedPtr & subcolumn) { subcolumn = IColumn::mutate(std::move(subcolumn).detach()); });
        return res;
    }

    /// Checks if column has dynamic subcolumns.
    virtual bool hasDynamicStructure() const { return false; }

    /// For columns with dynamic subcolumns checks if columns have equal dynamic structure.
    [[nodiscard]] virtual bool dynamicStructureEquals(const IColumn & rhs) const { return structureEquals(rhs); }
    /// For columns with dynamic subcolumns this method takes dynamic structure from source columns
    /// and creates proper resulting dynamic structure in advance for merge of these source columns.
    virtual void takeDynamicStructureFromSourceColumns(const std::vector<Ptr> & /*source_columns*/) {}

    /** Some columns can contain another columns inside.
      * So, we have a tree of columns. But not all combinations are possible.
      * There are the following rules:
      *
      * ColumnConst may be only at top. It cannot be inside any column.
      * ColumnNullable can contain only simple columns.
      */

    /// Various properties on behaviour of column type.

    /// True if column contains something nullable inside. It's true for ColumnNullable, can be true or false for ColumnConst, etc.
    [[nodiscard]] virtual bool isNullable() const { return false; }

    /// It's a special kind of column, that contain single value, but is not a ColumnConst.
    [[nodiscard]] virtual bool isDummy() const { return false; }

    /** Memory layout properties.
      *
      * Each value of a column can be placed in memory contiguously or not.
      *
      * Example: simple columns like UInt64 or FixedString store their values contiguously in single memory buffer.
      *
      * Example: Tuple store values of each component in separate subcolumn, so the values of Tuples with at least two components are not contiguous.
      * Another example is Nullable. Each value have null flag, that is stored separately, so the value is not contiguous in memory.
      *
      * There are some important cases, when values are not stored contiguously, but for each value, you can get contiguous memory segment,
      *  that will unambiguously identify the value. In this case, methods getDataAt and insertData are implemented.
      * Example: String column: bytes of strings are stored concatenated in one memory buffer
      *  and offsets to that buffer are stored in another buffer. The same is for Array of fixed-size contiguous elements.
      *
      * To avoid confusion between these cases, we don't have isContiguous method.
      */

    /// Values in column have fixed size (including the case when values span many memory segments).
    [[nodiscard]] virtual bool valuesHaveFixedSize() const { return isFixedAndContiguous(); }

    /// Values in column are represented as continuous memory segment of fixed size. Implies valuesHaveFixedSize.
    [[nodiscard]] virtual bool isFixedAndContiguous() const { return false; }

    /// If isFixedAndContiguous, returns the underlying data array, otherwise throws an exception.
    [[nodiscard]] virtual std::string_view getRawData() const;

    /// If valuesHaveFixedSize, returns size of value, otherwise throw an exception.
    [[nodiscard]] virtual size_t sizeOfValueIfFixed() const;

    /// Column is ColumnVector of numbers or ColumnConst of it. Note that Nullable columns are not numeric.
    [[nodiscard]] virtual bool isNumeric() const { return false; }

    /// If the only value column can contain is NULL.
    /// Does not imply type of object, because it can be ColumnNullable(ColumnNothing) or ColumnConst(ColumnNullable(ColumnNothing))
    [[nodiscard]] virtual bool onlyNull() const { return false; }

    /// Can be inside ColumnNullable.
    [[nodiscard]] virtual bool canBeInsideNullable() const { return false; }

    [[nodiscard]] virtual bool lowCardinality() const { return false; }

    [[nodiscard]] virtual bool isSparse() const { return false; }

    [[nodiscard]] virtual bool isConst() const { return false; }

    [[nodiscard]] virtual bool isCollationSupported() const { return false; }

    virtual ~IColumn() = default;
    IColumn() = default;
    IColumn(const IColumn &) = default;

    /** Print column name, size, and recursively print all subcolumns.
      */
    [[nodiscard]] String dumpStructure() const;

protected:
    template <typename Compare, typename Sort, typename PartialSort>
    void getPermutationImpl(size_t limit, Permutation & res, Compare compare, Sort full_sort, PartialSort partial_sort) const;

    template <typename Compare, typename Equals, typename Sort, typename PartialSort>
    void updatePermutationImpl(
        size_t limit,
        Permutation & res,
        EqualRanges & equal_ranges,
        Compare compare,
        Equals equals,
        Sort full_sort,
        PartialSort partial_sort) const;

#if defined(DEBUG_OR_SANITIZER_BUILD)
    virtual void doInsertFrom(const IColumn & src, size_t n);

    virtual void doInsertRangeFrom(const IColumn & src, size_t start, size_t length) = 0;

    virtual void doInsertManyFrom(const IColumn & src, size_t position, size_t length)
    {
        for (size_t i = 0; i < length; ++i)
            insertFrom(src, position);
    }

    virtual int doCompareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const = 0;

private:
    void assertTypeEquality(const IColumn & rhs) const
    {
        /// For Sparse and Const columns, we can compare only internal types. It is considered normal to e.g. insert from normal vector column to a sparse vector column.
        /// This case is specifically handled in ColumnSparse implementation. Similar situation with Const column.
        /// For the rest of column types we can compare the types directly.
        chassert((isConst() || isSparse()) ? getDataType() == rhs.getDataType() : typeid(*this) == typeid(rhs));
    }
#endif
};


template <typename ... Args>
struct IsMutableColumns;

template <typename Arg, typename ... Args>
struct IsMutableColumns<Arg, Args ...>
{
    static const bool value = std::is_assignable_v<MutableColumnPtr &&, Arg> && IsMutableColumns<Args ...>::value;
};

template <>
struct IsMutableColumns<> { static const bool value = true; };


/// Throws LOGICAL_ERROR if the type doesn't match.
template <typename Type>
const Type & checkAndGetColumn(const IColumn & column)
{
    return typeid_cast<const Type &>(column);
}

/// Returns nullptr if the type doesn't match.
/// If you're going to dereference the returned pointer without checking for null, use the
/// `const IColumn &` overload above instead.
template <typename Type>
const Type * checkAndGetColumn(const IColumn * column)
{
    return typeid_cast<const Type *>(column);
}

template <typename Type>
bool checkColumn(const IColumn & column)
{
    return checkAndGetColumn<Type>(&column);
}

template <typename Type>
bool checkColumn(const IColumn * column)
{
    return checkAndGetColumn<Type>(column);
}

/// True if column's an ColumnConst instance. It's just a syntax sugar for type check.
bool isColumnConst(const IColumn & column);

/// True if column's an ColumnNullable instance. It's just a syntax sugar for type check.
bool isColumnNullable(const IColumn & column);

/// True if column's an ColumnLazy instance. It's just a syntax sugar for type check.
bool isColumnLazy(const IColumn & column);

/// True if column's is ColumnNullable or ColumnLowCardinality with nullable nested column.
bool isColumnNullableOrLowCardinalityNullable(const IColumn & column);

/// Implement methods to devirtualize some calls of IColumn in final descendents.
/// `typename Parent` is needed because some columns don't inherit IColumn directly.
/// See ColumnFixedSizeHelper for example.
template <typename Derived, typename Parent = IColumn>
class IColumnHelper : public Parent
{
private:
    using Self = IColumnHelper<Derived, Parent>;

    friend Derived;
    friend class COWHelper<Self, Derived>;

    IColumnHelper() = default;
    IColumnHelper(const IColumnHelper &) = default;

    /// Devirtualize insertFrom.
    MutableColumns scatter(IColumn::ColumnIndex num_columns, const IColumn::Selector & selector) const override;

    /// Devirtualize insertFrom and insertRangeFrom.
    void gather(ColumnGathererStream & gatherer) override;

    /// Devirtualize compareAt.
    void compareColumn(
        const IColumn & rhs_base,
        size_t rhs_row_num,
        PaddedPODArray<UInt64> * row_indexes,
        PaddedPODArray<Int8> & compare_results,
        int direction,
        int nan_direction_hint) const override;

    /// Devirtualize compareAt.
    bool hasEqualValues() const override;

    /// Devirtualize isDefaultAt.
    double getRatioOfDefaultRows(double sample_ratio) const override;

    /// Devirtualize isDefaultAt.
    UInt64 getNumberOfDefaultRows() const override;

    /// Devirtualize isDefaultAt.
    void getIndicesOfNonDefaultRows(IColumn::Offsets & indices, size_t from, size_t limit) const override;

    /// Devirtualize byteSizeAt.
    void collectSerializedValueSizes(PaddedPODArray<UInt64> & sizes, const UInt8 * is_null) const override;

    /// Fills column values from RowRefList
    /// If row_refs_are_ranges is true, then each RowRefList has one element with >=1 consecutive rows
    void fillFromRowRefs(const DataTypePtr & type, size_t source_column_index_in_block, const PaddedPODArray<UInt64> & row_refs, bool row_refs_are_ranges) override;

    /// Fills column values from list of columns and row numbers
    /// `columns` and `row_nums` must have same size
    void fillFromBlocksAndRowNumbers(const DataTypePtr & type, size_t source_column_index_in_block, const std::vector<const Columns *> & columns, const std::vector<UInt32> & row_nums) override;

    /// Move common implementations into the same translation unit to ensure they are properly inlined.
    char * serializeValueIntoMemoryWithNull(size_t n, char * memory, const UInt8 * is_null) const override;
    StringRef serializeValueIntoArenaWithNull(size_t n, Arena & arena, char const *& begin, const UInt8 * is_null) const override;
    char * serializeValueIntoMemory(size_t n, char * memory) const override;
    StringRef serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const override;
};

}
