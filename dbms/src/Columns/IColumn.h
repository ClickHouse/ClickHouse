#pragma once

#include <Core/Field.h>
#include <Common/COWPtr.h>
#include <Common/PODArray.h>
#include <Common/Exception.h>
#include <common/StringRef.h>


class SipHash;


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_GET_SIZE_OF_FIELD;
    extern const int NOT_IMPLEMENTED;
    extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
}

class Arena;
class ColumnGathererStream;

/// Declares interface to store columns in memory.
class IColumn : public COWPtr<IColumn>
{
private:
    friend class COWPtr<IColumn>;

    /// Creates the same column with the same data.
    /// This is internal method to use from COWPtr.
    /// It performs shallow copy with copy-ctor and not useful from outside.
    /// If you want to copy column for modification, look at 'mutate' method.
    virtual MutablePtr clone() const = 0;

public:
    /// Name of a Column. It is used in info messages.
    virtual std::string getName() const { return getFamilyName(); };

    /// Name of a Column kind, without parameters (example: FixedString, Array).
    virtual const char * getFamilyName() const = 0;

    /** If column isn't constant, returns nullptr (or itself).
      * If column is constant, transforms constant to full column (if column type allows such tranform) and return it.
      */
    virtual MutablePtr convertToFullColumnIfConst() const { return {}; }

    /// Creates empty column with the same type.
    virtual MutablePtr cloneEmpty() const { return cloneResized(0); }

    /// Creates column with the same type and specified size.
    /// If size is less current size, then data is cut.
    /// If size is greater, than default values are appended.
    virtual MutablePtr cloneResized(size_t /*size*/) const { throw Exception("Cannot cloneResized() column " + getName(), ErrorCodes::NOT_IMPLEMENTED); }

    /// Returns number of values in column.
    virtual size_t size() const = 0;

    /// There are no values in columns.
    bool empty() const { return size() == 0; }

    /// Returns value of n-th element in universal Field representation.
    /// Is used in rare cases, since creation of Field instance is expensive usually.
    virtual Field operator[](size_t n) const = 0;

    /// Like the previous one, but avoids extra copying if Field is in a container, for example.
    virtual void get(size_t n, Field & res) const = 0;

    /// If possible, returns pointer to memory chunk which contains n-th element (if it isn't possible, throws an exception)
    /// Is used to optimize some computations (in aggregation, for example).
    virtual StringRef getDataAt(size_t n) const = 0;

    /// Like getData, but has special behavior for columns that contain variable-length strings.
    /// Returns zero-ending memory chunk (i.e. its size is 1 byte longer).
    virtual StringRef getDataAtWithTerminatingZero(size_t n) const
    {
        return getDataAt(n);
    }

    /// If column stores integers, it returns n-th element transformed to UInt64 using static_cast.
    /// If column stores floting point numbers, bits of n-th elements are copied to lower bits of UInt64, the remaining bits are zeros.
    /// Is used to optimize some computations (in aggregation, for example).
    virtual UInt64 get64(size_t /*n*/) const
    {
        throw Exception("Method get64 is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    /** If column is numeric, return value of n-th element, casted to UInt64.
      * Otherwise throw an exception.
      */
    virtual UInt64 getUInt(size_t /*n*/) const
    {
        throw Exception("Method getUInt is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    virtual Int64 getInt(size_t /*n*/) const
    {
        throw Exception("Method getInt is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    virtual bool isNullAt(size_t /*n*/) const { return false; }

    /// Removes all elements outside of specified range.
    /// Is used in LIMIT operation, for example.
    virtual MutablePtr cut(size_t start, size_t length) const
    {
        MutablePtr res = cloneEmpty();
        res->insertRangeFrom(*this, start, length);
        return res;
    }

    /// Appends new value at the end of column (column's size is increased by 1).
    /// Is used to transform raw strings to Blocks (for example, inside input format parsers)
    virtual void insert(const Field & x) = 0;

    /// Appends n-th element from other column with the same type.
    /// Is used in merge-sort and merges. It could be implemented in inherited classes more optimally than default implementation.
    virtual void insertFrom(const IColumn & src, size_t n) { insert(src[n]); }

    /// Appends range of elements from other column.
    /// Could be used to concatenate columns.
    virtual void insertRangeFrom(const IColumn & src, size_t start, size_t length) = 0;

    /// Appends data located in specified memory chunk if it is possible (throws an exception if it cannot be implemented).
    /// Is used to optimize some computations (in aggregation, for example).
    /// Parameter length could be ignored if column values have fixed size.
    virtual void insertData(const char * pos, size_t length) = 0;

    /// Like getData, but has special behavior for columns that contain variable-length strings.
    /// In this special case inserting data should be zero-ending (i.e. length is 1 byte greater than real string size).
    virtual void insertDataWithTerminatingZero(const char * pos, size_t length)
    {
        insertData(pos, length);
    }

    /// Appends "default value".
    /// Is used when there are need to increase column size, but inserting value doesn't make sense.
    /// For example, ColumnNullable(Nested) absolutely ignores values of nested column if it is marked as NULL.
    virtual void insertDefault() = 0;

    /** Removes last n elements.
      * Is used to support exeption-safety of several operations.
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
    virtual StringRef serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const = 0;

    /// Deserializes a value that was serialized using IColumn::serializeValueIntoArena method.
    /// Returns pointer to the position after the read data.
    virtual const char * deserializeAndInsertFromArena(const char * pos) = 0;

    /// Update state of hash function with value of n-th element.
    /// On subsequent calls of this method for sequence of column values of arbitary types,
    ///  passed bytes to hash must identify sequence of values unambiguously.
    virtual void updateHashWithValue(size_t n, SipHash & hash) const = 0;

    /** Removes elements that don't match the filter.
      * Is used in WHERE and HAVING operations.
      * If result_size_hint > 0, then makes advance reserve(result_size_hint) for the result column;
      *  if 0, then don't makes reserve(),
      *  otherwise (i.e. < 0), makes reserve() using size of source column.
      */
    using Filter = PaddedPODArray<UInt8>;
    virtual MutablePtr filter(const Filter & filt, ssize_t result_size_hint) const = 0;

    /// Permutes elements using specified permutation. Is used in sortings.
    /// limit - if it isn't 0, puts only first limit elements in the result.
    using Permutation = PaddedPODArray<size_t>;
    virtual MutablePtr permute(const Permutation & perm, size_t limit) const = 0;

    /** Compares (*this)[n] and rhs[m].
      * Returns negative number, 0, or positive number (*this)[n] is less, equal, greater than rhs[m] respectively.
      * Is used in sortings.
      *
      * If one of element's value is NaN or NULLs, then:
      * - if nan_direction_hint == -1, NaN and NULLs are considered as least than everything other;
      * - if nan_direction_hint ==  1, NaN and NULLs are considered as greatest than everything other.
      * For example, if nan_direction_hint == -1 is used by descending sorting, NaNs will be at the end.
      *
      * For non Nullable and non floating point types, nan_direction_hint is ignored.
      */
    virtual int compareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const = 0;

    /** Returns a permutation that sorts elements of this column,
      *  i.e. perm[i]-th element of source column should be i-th element of sorted column.
      * reverse - reverse ordering (acsending).
      * limit - if isn't 0, then only first limit elements of the result column could be sorted.
      * nan_direction_hint - see above.
      */
    virtual void getPermutation(bool reverse, size_t limit, int nan_direction_hint, Permutation & res) const = 0;

    /** Copies each element according offsets parameter.
      * (i-th element should be copied offsets[i] - offsets[i - 1] times.)
      * It is necessary in ARRAY JOIN operation.
      */
    using Offset = UInt64;
    using Offsets = PaddedPODArray<Offset>;
    virtual MutablePtr replicate(const Offsets & offsets) const = 0;

    /** Split column to smaller columns. Each value goes to column index, selected by corresponding element of 'selector'.
      * Selector must contain values from 0 to num_columns - 1.
      * For default implementation, see scatterImpl.
      */
    using ColumnIndex = UInt64;
    using Selector = PaddedPODArray<ColumnIndex>;
    virtual std::vector<MutablePtr> scatter(ColumnIndex num_columns, const Selector & selector) const = 0;

    /// Insert data from several other columns according to source mask (used in vertical merge).
    /// For now it is a helper to de-virtualize calls to insert*() functions inside gather loop
    /// (descendants should call gatherer_stream.gather(*this) to implement this function.)
    /// TODO: interface decoupled from ColumnGathererStream that allows non-generic specializations.
    virtual void gather(ColumnGathererStream & gatherer_stream) = 0;

    /** Computes minimum and maximum element of the column.
      * In addition to numeric types, the funtion is completely implemented for Date and DateTime.
      * For strings and arrays function should retrurn default value.
      *  (except for constant columns; they should return value of the constant).
      * If column is empty function should return default value.
      */
    virtual void getExtremes(Field & min, Field & max) const = 0;

    /// Reserves memory for specified amount of elements. If reservation isn't possible, does nothing.
    /// It affects performance only (not correctness).
    virtual void reserve(size_t /*n*/) {};

    /// Size of column data in memory (may be approximate) - for profiling. Zero, if could not be determined.
    virtual size_t byteSize() const = 0;

    /// Size of memory, allocated for column.
    /// This is greater or equals to byteSize due to memory reservation in containers.
    /// Zero, if could be determined.
    virtual size_t allocatedBytes() const = 0;

    /// If the column contains subcolumns (such as Array, Nullable, etc), do callback on them.
    /// Shallow: doesn't do recursive calls; don't do call for itself.
    using ColumnCallback = std::function<void(Ptr&)>;
    virtual void forEachSubcolumn(ColumnCallback) {}


    MutablePtr mutate() const
    {
        MutablePtr res = COWPtr<IColumn>::mutate();
        res->forEachSubcolumn([](Ptr & subcolumn) { subcolumn = subcolumn->mutate(); });
        return res;
    }


    /** Some columns can contain another columns inside.
      * So, we have a tree of columns. But not all combinations are possible.
      * There are the following rules:
      *
      * ColumnConst may be only at top. It cannot be inside any column.
      * ColumnNullable can contain only simple columns.
      */

    /// Various properties on behaviour of column type.

    /// Is this column a container for Nullable values? It's true only for ColumnNullable.
    /// Note that ColumnConst(ColumnNullable(...)) is not considered.
    virtual bool isColumnNullable() const { return false; }

    /// Column stores a constant value. It's true only for ColumnConst wrapper.
    virtual bool isColumnConst() const { return false; }

    /// It's a special kind of column, that contain single value, but is not a ColumnConst.
    virtual bool isDummy() const { return false; }

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
    virtual bool valuesHaveFixedSize() const { return isFixedAndContiguous(); }

    /// Values in column are represented as continuous memory segment of fixed size. Implies valuesHaveFixedSize.
    virtual bool isFixedAndContiguous() const { return false; }

    /// If valuesHaveFixedSize, returns size of value, otherwise throw an exception.
    virtual size_t sizeOfValueIfFixed() const { throw Exception("Values of column " + getName() + " are not fixed size.", ErrorCodes::CANNOT_GET_SIZE_OF_FIELD); }

    /// Column is ColumnVector of numbers or ColumnConst of it. Note that Nullable columns are not numeric.
    /// Implies isFixedAndContiguous.
    virtual bool isNumeric() const { return false; }

    /// If the only value column can contain is NULL.
    /// Does not imply type of object, because it can be ColumnNullable(ColumnNothing) or ColumnConst(ColumnNullable(ColumnNothing))
    virtual bool onlyNull() const { return false; }

    /// Can be inside ColumnNullable.
    virtual bool canBeInsideNullable() const { return false; }


    virtual ~IColumn() {}

    /** Print column name, size, and recursively print all subcolumns.
      */
    String dumpStructure() const;

protected:

    /// Template is to devirtualize calls to insertFrom method.
    /// In derived classes (that use final keyword), implement scatter method as call to scatterImpl.
    template <typename Derived>
    std::vector<MutablePtr> scatterImpl(ColumnIndex num_columns, const Selector & selector) const
    {
        size_t num_rows = size();

        if (num_rows != selector.size())
            throw Exception(
                    "Size of selector: " + std::to_string(selector.size()) + " doesn't match size of column: " + std::to_string(num_rows),
                    ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

        std::vector<MutablePtr> columns(num_columns);
        for (auto & column : columns)
            column = cloneEmpty();

        {
            size_t reserve_size = num_rows * 1.1 / num_columns;    /// 1.1 is just a guess. Better to use n-sigma rule.

            if (reserve_size > 1)
                for (auto & column : columns)
                    column->reserve(reserve_size);
        }

        for (size_t i = 0; i < num_rows; ++i)
            static_cast<Derived &>(*columns[selector[i]]).insertFrom(*this, i);

        return columns;
    }
};

using ColumnPtr = IColumn::Ptr;
using MutableColumnPtr  = IColumn::MutablePtr;
using Columns = std::vector<ColumnPtr>;
using MutableColumns = std::vector<MutableColumnPtr>;

using ColumnRawPtrs = std::vector<const IColumn *>;
//using MutableColumnRawPtrs = std::vector<IColumn *>;

}
