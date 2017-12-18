#pragma once

#include <Columns/IColumn.h>
#include <Columns/ColumnVector.h>
#include <Core/Defines.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int NOT_IMPLEMENTED;
    extern const int BAD_ARGUMENTS;
}

/** A column of array values.
  * In memory, it is represented as one column of a nested type, whose size is equal to the sum of the sizes of all arrays,
  *  and as an array of offsets in it, which allows you to get each element.
  */
class ColumnArray final : public COWPtrHelper<IColumn, ColumnArray>
{
private:
    friend class COWPtrHelper<IColumn, ColumnArray>;

    /** Create an array column with specified values and offsets. */
    ColumnArray(const ColumnPtr & nested_column, const ColumnPtr & offsets_column);

    /** Create an empty column of arrays with the type of values as in the column `nested_column` */
    ColumnArray(const ColumnPtr & nested_column);

    ColumnArray(const ColumnArray &) = default;

public:
    /** On the index i there is an offset to the beginning of the i + 1 -th element. */
    using ColumnOffsets = ColumnVector<Offset>;

    std::string getName() const override;
    const char * getFamilyName() const override { return "Array"; }
    MutableColumnPtr cloneResized(size_t size) const override;
    size_t size() const override;
    Field operator[](size_t n) const override;
    void get(size_t n, Field & res) const override;
    StringRef getDataAt(size_t n) const override;
    void insertData(const char * pos, size_t length) override;
    StringRef serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const override;
    const char * deserializeAndInsertFromArena(const char * pos) override;
    void updateHashWithValue(size_t n, SipHash & hash) const override;
    void insertRangeFrom(const IColumn & src, size_t start, size_t length) override;
    void insert(const Field & x) override;
    void insertFrom(const IColumn & src_, size_t n) override;
    void insertDefault() override;
    void popBack(size_t n) override;
    MutableColumnPtr filter(const Filter & filt, ssize_t result_size_hint) const override;
    MutableColumnPtr permute(const Permutation & perm, size_t limit) const override;
    int compareAt(size_t n, size_t m, const IColumn & rhs_, int nan_direction_hint) const override;
    void getPermutation(bool reverse, size_t limit, int nan_direction_hint, Permutation & res) const override;
    void reserve(size_t n) override;
    size_t byteSize() const override;
    size_t allocatedBytes() const override;
    MutableColumnPtr replicate(const Offsets & replicate_offsets) const override;
    MutableColumnPtr convertToFullColumnIfConst() const override;
    void getExtremes(Field & min, Field & max) const override;

    bool hasEqualOffsets(const ColumnArray & other) const;

    /** More efficient methods of manipulation */
    IColumn & getData() { return *data->assumeMutable(); }
    const IColumn & getData() const { return *data; }

    IColumn & getOffsetsColumn() { return *offsets->assumeMutable(); }
    const IColumn & getOffsetsColumn() const { return *offsets; }

    Offsets & ALWAYS_INLINE getOffsets()
    {
        return static_cast<ColumnOffsets &>(*offsets->assumeMutable()).getData();
    }

    const Offsets & ALWAYS_INLINE getOffsets() const
    {
        return static_cast<const ColumnOffsets &>(*offsets).getData();
    }

    //MutableColumnPtr getDataMutablePtr() { return data->assumeMutable(); }
    const ColumnPtr & getDataPtr() const { return data; }
    ColumnPtr & getDataPtr() { return data; }

    //MutableColumnPtr getOffsetsMutablePtr() { return offsets->assumeMutable(); }
    const ColumnPtr & getOffsetsPtr() const { return offsets; }
    ColumnPtr & getOffsetsPtr() { return offsets; }

    MutableColumns scatter(ColumnIndex num_columns, const Selector & selector) const override
    {
        return scatterImpl<ColumnArray>(num_columns, selector);
    }

    void gather(ColumnGathererStream & gatherer_stream) override;

    void forEachSubcolumn(ColumnCallback callback) override
    {
        callback(offsets);
        callback(data);
    }

private:
    ColumnPtr data;
    ColumnPtr offsets;

    size_t ALWAYS_INLINE offsetAt(size_t i) const { return i == 0 ? 0 : getOffsets()[i - 1]; }
    size_t ALWAYS_INLINE sizeAt(size_t i) const { return i == 0 ? getOffsets()[0] : (getOffsets()[i] - getOffsets()[i - 1]); }


    /// Multiply values if the nested column is ColumnVector<T>.
    template <typename T>
    MutableColumnPtr replicateNumber(const Offsets & replicate_offsets) const;

    /// Multiply the values if the nested column is ColumnString. The code is too complicated.
    MutableColumnPtr replicateString(const Offsets & replicate_offsets) const;

    /** Non-constant arrays of constant values are quite rare.
      * Most functions can not work with them, and does not create such columns as a result.
      * An exception is the function `replicate`(see FunctionsMiscellaneous.h), which has service meaning for the implementation of lambda functions.
      * Only for its sake is the implementation of the `replicate` method for ColumnArray(ColumnConst).
      */
    MutableColumnPtr replicateConst(const Offsets & replicate_offsets) const;

    /** The following is done by simply replicating of nested columns.
      */
    MutableColumnPtr replicateTuple(const Offsets & replicate_offsets) const;
    MutableColumnPtr replicateNullable(const Offsets & replicate_offsets) const;
    MutableColumnPtr replicateGeneric(const Offsets & replicate_offsets) const;


    /// Specializations for the filter function.
    template <typename T>
    MutableColumnPtr filterNumber(const Filter & filt, ssize_t result_size_hint) const;

    MutableColumnPtr filterString(const Filter & filt, ssize_t result_size_hint) const;
    MutableColumnPtr filterTuple(const Filter & filt, ssize_t result_size_hint) const;
    MutableColumnPtr filterNullable(const Filter & filt, ssize_t result_size_hint) const;
    MutableColumnPtr filterGeneric(const Filter & filt, ssize_t result_size_hint) const;
};


}
