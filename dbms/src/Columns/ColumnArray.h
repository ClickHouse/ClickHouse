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
    ColumnArray(MutableColumnPtr && nested_column, MutableColumnPtr && offsets_column);

    /** Create an empty column of arrays with the type of values as in the column `nested_column` */
    explicit ColumnArray(MutableColumnPtr && nested_column);

    ColumnArray(const ColumnArray &) = default;

public:
    /** Create immutable column using immutable arguments. This arguments may be shared with other columns.
      * Use IColumn::mutate in order to make mutable column and mutate shared nested columns.
      */
    using Base = COWPtrHelper<IColumn, ColumnArray>;

    static Ptr create(const ColumnPtr & nested_column, const ColumnPtr & offsets_column)
    {
        return ColumnArray::create(nested_column->assumeMutable(), offsets_column->assumeMutable());
    }

    static Ptr create(const ColumnPtr & nested_column)
    {
        return ColumnArray::create(nested_column->assumeMutable());
    }

    template <typename ... Args, typename = typename std::enable_if<IsMutableColumns<Args ...>::value>::type>
    static MutablePtr create(Args &&... args) { return Base::create(std::forward<Args>(args)...); }

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
    ColumnPtr filter(const Filter & filt, ssize_t result_size_hint) const override;
    ColumnPtr permute(const Permutation & perm, size_t limit) const override;
    ColumnPtr index(const IColumn & indexes, size_t limit) const override;
    template <typename Type> ColumnPtr indexImpl(const PaddedPODArray<Type> & indexes, size_t limit) const;
    int compareAt(size_t n, size_t m, const IColumn & rhs_, int nan_direction_hint) const override;
    void getPermutation(bool reverse, size_t limit, int nan_direction_hint, Permutation & res) const override;
    void reserve(size_t n) override;
    size_t byteSize() const override;
    size_t allocatedBytes() const override;
    ColumnPtr replicate(const Offsets & replicate_offsets) const override;
    ColumnPtr convertToFullColumnIfConst() const override;
    void getExtremes(Field & min, Field & max) const override;

    bool hasEqualOffsets(const ColumnArray & other) const;

    /** More efficient methods of manipulation */
    IColumn & getData() { return data->assumeMutableRef(); }
    const IColumn & getData() const { return *data; }

    IColumn & getOffsetsColumn() { return offsets->assumeMutableRef(); }
    const IColumn & getOffsetsColumn() const { return *offsets; }

    Offsets & ALWAYS_INLINE getOffsets()
    {
        return static_cast<ColumnOffsets &>(offsets->assumeMutableRef()).getData();
    }

    const Offsets & ALWAYS_INLINE getOffsets() const
    {
        return static_cast<const ColumnOffsets &>(*offsets).getData();
    }

    const ColumnPtr & getDataPtr() const { return data; }
    ColumnPtr & getDataPtr() { return data; }

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
    ColumnPtr replicateNumber(const Offsets & replicate_offsets) const;

    /// Multiply the values if the nested column is ColumnString. The code is too complicated.
    ColumnPtr replicateString(const Offsets & replicate_offsets) const;

    /** Non-constant arrays of constant values are quite rare.
      * Most functions can not work with them, and does not create such columns as a result.
      * An exception is the function `replicate`(see FunctionsMiscellaneous.h), which has service meaning for the implementation of lambda functions.
      * Only for its sake is the implementation of the `replicate` method for ColumnArray(ColumnConst).
      */
    ColumnPtr replicateConst(const Offsets & replicate_offsets) const;

    /** The following is done by simply replicating of nested columns.
      */
    ColumnPtr replicateTuple(const Offsets & replicate_offsets) const;
    ColumnPtr replicateNullable(const Offsets & replicate_offsets) const;
    ColumnPtr replicateGeneric(const Offsets & replicate_offsets) const;


    /// Specializations for the filter function.
    template <typename T>
    ColumnPtr filterNumber(const Filter & filt, ssize_t result_size_hint) const;

    ColumnPtr filterString(const Filter & filt, ssize_t result_size_hint) const;
    ColumnPtr filterTuple(const Filter & filt, ssize_t result_size_hint) const;
    ColumnPtr filterNullable(const Filter & filt, ssize_t result_size_hint) const;
    ColumnPtr filterGeneric(const Filter & filt, ssize_t result_size_hint) const;
};


}
