#pragma once

#include <Core/Field.h>
#include <Core/NamesAndTypes.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Columns/IColumn.h>
#include <Common/WeakHash.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

class IFunctionBase;
using FunctionBasePtr = std::shared_ptr<const IFunctionBase>;

/** A column containing a lambda expression.
  * Contains an expression and captured columns, but not input arguments.
  */
class ColumnFunction final : public COWHelper<IColumnHelper<ColumnFunction>, ColumnFunction>
{
private:
    friend class COWHelper<IColumnHelper<ColumnFunction>, ColumnFunction>;

    ColumnFunction(
        size_t size,
        FunctionBasePtr function_,
        const ColumnsWithTypeAndName & columns_to_capture,
        bool is_short_circuit_argument_ = false,
        bool is_function_compiled_ = false,
        bool recursively_convert_result_to_full_column_if_low_cardinality_ = false);

public:
    const char * getFamilyName() const override { return "Function"; }
    TypeIndex getDataType() const override { return TypeIndex::Function; }

    MutableColumnPtr cloneResized(size_t size) const override;

    size_t size() const override { return elements_size; }

    ColumnPtr cut(size_t start, size_t length) const override;
    ColumnPtr replicate(const Offsets & offsets) const override;
    ColumnPtr filter(const Filter & filt, ssize_t result_size_hint) const override;
    void expand(const Filter & mask, bool inverted) override;
    ColumnPtr permute(const Permutation & perm, size_t limit) const override;
    ColumnPtr index(const IColumn & indexes, size_t limit) const override;

    std::vector<MutableColumnPtr> scatter(IColumn::ColumnIndex num_columns,
                                          const IColumn::Selector & selector) const override;

    void getExtremes(Field &, Field &) const override {}

    size_t byteSize() const override;
    size_t byteSizeAt(size_t n) const override;
    size_t allocatedBytes() const override;

    void appendArguments(const ColumnsWithTypeAndName & columns);
    ColumnWithTypeAndName reduce() const;

    Field operator[](size_t n) const override;

    void get(size_t n, Field & res) const override;

    StringRef getDataAt(size_t) const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot get value from {}", getName());
    }

    bool isDefaultAt(size_t) const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "isDefaultAt is not implemented for {}", getName());
    }

    void insert(const Field &) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot insert into {}", getName());
    }

    bool tryInsert(const Field &) override
    {
        return false;
    }

    void insertDefault() override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot insert into {}", getName());
    }

#if !defined(DEBUG_OR_SANITIZER_BUILD)
    void insertFrom(const IColumn & src, size_t n) override;
#else
    void doInsertFrom(const IColumn & src, size_t n) override;
#endif
#if !defined(DEBUG_OR_SANITIZER_BUILD)
    void insertRangeFrom(const IColumn &, size_t start, size_t length) override;
#else
    void doInsertRangeFrom(const IColumn &, size_t start, size_t length) override;
#endif

    void insertData(const char *, size_t) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot insert into {}", getName());
    }

    StringRef serializeValueIntoArena(size_t, Arena &, char const *&) const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot serialize from {}", getName());
    }

    const char * deserializeAndInsertFromArena(const char *) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot deserialize to {}", getName());
    }

    const char * skipSerializedInArena(const char*) const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot skip serialized {}", getName());
    }

    void updateHashWithValue(size_t, SipHash &) const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "updateHashWithValue is not implemented for {}", getName());
    }

    WeakHash32 getWeakHash32() const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "getWeakHash32 is not implemented for {}", getName());
    }

    void updateHashFast(SipHash &) const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "updateHashFast is not implemented for {}", getName());
    }

    void popBack(size_t) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "popBack is not implemented for {}", getName());
    }

#if !defined(DEBUG_OR_SANITIZER_BUILD)
    int compareAt(size_t, size_t, const IColumn &, int) const override
#else
    int doCompareAt(size_t, size_t, const IColumn &, int) const override
#endif
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "compareAt is not implemented for {}", getName());
    }

    void compareColumn(const IColumn &, size_t, PaddedPODArray<UInt64> *, PaddedPODArray<Int8> &, int, int) const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "compareColumn is not implemented for {}", getName());
    }

    bool hasEqualValues() const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "hasEqualValues is not implemented for {}", getName());
    }

    void getPermutation(IColumn::PermutationSortDirection, IColumn::PermutationSortStability,
                        size_t, int, Permutation &) const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "getPermutation is not implemented for {}", getName());
    }

    void updatePermutation(IColumn::PermutationSortDirection, IColumn::PermutationSortStability,
                        size_t, int, Permutation &, EqualRanges &) const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "updatePermutation is not implemented for {}", getName());
    }

    void gather(ColumnGathererStream &) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method gather is not supported for {}", getName());
    }

    double getRatioOfDefaultRows(double) const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method getRatioOfDefaultRows is not supported for {}", getName());
    }

    UInt64 getNumberOfDefaultRows() const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method getNumberOfDefaultRows is not supported for {}", getName());
    }

    void getIndicesOfNonDefaultRows(Offsets &, size_t, size_t) const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method getIndicesOfNonDefaultRows is not supported for {}", getName());
    }

    bool isShortCircuitArgument() const { return is_short_circuit_argument; }

    DataTypePtr getResultType() const;

    /// Create copy of this column, but with recursively_convert_result_to_full_column_if_low_cardinality = true
    ColumnPtr recursivelyConvertResultToFullColumnIfLowCardinality() const;

    const FunctionBasePtr & getFunction() const { return function; }
    const ColumnsWithTypeAndName & getCapturedColumns() const { return captured_columns; }

private:
    size_t elements_size;
    FunctionBasePtr function;
    ColumnsWithTypeAndName captured_columns;

    /// Determine if it's used as a lazy executed argument for short-circuit function.
    /// It's needed to distinguish between lazy executed argument and
    /// argument with ColumnFunction column (some functions can return it)
    /// See ExpressionActions.cpp for details.
    bool is_short_circuit_argument;

    /// Special flag for lazy executed argument for short-circuit function.
    /// If true, call recursiveRemoveLowCardinality on the result column
    /// when function will be executed.
    /// It's used when short-circuit function uses default implementation
    /// for low cardinality arguments.
    bool recursively_convert_result_to_full_column_if_low_cardinality = false;

    /// Determine if passed function is compiled. Used for profiling.
    bool is_function_compiled;

    void appendArgument(const ColumnWithTypeAndName & column);
};

const ColumnFunction * checkAndGetShortCircuitArgument(const ColumnPtr & column);

}
