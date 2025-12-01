#pragma once

#include "Common/COW.h"
#include "Common/WeakHash.h"
#include <Common/PODArray.h>
#include "Columns/IColumn.h"
#include "fsst.h"

namespace DB
{

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
extern const int INCORRECT_DATA;
}

class ColumnFSST final : public COWHelper<IColumnHelper<ColumnFSST>, ColumnFSST>
{
private:
    friend class COWHelper<IColumnHelper<ColumnFSST>, ColumnFSST>;
    friend class COW<IColumn>;

    WrappedPtr string_column;
    fsst_decoder_t fsst;

    Offsets offsets;
    Offsets origin_lengths;

    explicit ColumnFSST(MutableColumnPtr && _string_column)
        : string_column(std::move(_string_column))
    {
    }

    ColumnFSST(const ColumnFSST &) { throwNotImplemented(); }

public:
    std::string getName() const override { return "FixedString(FSST)"; }
    const char * getFamilyName() const override { return "FixedString"; }
    TypeIndex getDataType() const override { return TypeIndex::FixedString; }

    [[nodiscard]] size_t size() const override { return string_column->size(); }

    [[nodiscard]] Field operator[](size_t n) const override;
    void get(size_t n, Field & res) const override;

    DataTypePtr getValueNameAndTypeImpl(WriteBufferFromOwnString &, size_t, const Options &) const override { throwNotImplemented(); }

    [[nodiscard]] StringRef getDataAt(size_t /* n */) const override { throwNotImplemented(); }

    [[nodiscard]] bool isDefaultAt(size_t) const override;

    void insert(const Field & /* x */) override { throwNotImplemented(); }
    bool tryInsert(const Field & /* x */) override { throwNotImplemented(); }
    void insertData(const char * /* pos */, size_t /* length */) override { throwNotImplemented(); }
    void insertDefault() override { throwNotImplemented(); }

    void popBack(size_t /* n */) override { throwNotImplemented(); }

    [[nodiscard]] const char * deserializeAndInsertFromArena(const char * /* pos */) override { throwNotImplemented(); }
    [[nodiscard]] const char * skipSerializedInArena(const char *) const override { throwNotImplemented(); }

    void updateHashWithValue(size_t /* n */, SipHash & /* hash */) const override { throwNotImplemented(); }
    WeakHash32 getWeakHash32() const override { throwNotImplemented(); }
    void updateHashFast(SipHash & /* hash */) const override { throwNotImplemented(); }

    [[nodiscard]] ColumnPtr filter(const Filter & /* filt */, ssize_t /* result_size_hint */) const override { throwNotImplemented(); }
    void expand(const Filter & /*mask*/, bool /*inverted*/) override { throwNotImplemented(); }
    [[nodiscard]] ColumnPtr permute(const Permutation & /* perm */ , size_t /* limit */) const override { throwNotImplemented(); }
    [[nodiscard]] ColumnPtr index(const IColumn & /* indexes */ , size_t /* limit */) const override { throwNotImplemented(); }

    void compareColumn(
        const IColumn & /* rhs */,
        size_t /* rhs_row_num */,
        PaddedPODArray<UInt64> * /* row_indexes */,
        PaddedPODArray<Int8> & /* compare_results */,
        int /* direction */,
        int /* nan_direction_hint */) const override
    {
        throwNotImplemented();
    }

    void getPermutation(
        PermutationSortDirection /* direction */,
        PermutationSortStability /* stability */,
        size_t /* limit */,
        int /* nan_direction_hint */,
        Permutation & /* res */) const override
    {
        throwNotImplemented();
    }
    void updatePermutation(
        PermutationSortDirection /* direction */,
        PermutationSortStability /* stability */,
        size_t /* limit */,
        int /* nan_direction_hint */,
        Permutation & /* res */,
        EqualRanges & /* equal_ranges */) const override
    {
        throwNotImplemented();
    }

    [[nodiscard]] ColumnPtr replicate(const Offsets & /* offsets */) const override { throwNotImplemented(); }

    void gather(ColumnGathererStream & /* gatherer_stream */) override { throwNotImplemented(); }

    void getExtremes(Field & /* min */, Field & /* max */) const override { throwNotImplemented(); }

    [[nodiscard]] size_t byteSize() const override;
    [[nodiscard]] size_t byteSizeAt(size_t) const override;
    [[nodiscard]] size_t allocatedBytes() const override { throwNotImplemented(); }

    [[nodiscard]] double getRatioOfDefaultRows(double /* sample_ratio = 1.0 */) const override { throwNotImplemented(); }
    [[nodiscard]] UInt64 getNumberOfDefaultRows() const override { throwNotImplemented(); }

    void getIndicesOfNonDefaultRows(Offsets & /* indices */, size_t /* from */, size_t /* limit */) const override { throwNotImplemented(); }

    ColumnPtr updateFrom(const Patch & /* patch */) const override { throwNotImplemented(); }
    void updateInplaceFrom(const Patch & /* patch */) override { throwNotImplemented(); }

    void doInsertRangeFrom(const IColumn & /* src */, size_t /* start */, size_t /* length */) override { throwNotImplemented(); }
    int doCompareAt(size_t /* n */, size_t /* m */, const IColumn & /* rhs */, int /* nan_direction_hint */) const override { throwNotImplemented(); }

    WrappedPtr getStringColumn() const { return string_column; }

    fsst_decoder_t & getFsst() { return fsst; }
    const fsst_decoder_t & getFsst() const { return fsst; }

    Offsets & getOffsets() { return offsets; }
    const Offsets & getOffsets() const { return offsets; }


    [[noreturn]] static void throwNotImplemented()
    {
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED, "text escaped/text quoted/text csv/whole text/json serialization are not implemented for FSST");
    }
};
};
