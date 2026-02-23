#pragma once

#include <cstddef>
#include <memory>
#include <vector>

#include <Columns/ColumnString.h>
#include <Columns/IColumn.h>
#include <Columns/IColumn_fwd.h>
#include <DataTypes/Serializations/SerializationStringFSST.h>
#include <base/types.h>
#include <Common/COW.h>
#include <Common/PODArray.h>
#include <Common/WeakHash.h>

#include <fsst.h>

namespace DB
{

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
extern const int INCORRECT_DATA;
extern const int LOGICAL_ERROR;
extern const int PARAMETER_OUT_OF_BOUND;
}


class ColumnFSST final : public COWHelper<IColumnHelper<ColumnFSST>, ColumnFSST>
{
    struct ComparatorBase;

    using ComparatorAscendingUnstable = ComparatorAscendingUnstableImpl<ComparatorBase>;
    using ComparatorAscendingStable = ComparatorAscendingStableImpl<ComparatorBase>;
    using ComparatorDescendingUnstable = ComparatorDescendingUnstableImpl<ComparatorBase>;
    using ComparatorDescendingStable = ComparatorDescendingStableImpl<ComparatorBase>;
    using ComparatorEqual = ComparatorEqualImpl<ComparatorBase>;

private:
    friend class COWHelper<IColumnHelper<ColumnFSST>, ColumnFSST>;
    friend class COW<IColumn>;

    struct BatchDsc
    {
        std::shared_ptr<fsst_decoder_t> decoder;
        size_t batch_start_index;
    };

    WrappedPtr string_column;
    std::vector<UInt64> origin_lengths;
    std::vector<BatchDsc> decoders;

    explicit ColumnFSST(MutableColumnPtr && _string_column)
        : string_column(std::move(_string_column))
    {
    }

    ColumnFSST(const ColumnFSST &) { throwNotImplemented(); }

    std::optional<size_t> batchByRow(size_t row) const;

    void filterInnerData(const Filter & filt, std::vector<UInt64> & lengths, std::vector<BatchDsc> & decoders) const;

    void decompressRow(size_t row_num, String & out) const;

public:
    using Base = COWHelper<IColumnHelper<ColumnFSST>, ColumnFSST>;
    static Ptr create(const ColumnPtr & nested) { return Base::create(nested->assumeMutable()); }
    static Ptr create(ColumnPtr && nested, std::vector<BatchDsc> decoders, std::vector<UInt64> origin_lengths)
    {
        return Base::create(std::move(nested), std::move(decoders), std::move(origin_lengths));
    }

    ColumnFSST(ColumnPtr && _nested, std::vector<BatchDsc> _decoders, std::vector<UInt64> _origin_lengths)
        : string_column(std::move(_nested))
        , origin_lengths(std::move(_origin_lengths))
        , decoders(std::move(_decoders))
    {
    }

    std::string getName() const override { return "FixedString(FSST)"; }
    const char * getFamilyName() const override { return "FixedString"; }
    TypeIndex getDataType() const override { return TypeIndex::FixedString; }

    [[nodiscard]] size_t size() const override { return string_column->size(); }

    [[nodiscard]] Field operator[](size_t n) const override;
    void get(size_t n, Field & res) const override;

    DataTypePtr getValueNameAndTypeImpl(WriteBufferFromOwnString & name_buf, size_t n, const Options & options) const override
    {
        return string_column->getValueNameAndTypeImpl(name_buf, n, options);
    }

    [[nodiscard]] std::string_view getDataAt(size_t) const override { throwNotSupported(); }
    [[nodiscard]] bool isDefaultAt(size_t n) const override;

    /*
    ColumnFSST is immutable to inserts.
    It is possible to add insert methods, but several questions arise:

    1)  All data in ColumnFSST are divided into "batches". A batch is a pair
        (FSST descriptor; compressed block of data). If a new string were inserted,
        should it be added to the last batch? The last batch was built based on a
        previously provided block of data, so the compression ratio for the new
        string could be poor if the last batch is used.

    2)  If a new string were added to a new batch, when should this batch be
        flushed (i.e., compressed)? As soon as the batch is compressed, it is
        impossible/impractical to rebuild the FSST descriptor for it.
    */
    void insert(const Field &) override { throwNotSupported(); }
    bool tryInsert(const Field &) override { throwNotSupported(); }
    void insertData(const char *, size_t) override { throwNotSupported(); }
    void insertDefault() override { throwNotSupported(); }
    void deserializeAndInsertFromArena(ReadBuffer &, const SerializationSettings *) override { throwNotSupported(); }
    void skipSerializedInArena(ReadBuffer &) const override { throwNotSupported(); }

    void popBack(size_t n) override;

    MutableColumnPtr cloneEmpty() const override { return create(ColumnString::create())->assumeMutable(); }

    void updateHashWithValue(size_t n, SipHash & hash) const override { string_column->updateHashWithValue(n, hash); }
    WeakHash32 getWeakHash32() const override { return string_column->getWeakHash32(); }
    void updateHashFast(SipHash & hash) const override { string_column->updateHashFast(hash); }

    [[nodiscard]] ColumnPtr filter(const Filter & filt, ssize_t result_size_hint) const override;
    void filter(const Filter & filt) override;

    /* also need to implement compress logic in ColumnFSST */
    void expand(const Filter & /*mask*/, bool /*inverted*/) override { throwNotSupported(); }

    /* batches have no sense after reordering */
    [[nodiscard]] ColumnPtr permute(const Permutation & /* perm */, size_t /* limit */) const override { throwNotSupported(); }
    /* 
        Compression ratio can become extremely bad because the decoders were built
        based on how often symbol sequences appear in the current column state.
    */
    [[nodiscard]] ColumnPtr index(const IColumn & /* indexes */, size_t /* limit */) const override { throwNotSupported(); }

    void getPermutation(
        PermutationSortDirection direction,
        PermutationSortStability stability,
        size_t limit,
        int nan_direction_hint,
        Permutation & res) const override;

    void updatePermutation(
        PermutationSortDirection direction,
        PermutationSortStability stability,
        size_t limit,
        int nan_direction_hint,
        Permutation & res,
        EqualRanges & equal_ranges) const override;

    [[nodiscard]] ColumnPtr replicate(const Offsets & offsets) const override;

    void gather(ColumnGathererStream & /* gatherer_stream */) override { throwNotSupported(); }

    [[nodiscard]] size_t byteSize() const override;
    [[nodiscard]] size_t byteSizeAt(size_t) const override;
    [[nodiscard]] size_t allocatedBytes() const override;

    /* There is no definition of "default" value in FSST column */
    [[nodiscard]] double getRatioOfDefaultRows(double) const override { throwNotSupported(); }
    [[nodiscard]] UInt64 getNumberOfDefaultRows() const override { throwNotSupported(); }
    void getIndicesOfNonDefaultRows(Offsets &, size_t, size_t) const override { throwNotSupported(); }

    void doInsertRangeFrom(const IColumn & src, size_t start, size_t length) override;

    int doCompareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const override;

    WrappedPtr getStringColumn() const { return string_column; }
    const std::vector<BatchDsc> & getDecoders() const { return decoders; }
    const std::vector<UInt64> & getLengths() const { return origin_lengths; }

    void getExtremes(Field & min, Field & max, size_t start, size_t end) const override;

    void append(const CompressedField & x);
    void appendNewBatch(const CompressedField & x, std::shared_ptr<fsst_decoder_t> decoder);

    [[noreturn]] static void throwNotImplemented()
    {
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED, "text escaped/text quoted/text csv/whole text/json serialization are not implemented for FSST");
    }

    [[noreturn]] static void throwNotSupported() { throw Exception(ErrorCodes::LOGICAL_ERROR, "functionality is not supported for FSST"); }
};

ColumnPtr recursiveRemoveFSST(const ColumnPtr & column);

};
