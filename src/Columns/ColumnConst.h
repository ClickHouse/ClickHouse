#pragma once

#include <Core/Field.h>
#include <Common/Exception.h>
#include <Columns/IColumn.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>
#include <Common/PODArray.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}


/** ColumnConst contains another column with single element,
  *  but looks like a column with arbitrary amount of same elements.
  */
class ColumnConst final : public COWHelper<IColumnHelper<ColumnConst>, ColumnConst>
{
private:
    friend class COWHelper<IColumnHelper<ColumnConst>, ColumnConst>;

    WrappedPtr data;
    size_t s;

    ColumnConst(const ColumnPtr & data, size_t s_);
    ColumnConst(const ColumnConst & src) = default;

public:
    bool isConst() const override { return true; }

    ColumnPtr convertToFullColumn() const;

    ColumnPtr convertToFullColumnIfConst() const override
    {
        return convertToFullColumn();
    }

    ColumnPtr removeLowCardinality() const;

    std::string getName() const override
    {
        return "Const(" + data->getName() + ")";
    }

    const char * getFamilyName() const override
    {
        return "Const";
    }

    TypeIndex getDataType() const override
    {
        return data->getDataType();
    }

    MutableColumnPtr cloneResized(size_t new_size) const override
    {
        return ColumnConst::create(data, new_size);
    }

    size_t size() const override
    {
        return s;
    }

    Field operator[](size_t) const override
    {
        return (*data)[0];
    }

    void get(size_t, Field & res) const override
    {
        data->get(0, res);
    }

    StringRef getDataAt(size_t) const override
    {
        return data->getDataAt(0);
    }

    UInt64 get64(size_t) const override
    {
        return data->get64(0);
    }

    UInt64 getUInt(size_t) const override
    {
        return data->getUInt(0);
    }

    Int64 getInt(size_t) const override
    {
        return data->getInt(0);
    }

    bool getBool(size_t) const override
    {
        return data->getBool(0);
    }

    Float64 getFloat64(size_t) const override
    {
        return data->getFloat64(0);
    }

    Float32 getFloat32(size_t) const override
    {
        return data->getFloat32(0);
    }

    bool isDefaultAt(size_t) const override
    {
        return data->isDefaultAt(0);
    }

    bool isNullAt(size_t) const override
    {
        return data->isNullAt(0);
    }

#if !defined(DEBUG_OR_SANITIZER_BUILD)
    void insertRangeFrom(const IColumn &, size_t /*start*/, size_t length) override
#else
    void doInsertRangeFrom(const IColumn &, size_t /*start*/, size_t length) override
#endif
    {
        s += length;
    }

    void insert(const Field &) override
    {
        ++s;
    }

    bool tryInsert(const Field & field) override
    {
        auto tmp = data->cloneEmpty();
        if (!tmp->tryInsert(field))
            return false;
        ++s;
        return true;
    }

    void insertData(const char *, size_t) override
    {
        ++s;
    }

#if !defined(DEBUG_OR_SANITIZER_BUILD)
    void insertFrom(const IColumn &, size_t) override
#else
    void doInsertFrom(const IColumn &, size_t) override
#endif
    {
        ++s;
    }

#if !defined(DEBUG_OR_SANITIZER_BUILD)
    void insertManyFrom(const IColumn & /*src*/, size_t /* position */, size_t length) override { s += length; }
#else
    void doInsertManyFrom(const IColumn & /*src*/, size_t /* position */, size_t length) override { s += length; }
#endif

    void insertDefault() override
    {
        ++s;
    }

    void popBack(size_t n) override
    {
        s -= n;
    }

    StringRef serializeValueIntoArena(size_t, Arena & arena, char const *& begin) const override
    {
        return data->serializeValueIntoArena(0, arena, begin);
    }

    char * serializeValueIntoMemory(size_t, char * memory) const override
    {
        return data->serializeValueIntoMemory(0, memory);
    }

    const char * deserializeAndInsertFromArena(const char * pos) override
    {
        const auto * res = data->deserializeAndInsertFromArena(pos);
        data->popBack(1);
        ++s;
        return res;
    }

    const char * skipSerializedInArena(const char * pos) const override
    {
        return data->skipSerializedInArena(pos);
    }

    void updateHashWithValue(size_t, SipHash & hash) const override
    {
        data->updateHashWithValue(0, hash);
    }

    WeakHash32 getWeakHash32() const override;

    void updateHashFast(SipHash & hash) const override
    {
        data->updateHashFast(hash);
    }

    ColumnPtr filter(const Filter & filt, ssize_t result_size_hint) const override;
    void expand(const Filter & mask, bool inverted) override;

    ColumnPtr replicate(const Offsets & offsets) const override;
    ColumnPtr permute(const Permutation & perm, size_t limit) const override;
    ColumnPtr index(const IColumn & indexes, size_t limit) const override;
    void getPermutation(PermutationSortDirection direction, PermutationSortStability stability,
                        size_t limit, int nan_direction_hint, Permutation & res) const override;
    void updatePermutation(PermutationSortDirection direction, PermutationSortStability stability,
                        size_t limit, int nan_direction_hint, Permutation & res, EqualRanges & equal_ranges) const override;

    size_t byteSize() const override
    {
        return data->byteSize() + sizeof(s);
    }

    size_t byteSizeAt(size_t) const override
    {
        return data->byteSizeAt(0);
    }

    size_t allocatedBytes() const override
    {
        return data->allocatedBytes() + sizeof(s);
    }

#if !defined(DEBUG_OR_SANITIZER_BUILD)
    int compareAt(size_t, size_t, const IColumn & rhs, int nan_direction_hint) const override
#else
    int doCompareAt(size_t, size_t, const IColumn & rhs, int nan_direction_hint) const override
#endif
    {
        return data->compareAt(0, 0, *assert_cast<const ColumnConst &>(rhs).data, nan_direction_hint);
    }

    void compareColumn(const IColumn & rhs, size_t rhs_row_num,
                       PaddedPODArray<UInt64> * row_indexes, PaddedPODArray<Int8> & compare_results,
                       int direction, int nan_direction_hint) const override;

    bool hasEqualValues() const override { return true; }

    MutableColumns scatter(ColumnIndex num_columns, const Selector & selector) const override;

    void gather(ColumnGathererStream &) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot gather into constant column {}", getName());
    }

    void getExtremes(Field & min, Field & max) const override
    {
        data->getExtremes(min, max);
    }

    void forEachSubcolumn(MutableColumnCallback callback) override
    {
        callback(data);
    }

    void forEachSubcolumnRecursively(RecursiveMutableColumnCallback callback) override
    {
        callback(*data);
        data->forEachSubcolumnRecursively(callback);
    }

    bool structureEquals(const IColumn & rhs) const override
    {
        if (const auto * rhs_concrete = typeid_cast<const ColumnConst *>(&rhs))
            return data->structureEquals(*rhs_concrete->data);
        return false;
    }

    double getRatioOfDefaultRows(double) const override
    {
        return data->isDefaultAt(0) ? 1.0 : 0.0;
    }

    UInt64 getNumberOfDefaultRows() const override
    {
        return data->isDefaultAt(0) ? s : 0;
    }

    void getIndicesOfNonDefaultRows(Offsets & indices, size_t from, size_t limit) const override
    {
        if (!data->isDefaultAt(0))
        {
            size_t to = limit && from + limit < size() ? from + limit : size();
            indices.reserve_exact(indices.size() + to - from);
            for (size_t i = from; i < to; ++i)
                indices.push_back(i);
        }
    }

    bool isNullable() const override { return isColumnNullable(*data); }
    bool onlyNull() const override { return data->isNullAt(0); }
    bool isNumeric() const override { return data->isNumeric(); }
    bool isFixedAndContiguous() const override { return data->isFixedAndContiguous(); }
    bool valuesHaveFixedSize() const override { return data->valuesHaveFixedSize(); }
    size_t sizeOfValueIfFixed() const override { return data->sizeOfValueIfFixed(); }
    std::string_view getRawData() const override { return data->getRawData(); }

    /// Not part of the common interface.

    IColumn & getDataColumn() { return *data; }
    const IColumn & getDataColumn() const { return *data; }
    const ColumnPtr & getDataColumnPtr() const { return data; }

    Field getField() const { return getDataColumn()[0]; }

    /// The constant value. It is valid even if the size of the column is 0.
    template <typename T>
    T getValue() const { return static_cast<T>(getField().safeGet<T>()); }

    bool isCollationSupported() const override { return data->isCollationSupported(); }

    bool hasDynamicStructure() const override { return data->hasDynamicStructure(); }
};

ColumnConst::Ptr createColumnConst(const ColumnPtr & column, Field value);
ColumnConst::Ptr createColumnConst(const ColumnPtr & column, size_t const_value_index);
ColumnConst::Ptr createColumnConstWithDefaultValue(const ColumnPtr  &column);


}
