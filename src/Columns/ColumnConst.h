#pragma once

#include <Core/Field.h>
#include <Common/Exception.h>
#include <Columns/IColumn.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}


/** ColumnConst contains another column with single element,
  *  but looks like a column with arbitrary amount of same elements.
  */
class ColumnConst final : public COWHelper<IColumn, ColumnConst>
{
private:
    friend class COWHelper<IColumn, ColumnConst>;

    WrappedPtr data;
    size_t s;

    ColumnConst(const ColumnPtr & data, size_t s_);
    ColumnConst(const ColumnConst & src) = default;

public:
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

    StringRef getDataAtWithTerminatingZero(size_t) const override
    {
        return data->getDataAtWithTerminatingZero(0);
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

    bool isNullAt(size_t) const override
    {
        return data->isNullAt(0);
    }

    void insertRangeFrom(const IColumn &, size_t /*start*/, size_t length) override
    {
        s += length;
    }

    void insert(const Field &) override
    {
        ++s;
    }

    void insertData(const char *, size_t) override
    {
        ++s;
    }

    void insertFrom(const IColumn &, size_t) override
    {
        ++s;
    }

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

    const char * deserializeAndInsertFromArena(const char * pos) override
    {
        auto res = data->deserializeAndInsertFromArena(pos);
        data->popBack(1);
        ++s;
        return res;
    }

    void updateHashWithValue(size_t, SipHash & hash) const override
    {
        data->updateHashWithValue(0, hash);
    }

    void updateWeakHash32(WeakHash32 & hash) const override;

    void updateHashFast(SipHash & hash) const override
    {
        data->updateHashFast(hash);
    }

    ColumnPtr filter(const Filter & filt, ssize_t result_size_hint) const override;
    ColumnPtr replicate(const Offsets & offsets) const override;
    ColumnPtr permute(const Permutation & perm, size_t limit) const override;
    ColumnPtr index(const IColumn & indexes, size_t limit) const override;
    void getPermutation(bool reverse, size_t limit, int nan_direction_hint, Permutation & res) const override;
    void updatePermutation(bool reverse, size_t limit, int nan_direction_hint, Permutation & res, EqualRanges & equal_range) const override;

    size_t byteSize() const override
    {
        return data->byteSize() + sizeof(s);
    }

    size_t allocatedBytes() const override
    {
        return data->allocatedBytes() + sizeof(s);
    }

    int compareAt(size_t, size_t, const IColumn & rhs, int nan_direction_hint) const override
    {
        return data->compareAt(0, 0, *assert_cast<const ColumnConst &>(rhs).data, nan_direction_hint);
    }

    void compareColumn(const IColumn & rhs, size_t rhs_row_num,
                       PaddedPODArray<UInt64> * row_indexes, PaddedPODArray<Int8> & compare_results,
                       int direction, int nan_direction_hint) const override;

    MutableColumns scatter(ColumnIndex num_columns, const Selector & selector) const override;

    void gather(ColumnGathererStream &) override
    {
        throw Exception("Cannot gather into constant column " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    void getExtremes(Field & min, Field & max) const override
    {
        data->getExtremes(min, max);
    }

    void forEachSubcolumn(ColumnCallback callback) override
    {
        callback(data);
    }

    bool structureEquals(const IColumn & rhs) const override
    {
        if (auto rhs_concrete = typeid_cast<const ColumnConst *>(&rhs))
            return data->structureEquals(*rhs_concrete->data);
        return false;
    }

    bool isNullable() const override { return isColumnNullable(*data); }
    bool onlyNull() const override { return data->isNullAt(0); }
    bool isNumeric() const override { return data->isNumeric(); }
    bool isFixedAndContiguous() const override { return data->isFixedAndContiguous(); }
    bool valuesHaveFixedSize() const override { return data->valuesHaveFixedSize(); }
    size_t sizeOfValueIfFixed() const override { return data->sizeOfValueIfFixed(); }
    StringRef getRawData() const override { return data->getRawData(); }

    /// Not part of the common interface.

    IColumn & getDataColumn() { return *data; }
    const IColumn & getDataColumn() const { return *data; }
    const ColumnPtr & getDataColumnPtr() const { return data; }

    Field getField() const { return getDataColumn()[0]; }

    /// The constant value. It is valid even if the size of the column is 0.
    template <typename T>
    T getValue() const { return getField().safeGet<NearestFieldType<T>>(); }

    bool isCollationSupported() const override { return data->isCollationSupported(); }
};

}
