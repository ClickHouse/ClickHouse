#pragma once

#include <Columns/ColumnVectorHelper.h>
#include <Columns/IColumn.h>
#include <Columns/IColumnImpl.h>
#include <Core/Field.h>
#include <Core/DecimalFunctions.h>
#include <Common/typeid_cast.h>
#include <common/sort.h>

#include <cmath>


namespace DB
{
/// PaddedPODArray extended by Decimal scale
template <typename T>
class DecimalPaddedPODArray : public PaddedPODArray<T>
{
public:
    using Base = PaddedPODArray<T>;
    using Base::operator[];

    DecimalPaddedPODArray(size_t size, UInt32 scale_)
    :   Base(size),
        scale(scale_)
    {}

    DecimalPaddedPODArray(const DecimalPaddedPODArray & other)
    :   Base(other.begin(), other.end()),
        scale(other.scale)
    {}

    DecimalPaddedPODArray(DecimalPaddedPODArray && other)
    {
        this->swap(other);
        std::swap(scale, other.scale);
    }

    DecimalPaddedPODArray & operator=(DecimalPaddedPODArray && other)
    {
        this->swap(other);
        std::swap(scale, other.scale);
        return *this;
    }

    UInt32 getScale() const { return scale; }

private:
    UInt32 scale;
};

/// Prevent implicit template instantiation of DecimalPaddedPODArray for common decimal types

extern template class DecimalPaddedPODArray<Decimal32>;
extern template class DecimalPaddedPODArray<Decimal64>;
extern template class DecimalPaddedPODArray<Decimal128>;
extern template class DecimalPaddedPODArray<Decimal256>;
extern template class DecimalPaddedPODArray<DateTime64>;

/// A ColumnVector for Decimals
template <typename T>
class ColumnDecimal final : public COWHelper<ColumnVectorHelper, ColumnDecimal<T>>
{
    static_assert(IsDecimalNumber<T>);

private:
    using Self = ColumnDecimal;
    friend class COWHelper<ColumnVectorHelper, Self>;

public:
    using ValueType = T;
    using NativeT = typename T::NativeType;
    using Container = DecimalPaddedPODArray<T>;

private:
    ColumnDecimal(const size_t n, UInt32 scale_)
    :   data(n, scale_),
        scale(scale_)
    {}

    ColumnDecimal(const ColumnDecimal & src)
    :   data(src.data),
        scale(src.scale)
    {}

public:
    const char * getFamilyName() const override { return TypeName<T>; }
    TypeIndex getDataType() const override { return TypeId<T>; }

    bool isNumeric() const override { return false; }
    bool canBeInsideNullable() const override { return true; }
    bool isFixedAndContiguous() const final { return true; }
    size_t sizeOfValueIfFixed() const override { return sizeof(T); }

    size_t size() const override { return data.size(); }
    size_t byteSize() const override { return data.size() * sizeof(data[0]); }
    size_t byteSizeAt(size_t) const override { return sizeof(data[0]); }
    size_t allocatedBytes() const override { return data.allocated_bytes(); }
    void protect() override { data.protect(); }
    void reserve(size_t n) override { data.reserve(n); }

    void insertFrom(const IColumn & src, size_t n) override { data.push_back(static_cast<const Self &>(src).getData()[n]); }
    void insertData(const char * src, size_t /*length*/) override;
    void insertDefault() override { data.push_back(T()); }
    virtual void insertManyDefaults(size_t length) override
    {
        data.resize_fill(data.size() + length);
    }
    void insert(const Field & x) override { data.push_back(DB::get<T>(x)); }
    void insertRangeFrom(const IColumn & src, size_t start, size_t length) override;

    void popBack(size_t n) override
    {
        data.resize_assume_reserved(data.size() - n);
    }

    StringRef getRawData() const override
    {
        return StringRef(reinterpret_cast<const char*>(data.data()), byteSize());
    }

    StringRef getDataAt(size_t n) const override
    {
        return StringRef(reinterpret_cast<const char *>(&data[n]), sizeof(data[n]));
    }

    Float64 getFloat64(size_t n) const final { return DecimalUtils::convertTo<Float64>(data[n], scale); }

    StringRef serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const override;
    const char * deserializeAndInsertFromArena(const char * pos) override;
    const char * skipSerializedInArena(const char * pos) const override;
    void updateHashWithValue(size_t n, SipHash & hash) const override;
    void updateWeakHash32(WeakHash32 & hash) const override;
    void updateHashFast(SipHash & hash) const override;
    int compareAt(size_t n, size_t m, const IColumn & rhs_, int nan_direction_hint) const override;
    void compareColumn(const IColumn & rhs, size_t rhs_row_num,
                       PaddedPODArray<UInt64> * row_indexes, PaddedPODArray<Int8> & compare_results,
                       int direction, int nan_direction_hint) const override;
    bool hasEqualValues() const override;
    void getPermutation(bool reverse, size_t limit, int nan_direction_hint, IColumn::Permutation & res) const override;
    void updatePermutation(bool reverse, size_t limit, int, IColumn::Permutation & res, EqualRanges& equal_range) const override;

    MutableColumnPtr cloneResized(size_t size) const override;

    Field operator[](size_t n) const override { return DecimalField(data[n], scale); }
    void get(size_t n, Field & res) const override { res = (*this)[n]; }
    bool getBool(size_t n) const override { return bool(data[n].value); }
    Int64 getInt(size_t n) const override { return Int64(data[n].value) * scale; }
    UInt64 get64(size_t n) const override;
    bool isDefaultAt(size_t n) const override { return data[n].value == 0; }

    ColumnPtr filter(const IColumn::Filter & filt, ssize_t result_size_hint) const override;
    ColumnPtr permute(const IColumn::Permutation & perm, size_t limit) const override;
    ColumnPtr index(const IColumn & indexes, size_t limit) const override;

    template <typename Type>
    ColumnPtr indexImpl(const PaddedPODArray<Type> & indexes, size_t limit) const;

    ColumnPtr replicate(const IColumn::Offsets & offsets) const override;
    void getExtremes(Field & min, Field & max) const override;

    MutableColumns scatter(IColumn::ColumnIndex num_columns, const IColumn::Selector & selector) const override
    {
        return this->template scatterImpl<Self>(num_columns, selector);
    }

    void gather(ColumnGathererStream & gatherer_stream) override;

    bool structureEquals(const IColumn & rhs) const override
    {
        if (auto rhs_concrete = typeid_cast<const ColumnDecimal<T> *>(&rhs))
            return scale == rhs_concrete->scale;
        return false;
    }

    ColumnPtr compress() const override;


    void insertValue(const T value) { data.push_back(value); }
    Container & getData() { return data; }
    const Container & getData() const { return data; }
    const T & getElement(size_t n) const { return data[n]; }
    T & getElement(size_t n) { return data[n]; }

    UInt32 getScale() const {return scale;}

protected:
    Container data;
    UInt32 scale;

    template <typename U>
    void permutation(bool reverse, size_t limit, PaddedPODArray<U> & res) const
    {
        size_t s = data.size();
        res.resize(s);
        for (U i = 0; i < s; ++i)
            res[i] = i;

        auto sort_end = res.end();
        if (limit && limit < s)
            sort_end = res.begin() + limit;

        if (reverse)
            partial_sort(res.begin(), sort_end, res.end(), [this](size_t a, size_t b) { return data[a] > data[b]; });
        else
            partial_sort(res.begin(), sort_end, res.end(), [this](size_t a, size_t b) { return data[a] < data[b]; });
    }
};

template <typename T>
template <typename Type>
ColumnPtr ColumnDecimal<T>::indexImpl(const PaddedPODArray<Type> & indexes, size_t limit) const
{
    size_t size = indexes.size();

    if (limit == 0)
        limit = size;
    else
        limit = std::min(size, limit);

    auto res = this->create(limit, scale);
    typename Self::Container & res_data = res->getData();
    for (size_t i = 0; i < limit; ++i)
        res_data[i] = data[indexes[i]];

    return res;
}


/// Prevent implicit template instantiation of ColumnDecimal for common decimal types

extern template class ColumnDecimal<Decimal32>;
extern template class ColumnDecimal<Decimal64>;
extern template class ColumnDecimal<Decimal128>;
extern template class ColumnDecimal<Decimal256>;
extern template class ColumnDecimal<DateTime64>;


}
