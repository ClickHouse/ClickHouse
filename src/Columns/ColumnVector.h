#pragma once

#include <Columns/ColumnFixedSizeHelper.h>
#include <Columns/IColumn.h>
#include <Columns/IColumnImpl.h>
#include <Common/TargetSpecific.h>
#include <Common/assert_cast.h>
#include <Core/CompareHelper.h>
#include <Core/Field.h>
#include <Core/TypeId.h>
#include <base/TypeName.h>
#include <base/unaligned.h>

#include "config.h"

#if USE_MULTITARGET_CODE
#    include <immintrin.h>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}


/** A template for columns that use a simple array to store.
 */
template <typename T>
class ColumnVector final : public COWHelper<IColumnHelper<ColumnVector<T>, ColumnFixedSizeHelper>, ColumnVector<T>>
{
    static_assert(!is_decimal<T>);

private:
    using Self = ColumnVector;
    friend class COWHelper<IColumnHelper<Self, ColumnFixedSizeHelper>, Self>;

    struct less;
    struct less_stable;
    struct greater;
    struct greater_stable;
    struct equals;

public:
    using ValueType = T;
    using Container = PaddedPODArray<ValueType>;

private:
    ColumnVector() = default;
    explicit ColumnVector(const size_t n) : data(n) {}
    ColumnVector(const size_t n, const ValueType x) : data(n, x) {}
    ColumnVector(const ColumnVector & src) : data(src.data.begin(), src.data.end()) {}

    /// Sugar constructor.
    ColumnVector(std::initializer_list<T> il) : data{il} {}

public:
    bool isNumeric() const override { return is_arithmetic_v<T>; }

    size_t size() const override
    {
        return data.size();
    }

#if !defined(DEBUG_OR_SANITIZER_BUILD)
    void insertFrom(const IColumn & src, size_t n) override
#else
    void doInsertFrom(const IColumn & src, size_t n) override
#endif
    {
        data.push_back(assert_cast<const Self &>(src).getData()[n]);
    }

#if !defined(DEBUG_OR_SANITIZER_BUILD)
    void insertManyFrom(const IColumn & src, size_t position, size_t length) override
#else
    void doInsertManyFrom(const IColumn & src, size_t position, size_t length) override
#endif
    {
        ValueType v = assert_cast<const Self &>(src).getData()[position];
        data.resize_fill(data.size() + length, v);
    }

    void insertMany(const Field & field, size_t length) override
    {
        data.resize_fill(data.size() + length, static_cast<T>(field.safeGet<T>()));
    }

    void insertData(const char * pos, size_t) override
    {
        data.emplace_back(unalignedLoad<T>(pos));
    }

    void insertDefault() override
    {
        data.push_back(T());
    }

    void insertManyDefaults(size_t length) override
    {
        data.resize_fill(data.size() + length, T());
    }

    void popBack(size_t n) override
    {
        data.resize_assume_reserved(data.size() - n);
    }

    const char * deserializeAndInsertFromArena(const char * pos) override;

    const char * skipSerializedInArena(const char * pos) const override;

    void updateHashWithValue(size_t n, SipHash & hash) const override;

    WeakHash32 getWeakHash32() const override;

    void updateHashFast(SipHash & hash) const override;

    size_t byteSize() const override
    {
        return data.size() * sizeof(data[0]);
    }

    size_t byteSizeAt(size_t) const override
    {
        return sizeof(data[0]);
    }

    size_t allocatedBytes() const override
    {
        return data.allocated_bytes();
    }

    void protect() override
    {
        data.protect();
    }

    void insertValue(const T value)
    {
        data.push_back(value);
    }

    template <class U>
    constexpr int compareAtOther(size_t n, size_t m, const ColumnVector<U> & rhs, int nan_direction_hint) const
    {
        return CompareHelper<T, U>::compare(data[n], rhs.data[m], nan_direction_hint);
    }

    /// This method implemented in header because it could be possibly devirtualized.
#if !defined(DEBUG_OR_SANITIZER_BUILD)
    int compareAt(size_t n, size_t m, const IColumn & rhs_, int nan_direction_hint) const override
#else
    int doCompareAt(size_t n, size_t m, const IColumn & rhs_, int nan_direction_hint) const override
#endif
    {
        return CompareHelper<T>::compare(data[n], assert_cast<const Self &>(rhs_).data[m], nan_direction_hint);
    }

#if USE_EMBEDDED_COMPILER

    bool isComparatorCompilable() const override;

    llvm::Value * compileComparator(llvm::IRBuilderBase & /*builder*/, llvm::Value * /*lhs*/, llvm::Value * /*rhs*/, llvm::Value * /*nan_direction_hint*/) const override;

#endif

    void getPermutation(IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability,
                    size_t limit, int nan_direction_hint, IColumn::Permutation & res) const override;

    void updatePermutation(IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability,
                    size_t limit, int nan_direction_hint, IColumn::Permutation & res, EqualRanges& equal_ranges) const override;

    size_t estimateCardinalityInPermutedRange(const IColumn::Permutation & permutation, const EqualRange & equal_range) const override;

    void reserve(size_t n) override
    {
        data.reserve_exact(n);
    }

    size_t capacity() const override
    {
        return data.capacity();
    }

    void shrinkToFit() override
    {
        data.shrink_to_fit();
    }

    const char * getFamilyName() const override { return TypeName<T>.data(); }
    TypeIndex getDataType() const override { return TypeToTypeIndex<T>; }

    MutableColumnPtr cloneResized(size_t size) const override;

    Field operator[](size_t n) const override
    {
        assert(n < data.size()); /// This assert is more strict than the corresponding assert inside PODArray.
        return data[n];
    }


    void get(size_t n, Field & res) const override
    {
        res = (*this)[n];
    }

    UInt64 get64(size_t n) const override;

    Float64 getFloat64(size_t n) const override;
    Float32 getFloat32(size_t n) const override;

    /// Out of range conversion is permitted.
    UInt64 NO_SANITIZE_UNDEFINED getUInt(size_t n) const override
    {
        if constexpr (is_arithmetic_v<T>)
            return UInt64(data[n]);
        else
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot get the value of {} as UInt", TypeName<T>);
    }

    /// Out of range conversion is permitted.
    Int64 NO_SANITIZE_UNDEFINED getInt(size_t n) const override
    {
        if constexpr (is_arithmetic_v<T>)
            return Int64(data[n]);
        else
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot get the value of {} as Int", TypeName<T>);
    }

    bool getBool(size_t n) const override
    {
        if constexpr (is_arithmetic_v<T>)
            return bool(data[n]);
        else
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot get the value of {} as bool", TypeName<T>);
    }

    void insert(const Field & x) override
    {
        data.push_back(static_cast<T>(x.safeGet<T>()));
    }

    bool tryInsert(const DB::Field & x) override;

#if !defined(DEBUG_OR_SANITIZER_BUILD)
    void insertRangeFrom(const IColumn & src, size_t start, size_t length) override;
#else
    void doInsertRangeFrom(const IColumn & src, size_t start, size_t length) override;
#endif

    ColumnPtr filter(const IColumn::Filter & filt, ssize_t result_size_hint) const override;

    void expand(const IColumn::Filter & mask, bool inverted) override;

    ColumnPtr permute(const IColumn::Permutation & perm, size_t limit) const override;

    ColumnPtr index(const IColumn & indexes, size_t limit) const override;

    template <typename Type>
    ColumnPtr indexImpl(const PaddedPODArray<Type> & indexes, size_t limit) const;

    ColumnPtr replicate(const IColumn::Offsets & offsets) const override;

    void getExtremes(Field & min, Field & max) const override;

    bool canBeInsideNullable() const override { return true; }
    bool isFixedAndContiguous() const override { return true; }
    size_t sizeOfValueIfFixed() const override { return sizeof(T); }

    std::string_view getRawData() const override
    {
        return {reinterpret_cast<const char*>(data.data()), byteSize()};
    }

    StringRef getDataAt(size_t n) const override
    {
        return StringRef(reinterpret_cast<const char *>(&data[n]), sizeof(data[n]));
    }

    bool isDefaultAt(size_t n) const override { return data[n] == T{}; }

    bool structureEquals(const IColumn & rhs) const override
    {
        return typeid(rhs) == typeid(ColumnVector<T>);
    }

    ColumnPtr createWithOffsets(const IColumn::Offsets & offsets, const ColumnConst & column_with_default_value, size_t total_rows, size_t shift) const override;

    ColumnPtr compress() const override;

    /// Replace elements that match the filter with zeroes. If inverted replaces not matched elements.
    void applyZeroMap(const IColumn::Filter & filt, bool inverted = false);

    /** More efficient methods of manipulation - to manipulate with data directly. */
    Container & getData()
    {
        return data;
    }

    const Container & getData() const
    {
        return data;
    }

    const T & getElement(size_t n) const
    {
        return data[n];
    }

    T & getElement(size_t n)
    {
        return data[n];
    }

protected:
    Container data;
};

DECLARE_DEFAULT_CODE(
template <typename Container, typename Type>
inline void vectorIndexImpl(const Container & data, const PaddedPODArray<Type> & indexes, size_t limit, Container & res_data)
{
    for (size_t i = 0; i < limit; ++i)
        res_data[i] = data[indexes[i]];
}
);

DECLARE_AVX512VBMI_SPECIFIC_CODE(
template <typename Container, typename Type>
inline void vectorIndexImpl(const Container & data, const PaddedPODArray<Type> & indexes, size_t limit, Container & res_data)
{
    static constexpr UInt64 MASK64 = 0xffffffffffffffff;
    const size_t limit64 = limit & ~63;
    size_t pos = 0;
    size_t data_size = data.size();

    auto data_pos = reinterpret_cast<const UInt8 *>(data.data());
    auto indexes_pos = reinterpret_cast<const UInt8 *>(indexes.data());
    auto res_pos = reinterpret_cast<UInt8 *>(res_data.data());

    if (limit == 0)
        return;   /// nothing to do, just return

    if (data_size <= 64)
    {
        /// one single mask load for table size <= 64
        __mmask64 last_mask = MASK64 >> (64 - data_size);
        __m512i table1 = _mm512_maskz_loadu_epi8(last_mask, data_pos);

        /// 64 bytes table lookup using one single permutexvar_epi8
        while (pos < limit64)
        {
            __m512i vidx = _mm512_loadu_epi8(indexes_pos + pos);
            __m512i out = _mm512_permutexvar_epi8(vidx, table1);
            _mm512_storeu_epi8(res_pos + pos, out);
            pos += 64;
        }
        /// tail handling
        if (limit > limit64)
        {
            __mmask64 tail_mask = MASK64 >> (limit64 + 64 - limit);
            __m512i vidx = _mm512_maskz_loadu_epi8(tail_mask, indexes_pos + pos);
            __m512i out = _mm512_permutexvar_epi8(vidx, table1);
            _mm512_mask_storeu_epi8(res_pos + pos, tail_mask, out);
        }
    }
    else if (data_size <= 128)
    {
        /// table size (64, 128] requires 2 zmm load
        __mmask64 last_mask = MASK64 >> (128 - data_size);
        __m512i table1 = _mm512_loadu_epi8(data_pos);
        __m512i table2 = _mm512_maskz_loadu_epi8(last_mask, data_pos + 64);

        /// 128 bytes table lookup using one single permute2xvar_epi8
        while (pos < limit64)
        {
            __m512i vidx = _mm512_loadu_epi8(indexes_pos + pos);
            __m512i out = _mm512_permutex2var_epi8(table1, vidx, table2);
            _mm512_storeu_epi8(res_pos + pos, out);
            pos += 64;
        }
        if (limit > limit64)
        {
            __mmask64 tail_mask = MASK64 >> (limit64 + 64 - limit);
            __m512i vidx = _mm512_maskz_loadu_epi8(tail_mask, indexes_pos + pos);
            __m512i out = _mm512_permutex2var_epi8(table1, vidx, table2);
            _mm512_mask_storeu_epi8(res_pos + pos, tail_mask, out);
        }
    }
    else
    {
        if (data_size > 256)
        {
            /// byte index will not exceed 256 boundary.
            data_size = 256;
        }

        __m512i table1 = _mm512_loadu_epi8(data_pos);
        __m512i table2 = _mm512_loadu_epi8(data_pos + 64);
        __m512i table3, table4;
        if (data_size <= 192)
        {
            /// only 3 tables need to load if size <= 192
            __mmask64 last_mask = MASK64 >> (192 - data_size);
            table3 = _mm512_maskz_loadu_epi8(last_mask, data_pos + 128);
            table4 = _mm512_setzero_si512();
        }
        else
        {
            __mmask64 last_mask = MASK64 >> (256 - data_size);
            table3 = _mm512_loadu_epi8(data_pos + 128);
            table4 = _mm512_maskz_loadu_epi8(last_mask, data_pos + 192);
        }

        /// 256 bytes table lookup can use: 2 permute2xvar_epi8 plus 1 blender with MSB
        while (pos < limit64)
        {
            __m512i vidx = _mm512_loadu_epi8(indexes_pos + pos);
            __m512i tmp1 = _mm512_permutex2var_epi8(table1, vidx, table2);
            __m512i tmp2 = _mm512_permutex2var_epi8(table3, vidx, table4);
            __mmask64 msb = _mm512_movepi8_mask(vidx);
            __m512i out = _mm512_mask_blend_epi8(msb, tmp1, tmp2);
            _mm512_storeu_epi8(res_pos + pos, out);
            pos += 64;
        }
        if (limit > limit64)
        {
            __mmask64 tail_mask = MASK64 >> (limit64 + 64 - limit);
            __m512i vidx = _mm512_maskz_loadu_epi8(tail_mask, indexes_pos + pos);
            __m512i tmp1 = _mm512_permutex2var_epi8(table1, vidx, table2);
            __m512i tmp2 = _mm512_permutex2var_epi8(table3, vidx, table4);
            __mmask64 msb = _mm512_movepi8_mask(vidx);
            __m512i out = _mm512_mask_blend_epi8(msb, tmp1, tmp2);
            _mm512_mask_storeu_epi8(res_pos + pos, tail_mask, out);
        }
    }
}
);

template <typename T>
template <typename Type>
ColumnPtr ColumnVector<T>::indexImpl(const PaddedPODArray<Type> & indexes, size_t limit) const
{
    assert(limit <= indexes.size());

    auto res = this->create(limit);
    typename Self::Container & res_data = res->getData();
#if USE_MULTITARGET_CODE
    if constexpr (sizeof(T) == 1 && sizeof(Type) == 1)
    {
        /// VBMI optimization only applicable for (U)Int8 types
        if (isArchSupported(TargetArch::AVX512VBMI))
        {
            TargetSpecific::AVX512VBMI::vectorIndexImpl<Container, Type>(data, indexes, limit, res_data);
            return res;
        }
    }
#endif
    TargetSpecific::Default::vectorIndexImpl<Container, Type>(data, indexes, limit, res_data);

    return res;
}

template <class TCol>
concept is_col_vector = std::is_same_v<TCol, ColumnVector<typename TCol::ValueType>>;

/// Prevent implicit template instantiation of ColumnVector for common types

extern template class ColumnVector<UInt8>;
extern template class ColumnVector<UInt16>;
extern template class ColumnVector<UInt32>;
extern template class ColumnVector<UInt64>;
extern template class ColumnVector<UInt128>;
extern template class ColumnVector<UInt256>;
extern template class ColumnVector<Int8>;
extern template class ColumnVector<Int16>;
extern template class ColumnVector<Int32>;
extern template class ColumnVector<Int64>;
extern template class ColumnVector<Int128>;
extern template class ColumnVector<Int256>;
extern template class ColumnVector<Float32>;
extern template class ColumnVector<Float64>;
extern template class ColumnVector<UUID>;
extern template class ColumnVector<IPv4>;
extern template class ColumnVector<IPv6>;

}
