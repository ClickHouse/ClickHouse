#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionSpaceFillingCurve.h>
#include <Functions/IFunction.h>
#include <Functions/PerformanceAdaptors.h>

#include <morton-nd/mortonND_LUT.h>
#if defined(__BMI2__)
#include <morton-nd/mortonND_BMI2.h>
#endif

#include <array>
#include <cstddef>
#include <tuple>
#include <utility>

namespace DB
{

// NOLINTBEGIN(bugprone-switch-missing-default-case)

#define EXTRACT_VECTOR(INDEX) \
        auto col##INDEX = ColumnUInt64::create(); \
        auto & vec##INDEX = col##INDEX->getData(); \
        vec##INDEX.resize(input_rows_count);

#define MASK(IDX, ...) \
        ((mask) ? shrink(mask->getColumn((IDX)).getUInt(0), std::get<IDX>(__VA_ARGS__)) : std::get<IDX>(__VA_ARGS__))

#define EXECUTE() \
    size_t nd; \
    const auto * col_const = typeid_cast<const ColumnConst *>(arguments[0].column.get()); \
    const auto * mask = typeid_cast<const ColumnTuple *>(col_const->getDataColumnPtr().get()); \
    if (mask) \
        nd = mask->tupleSize(); \
    else \
        nd = col_const->getUInt(0); \
    auto non_const_arguments = arguments; \
    non_const_arguments[1].column = non_const_arguments[1].column->convertToFullColumnIfConst(); \
    const ColumnPtr & col_code = non_const_arguments[1].column; \
    Columns tuple_columns(nd); \
    EXTRACT_VECTOR(0) \
    if (nd == 1) \
    { \
        if (mask) \
        { \
            for (size_t i = 0; i < input_rows_count; i++) \
            { \
                vec0[i] = shrink(mask->getColumn(0).getUInt(0), col_code->getUInt(i)); \
            } \
            tuple_columns[0] = std::move(col0); \
        } \
        else \
        { \
            for (size_t i = 0; i < input_rows_count; i++) \
            { \
                vec0[i] = col_code->getUInt(i); \
            } \
            tuple_columns[0] = std::move(col0); \
        } \
        return ColumnTuple::create(tuple_columns); \
    } \
    EXTRACT_VECTOR(1) \
    DECODE(2, \
       vec0[i] = MASK(0, res); \
       vec1[i] = MASK(1, res);) \
    EXTRACT_VECTOR(2) \
    DECODE(3, \
       vec0[i] = MASK(0, res); \
       vec1[i] = MASK(1, res); \
       vec2[i] = MASK(2, res);) \
    EXTRACT_VECTOR(3) \
    DECODE(4, \
       vec0[i] = MASK(0, res); \
       vec1[i] = MASK(1, res); \
       vec2[i] = MASK(2, res); \
       vec3[i] = MASK(3, res);) \
    EXTRACT_VECTOR(4) \
    DECODE(5, \
       vec0[i] = MASK(0, res); \
       vec1[i] = MASK(1, res); \
       vec2[i] = MASK(2, res); \
       vec3[i] = MASK(3, res); \
       vec4[i] = MASK(4, res);) \
    EXTRACT_VECTOR(5) \
    DECODE(6, \
       vec0[i] = MASK(0, res); \
       vec1[i] = MASK(1, res); \
       vec2[i] = MASK(2, res); \
       vec3[i] = MASK(3, res); \
       vec4[i] = MASK(4, res); \
       vec5[i] = MASK(5, res);) \
    EXTRACT_VECTOR(6) \
    DECODE(7, \
       vec0[i] = MASK(0, res); \
       vec1[i] = MASK(1, res); \
       vec2[i] = MASK(2, res); \
       vec3[i] = MASK(3, res); \
       vec4[i] = MASK(4, res); \
       vec5[i] = MASK(5, res); \
       vec6[i] = MASK(6, res);) \
    EXTRACT_VECTOR(7) \
    DECODE(8, \
       vec0[i] = MASK(0, res); \
       vec1[i] = MASK(1, res); \
       vec2[i] = MASK(2, res); \
       vec3[i] = MASK(3, res); \
       vec4[i] = MASK(4, res); \
       vec5[i] = MASK(5, res); \
       vec6[i] = MASK(6, res); \
       vec7[i] = MASK(7, res);) \
    switch (nd) \
    { \
        case 2: \
            tuple_columns[0] = std::move(col0); \
            tuple_columns[1] = std::move(col1); \
            break; \
        case 3: \
            tuple_columns[0] = std::move(col0); \
            tuple_columns[1] = std::move(col1); \
            tuple_columns[2] = std::move(col2); \
            return ColumnTuple::create(tuple_columns); \
        case 4: \
            tuple_columns[0] = std::move(col0); \
            tuple_columns[1] = std::move(col1); \
            tuple_columns[2] = std::move(col2); \
            tuple_columns[3] = std::move(col3); \
            return ColumnTuple::create(tuple_columns); \
        case 5: \
            tuple_columns[0] = std::move(col0); \
            tuple_columns[1] = std::move(col1); \
            tuple_columns[2] = std::move(col2); \
            tuple_columns[3] = std::move(col3); \
            tuple_columns[4] = std::move(col4); \
            return ColumnTuple::create(tuple_columns); \
        case 6: \
            tuple_columns[0] = std::move(col0); \
            tuple_columns[1] = std::move(col1); \
            tuple_columns[2] = std::move(col2); \
            tuple_columns[3] = std::move(col3); \
            tuple_columns[4] = std::move(col4); \
            tuple_columns[5] = std::move(col5); \
            return ColumnTuple::create(tuple_columns); \
        case 7: \
            tuple_columns[0] = std::move(col0); \
            tuple_columns[1] = std::move(col1); \
            tuple_columns[2] = std::move(col2); \
            tuple_columns[3] = std::move(col3); \
            tuple_columns[4] = std::move(col4); \
            tuple_columns[5] = std::move(col5); \
            tuple_columns[6] = std::move(col6); \
            return ColumnTuple::create(tuple_columns); \
        case 8: \
            tuple_columns[0] = std::move(col0); \
            tuple_columns[1] = std::move(col1); \
            tuple_columns[2] = std::move(col2); \
            tuple_columns[3] = std::move(col3); \
            tuple_columns[4] = std::move(col4); \
            tuple_columns[5] = std::move(col5); \
            tuple_columns[6] = std::move(col6); \
            tuple_columns[7] = std::move(col7); \
            return ColumnTuple::create(tuple_columns); \
    } \
    return ColumnTuple::create(tuple_columns);

#if !defined(__BMI2__)

#define DECODE(ND, ...) \
        if (nd == (ND)) \
        { \
            for (size_t i = 0; i < input_rows_count; i++) \
            { \
                auto res = MortonND_##ND##D_Dec.Decode(col_code->getUInt(i)); \
                __VA_ARGS__ \
            } \
        }

namespace morton_compress
{

/// The bits of a morton code that belong to field 0 of an ND-dimensional code. Every bit
/// position congruent to 0 mod ND counts, including positions at or above ND*FieldBits:
/// MortonNDLutDecoder folds those into the low fields too, so stopping at FieldBits would
/// decode codes with the top bits set differently from the decoder this one stands in for.
constexpr UInt64 fieldMask(size_t dimensions)
{
    UInt64 m = 0;
    for (size_t i = 0; i * dimensions < 64; ++i)
        m |= UInt64(1) << (i * dimensions);
    return m;
}

/// Per-step "bits to move" masks of a parallel-suffix bit compress over fieldMask().
/// A zero mask marks a step that moves nothing: the bit at position i*ND travels i*(ND-1)
/// places, so which steps drop out follows ND-1 and the longest travel, not the field width.
constexpr std::array<UInt64, 6> moveMasks(size_t dimensions)
{
    std::array<UInt64, 6> mv{};
    UInt64 m = fieldMask(dimensions);
    UInt64 mk = ~m << 1;
    for (size_t i = 0; i < 6; ++i)
    {
        UInt64 mp = mk ^ (mk << 1);
        mp ^= mp << 2;
        mp ^= mp << 4;
        mp ^= mp << 8;
        mp ^= mp << 16;
        mp ^= mp << 32;
        mv[i] = mp & m;
        m = (m ^ mv[i]) | (mv[i] >> (1u << i));
        mk = mk & ~mp;
    }
    return mv;
}

}

/// Gathers one field of a morton code with shifts and masks instead of table lookups: the
/// "sheep and goats" compress of Hacker's Delight 7-4, with every mask folded by constexpr.
template <size_t Dimensions>
struct MortonNDCompressDecoder
{
    static constexpr UInt64 field_mask = morton_compress::fieldMask(Dimensions);
    static constexpr std::array<UInt64, 6> move_masks = morton_compress::moveMasks(Dimensions);

    template <size_t Step>
    static constexpr UInt64 compressStep(UInt64 x)
    {
        if constexpr (move_masks[Step] == 0)
            return x;
        else
        {
            const UInt64 t = x & move_masks[Step];
            return (x ^ t) | (t >> (1u << Step));
        }
    }

    static constexpr UInt64 extractField(UInt64 code)
    {
        UInt64 x = code & field_mask;
        x = compressStep<0>(x);
        x = compressStep<1>(x);
        x = compressStep<2>(x);
        x = compressStep<3>(x);
        x = compressStep<4>(x);
        x = compressStep<5>(x);
        return x;
    }

    template <size_t... I>
    static constexpr auto decodeImpl(UInt64 code, std::index_sequence<I...>)
    {
        return std::make_tuple(extractField(code >> I)...);
    }

    constexpr auto Decode(UInt64 code) const { return decodeImpl(code, std::make_index_sequence<Dimensions>{}); }
};

/// Field 0 spans every bit position congruent to 0 mod ND up to bit 63, not just the low
/// FieldBits of them. These are the dimensions where the two spans differ, and where a
/// shorter mask decodes a code with the top bit set into the wrong field width.
static_assert(std::get<0>(MortonNDCompressDecoder<3>().Decode(0x8000000000000000ULL)) == 2097152);
static_assert(std::get<0>(MortonNDCompressDecoder<5>().Decode(0x1000000000000000ULL)) == 4096);
static_assert(std::get<0>(MortonNDCompressDecoder<6>().Decode(0x1000000000000000ULL)) == 1024);
static_assert(std::get<0>(MortonNDCompressDecoder<7>().Decode(0x8000000000000000ULL)) == 512);

/// AArch64 has no pdep/pext, so it reaches this arm rather than the BMI2 one below. The
/// compress decoder wins there at every dimension except 4 and 8, where the lookup table's
/// one shared load amortises over enough output fields to stay ahead.
template <size_t Dimensions>
constexpr bool use_compress_decoder =
#if defined(__aarch64__)
    Dimensions != 4 && Dimensions != 8;
#else
    false;
#endif

template <size_t Dimensions, size_t FieldBits>
constexpr auto makeMortonDecoder()
{
    if constexpr (use_compress_decoder<Dimensions>)
        return MortonNDCompressDecoder<Dimensions>();
    else
        return mortonnd::MortonNDLutDecoder<Dimensions, FieldBits, 8>();
}

constexpr auto MortonND_2D_Dec = makeMortonDecoder<2, 32>();
constexpr auto MortonND_3D_Dec = makeMortonDecoder<3, 21>();
constexpr auto MortonND_4D_Dec = makeMortonDecoder<4, 16>();
constexpr auto MortonND_5D_Dec = makeMortonDecoder<5, 12>();
constexpr auto MortonND_6D_Dec = makeMortonDecoder<6, 10>();
constexpr auto MortonND_7D_Dec = makeMortonDecoder<7, 9>();
constexpr auto MortonND_8D_Dec = makeMortonDecoder<8, 8>();
class FunctionMortonDecode : public FunctionSpaceFillingCurveDecode<8, 1, 8>
{
public:
    static constexpr auto name = "mortonDecode";
    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionMortonDecode>();
    }

    String getName() const override
    {
        return name;
    }

    static UInt64 shrink(UInt64 ratio, UInt64 value)
    {
        switch (ratio) // NOLINT(bugprone-switch-missing-default-case)
        {
            case 1:
                return value;
            case 2:
                return std::get<1>(MortonND_2D_Dec.Decode(value));
            case 3:
                return std::get<2>(MortonND_3D_Dec.Decode(value));
            case 4:
                return std::get<3>(MortonND_4D_Dec.Decode(value));
            case 5:
                return std::get<4>(MortonND_5D_Dec.Decode(value));
            case 6:
                return std::get<5>(MortonND_6D_Dec.Decode(value));
            case 7:
                return std::get<6>(MortonND_7D_Dec.Decode(value));
            case 8:
                return std::get<7>(MortonND_8D_Dec.Decode(value));
        }
        return value;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        EXECUTE()
    }
};

#else

#define DECODE(ND, ...) \
        if (nd == (ND)) \
        { \
            for (size_t i = 0; i < input_rows_count; i++) \
            { \
                auto res = MortonND_##ND##D::Decode(col_code->getUInt(i)); \
                __VA_ARGS__ \
            } \
        }

using MortonND_2D = mortonnd::MortonNDBmi<2, uint64_t>;
using MortonND_3D = mortonnd::MortonNDBmi<3, uint64_t>;
using MortonND_4D = mortonnd::MortonNDBmi<4, uint64_t>;
using MortonND_5D = mortonnd::MortonNDBmi<5, uint64_t>;
using MortonND_6D = mortonnd::MortonNDBmi<6, uint64_t>;
using MortonND_7D = mortonnd::MortonNDBmi<7, uint64_t>;
using MortonND_8D = mortonnd::MortonNDBmi<8, uint64_t>;
class FunctionMortonDecode : public FunctionSpaceFillingCurveDecode<8, 1, 8>
{
public:
    static constexpr auto name = "mortonDecode";
    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionMortonDecode>();
    }

    String getName() const override
    {
        return name;
    }

    static UInt64 shrink(UInt64 ratio, UInt64 value)
    {
        switch (ratio)
        {
            case 1:
                return value;
            case 2:
                return std::get<1>(MortonND_2D::Decode(value));
            case 3:
                return std::get<2>(MortonND_3D::Decode(value));
            case 4:
                return std::get<3>(MortonND_4D::Decode(value));
            case 5:
                return std::get<4>(MortonND_5D::Decode(value));
            case 6:
                return std::get<5>(MortonND_6D::Decode(value));
            case 7:
                return std::get<6>(MortonND_7D::Decode(value));
            case 8:
                return std::get<7>(MortonND_8D::Decode(value));
        }
        return value;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        EXECUTE()
    }
};

#endif

#undef DECODE
#undef MASK
#undef EXTRACT_VECTOR
#undef EXECUTE

// NOLINTEND(bugprone-switch-missing-default-case)

REGISTER_FUNCTION(MortonDecode)
{
    FunctionDocumentation::Description description = R"(
Decodes a Morton encoding (ZCurve) into the corresponding unsigned integer tuple.

As with the `mortonEncode` function, this function has two modes of operation:
- **Simple**
- **Expanded**

**Simple mode**

Accepts a resulting tuple size as the first argument and the code as the second argument.

**Expanded mode**

Accepts a range mask (tuple) as the first argument and the code as the second argument.
Each number in the mask configures the amount of range shrink:

* `1` - no shrink
* `2` - 2x shrink
* `3` - 3x shrink
⋮
* Up to 8x shrink.

Range expansion can be beneficial when you need a similar distribution for
arguments with wildly different ranges (or cardinality). For example: 'IP Address' `(0...FFFFFFFF)`
and 'Country code' `(0...FF)`. As with the encode function, this is limited to
8 numbers at most.
    )";
    FunctionDocumentation::Syntax syntax = R"(
-- Simple mode
mortonDecode(tuple_size, code)

-- Expanded mode
mortonDecode(range_mask, code)
)";
    FunctionDocumentation::Arguments arguments = {
        {"tuple_size", "Integer value no more than 8.", {"UInt8/16/32/64"}},
        {"range_mask", "For the expanded mode, the mask for each argument. The mask is a tuple of unsigned integers. Each number in the mask configures the amount of range shrink.", {"Tuple(UInt8/16/32/64)"}},
        {"code", "UInt64 code.", {"UInt64"}},
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a tuple of the specified size.", {"Tuple(UInt64)"}};
    FunctionDocumentation::Examples examples = {
        {"Simple mode", "SELECT mortonDecode(3, 53)", R"(["1", "2", "3"])"},
        {"Single argument", "SELECT mortonDecode(1, 1)", R"(["1"])"},
        {"Expanded mode, shrinking one argument", R"(SELECT mortonDecode(tuple(2), 32768))", R"(["128"])"},
        {"Column usage",
         R"(
-- First create the table and insert some data
CREATE TABLE morton_numbers(
    n1 UInt32,
    n2 UInt32,
    n3 UInt16,
    n4 UInt16,
    n5 UInt8,
    n6 UInt8,
    n7 UInt8,
    n8 UInt8
)
ENGINE=MergeTree()
ORDER BY n1;
INSERT INTO morton_numbers (*) values(1, 2, 3, 4, 5, 6, 7, 8);

-- Use column names instead of constants as function arguments
SELECT untuple(mortonDecode(8, mortonEncode(n1, n2, n3, n4, n5, n6, n7, n8))) FROM morton_numbers;
         )",
         "1 2 3 4 5 6 7 8"
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {24, 6};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Encoding;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionMortonDecode>(documentation);
}

}
