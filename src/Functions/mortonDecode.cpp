#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionSpaceFillingCurve.h>
#include <Functions/IFunction.h>
#include <Functions/PerformanceAdaptors.h>

#include <morton-nd/mortonND_LUT.h>
#if USE_MULTITARGET_CODE && defined(__BMI2__)
#include <morton-nd/mortonND_BMI2.h>
#endif

namespace DB
{

// NOLINTBEGIN(bugprone-switch-missing-default-case)

#define EXTRACT_VECTOR(INDEX) \
        auto col##INDEX = ColumnUInt64::create(); \
        auto & vec##INDEX = col##INDEX->getData(); \
        vec##INDEX.resize(input_rows_count);

#define DECODE(ND, ...) \
        if (nd == (ND)) \
        { \
            for (size_t i = 0; i < input_rows_count; i++) \
            { \
                auto res = MortonND_##ND##D_Dec.Decode(col_code->getUInt(i)); \
                __VA_ARGS__ \
            } \
        }

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

DECLARE_DEFAULT_CODE(
constexpr auto MortonND_2D_Dec = mortonnd::MortonNDLutDecoder<2, 32, 8>();
constexpr auto MortonND_3D_Dec = mortonnd::MortonNDLutDecoder<3, 21, 8>();
constexpr auto MortonND_4D_Dec = mortonnd::MortonNDLutDecoder<4, 16, 8>();
constexpr auto MortonND_5D_Dec = mortonnd::MortonNDLutDecoder<5, 12, 8>();
constexpr auto MortonND_6D_Dec = mortonnd::MortonNDLutDecoder<6, 10, 8>();
constexpr auto MortonND_7D_Dec = mortonnd::MortonNDLutDecoder<7, 9, 8>();
constexpr auto MortonND_8D_Dec = mortonnd::MortonNDLutDecoder<8, 8, 8>();
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
) // DECLARE_DEFAULT_CODE

#if defined(MORTON_ND_BMI2_ENABLED)
#undef DECODE
#define DECODE(ND, ...) \
        if (nd == (ND)) \
        { \
            for (size_t i = 0; i < input_rows_count; i++) \
            { \
                auto res = MortonND_##ND##D::Decode(col_code->getUInt(i)); \
                __VA_ARGS__ \
            } \
        }

DECLARE_AVX2_SPECIFIC_CODE(
using MortonND_2D = mortonnd::MortonNDBmi<2, uint64_t>;
using MortonND_3D = mortonnd::MortonNDBmi<3, uint64_t>;
using MortonND_4D = mortonnd::MortonNDBmi<4, uint64_t>;
using MortonND_5D = mortonnd::MortonNDBmi<5, uint64_t>;
using MortonND_6D = mortonnd::MortonNDBmi<6, uint64_t>;
using MortonND_7D = mortonnd::MortonNDBmi<7, uint64_t>;
using MortonND_8D = mortonnd::MortonNDBmi<8, uint64_t>;
class FunctionMortonDecode: public TargetSpecific::Default::FunctionMortonDecode
{
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
)
#endif // MORTON_ND_BMI2_ENABLED

#undef DECODE
#undef MASK
#undef EXTRACT_VECTOR
#undef EXECUTE

class FunctionMortonDecode: public TargetSpecific::Default::FunctionMortonDecode
{
public:
    explicit FunctionMortonDecode(ContextPtr context) : selector(context)
    {
        selector.registerImplementation<TargetArch::Default,
                                        TargetSpecific::Default::FunctionMortonDecode>();

#if USE_MULTITARGET_CODE && defined(MORTON_ND_BMI2_ENABLED)
        selector.registerImplementation<TargetArch::AVX2,
                                        TargetSpecific::AVX2::FunctionMortonDecode>();
#endif
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        return selector.selectAndExecute(arguments, result_type, input_rows_count);
    }

    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionMortonDecode>(context);
    }

private:
    ImplementationSelector<IFunction> selector;
};

// NOLINTEND(bugprone-switch-missing-default-case)

REGISTER_FUNCTION(MortonDecode)
{
    factory.registerFunction<FunctionMortonDecode>(FunctionDocumentation{
        .description=R"(
Decodes a Morton encoding (ZCurve) into the corresponding unsigned integer tuple

The function has two modes of operation:
- Simple
- Expanded

Simple: accepts a resulting tuple size as a first argument and the code as a second argument.
[example:simple]
Will decode into: `(1,2,3,4)`
The resulting tuple size cannot be more than 8

Expanded: accepts a range mask (tuple) as a first argument and the code as a second argument.
Each number in mask configures the amount of range shrink
1 - no shrink
2 - 2x shrink
3 - 3x shrink
....
Up to 8x shrink.
[example:range_shrank]
Note: see mortonEncode() docs on why range change might be beneficial.
Still limited to 8 numbers at most.

Morton code for one argument is always the argument itself (as a tuple).
[example:identity]
Produces: `(1)`

You can shrink one argument too:
[example:identity_shrank]
Produces: `(128)`

The function accepts a column of codes as a second argument:
[example:from_table]

The range tuple must be a constant:
[example:from_table_range]
)",
        .examples{
            {"simple", "SELECT mortonDecode(4, 2149)", ""},
            {"range_shrank", "SELECT mortonDecode((1,2), 1572864)", ""},
            {"identity", "SELECT mortonDecode(1, 1)", ""},
            {"identity_shrank", "SELECT mortonDecode(tuple(2), 32768)", ""},
            {"from_table", "SELECT mortonDecode(2, code) FROM table", ""},
            {"from_table_range", "SELECT mortonDecode((1,2), code) FROM table", ""},
            },
        .categories {"ZCurve", "Morton coding"}
    });
}

}
