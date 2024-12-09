#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnTuple.h>
#include <Functions/FunctionSpaceFillingCurve.h>
#include <Functions/PerformanceAdaptors.h>

#include <morton-nd/mortonND_LUT.h>
#if USE_MULTITARGET_CODE && defined(__BMI2__)
#include <morton-nd/mortonND_BMI2.h>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int TOO_MANY_ARGUMENTS_FOR_FUNCTION;
}

#define EXTRACT_VECTOR(INDEX) \
    const ColumnPtr & col##INDEX = non_const_arguments[(INDEX) + vectorStartIndex].column;

#define ENCODE(ND, ...) \
    if (nd == (ND)) \
    { \
        for (size_t i = 0; i < input_rows_count; i++) \
        {               \
            vec_res[i] = MortonND_##ND##D_Enc.Encode(__VA_ARGS__); \
        } \
        return col_res; \
    }

#define EXPAND(IDX, ...) \
    (mask) ? expand(mask->getColumn(IDX).getUInt(0), __VA_ARGS__) : __VA_ARGS__

#define MASK(ND, IDX, ...) \
    (EXPAND(IDX, __VA_ARGS__) & MortonND_##ND##D_Enc.InputMask())

#define EXECUTE() \
    size_t nd = arguments.size(); \
    size_t vectorStartIndex = 0; \
    const auto * const_col = typeid_cast<const ColumnConst *>(arguments[0].column.get()); \
    const ColumnTuple * mask; \
    if (const_col) \
        mask = typeid_cast<const ColumnTuple *>(const_col->getDataColumnPtr().get()); \
    else \
        mask = typeid_cast<const ColumnTuple *>(arguments[0].column.get()); \
    if (mask) \
    { \
        nd = mask->tupleSize(); \
        vectorStartIndex = 1; \
        for (size_t i = 0; i < nd; i++) \
        { \
            auto ratio = mask->getColumn(i).getUInt(0); \
            if (ratio > 8 || ratio < 1) \
                throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, \
                                "Illegal argument {} of function {}, should be a number in range 1-8", \
                                arguments[0].column->getName(), getName()); \
        } \
    } \
     \
    auto non_const_arguments = arguments; \
    for (auto & argument : non_const_arguments) \
        argument.column = argument.column->convertToFullColumnIfConst(); \
     \
    auto col_res = ColumnUInt64::create(); \
    ColumnUInt64::Container & vec_res = col_res->getData(); \
    vec_res.resize(input_rows_count); \
     \
    EXTRACT_VECTOR(0) \
    if (nd == 1) \
    { \
        for (size_t i = 0; i < input_rows_count; i++) \
        { \
            vec_res[i] = EXPAND(0, col0->getUInt(i)); \
        } \
        return col_res; \
    } \
     \
    EXTRACT_VECTOR(1) \
    ENCODE(2, \
           MASK(2, 0, col0->getUInt(i)), \
           MASK(2, 1, col1->getUInt(i))) \
    EXTRACT_VECTOR(2) \
    ENCODE(3, \
           MASK(3, 0, col0->getUInt(i)), \
           MASK(3, 1, col1->getUInt(i)), \
           MASK(3, 2, col2->getUInt(i))) \
    EXTRACT_VECTOR(3) \
    ENCODE(4, \
           MASK(4, 0, col0->getUInt(i)), \
           MASK(4, 1, col1->getUInt(i)), \
           MASK(4, 2, col2->getUInt(i)), \
           MASK(4, 3, col3->getUInt(i))) \
    EXTRACT_VECTOR(4) \
    ENCODE(5, \
           MASK(5, 0, col0->getUInt(i)), \
           MASK(5, 1, col1->getUInt(i)), \
           MASK(5, 2, col2->getUInt(i)), \
           MASK(5, 3, col3->getUInt(i)), \
           MASK(5, 4, col4->getUInt(i))) \
    EXTRACT_VECTOR(5) \
    ENCODE(6, \
           MASK(6, 0, col0->getUInt(i)), \
           MASK(6, 1, col1->getUInt(i)), \
           MASK(6, 2, col2->getUInt(i)), \
           MASK(6, 3, col3->getUInt(i)), \
           MASK(6, 4, col4->getUInt(i)), \
           MASK(6, 5, col5->getUInt(i))) \
    EXTRACT_VECTOR(6) \
    ENCODE(7, \
           MASK(7, 0, col0->getUInt(i)), \
           MASK(7, 1, col1->getUInt(i)), \
           MASK(7, 2, col2->getUInt(i)), \
           MASK(7, 3, col3->getUInt(i)), \
           MASK(7, 4, col4->getUInt(i)), \
           MASK(7, 5, col5->getUInt(i)), \
           MASK(7, 6, col6->getUInt(i))) \
    EXTRACT_VECTOR(7) \
    ENCODE(8, \
           MASK(8, 0, col0->getUInt(i)), \
           MASK(8, 1, col1->getUInt(i)), \
           MASK(8, 2, col2->getUInt(i)), \
           MASK(8, 3, col3->getUInt(i)), \
           MASK(8, 4, col4->getUInt(i)), \
           MASK(8, 5, col5->getUInt(i)), \
           MASK(8, 6, col6->getUInt(i)), \
           MASK(8, 7, col7->getUInt(i))) \
     \
    throw Exception(ErrorCodes::TOO_MANY_ARGUMENTS_FOR_FUNCTION, \
                    "Illegal number of UInt arguments of function {}, max: 8", \
                    getName()); \

DECLARE_DEFAULT_CODE(
constexpr auto MortonND_2D_Enc = mortonnd::MortonNDLutEncoder<2, 32, 8>();
constexpr auto MortonND_3D_Enc = mortonnd::MortonNDLutEncoder<3, 21, 8>();
constexpr auto MortonND_4D_Enc = mortonnd::MortonNDLutEncoder<4, 16, 8>();
constexpr auto MortonND_5D_Enc = mortonnd::MortonNDLutEncoder<5, 12, 8>();
constexpr auto MortonND_6D_Enc = mortonnd::MortonNDLutEncoder<6, 10, 8>();
constexpr auto MortonND_7D_Enc = mortonnd::MortonNDLutEncoder<7, 9, 8>();
constexpr auto MortonND_8D_Enc = mortonnd::MortonNDLutEncoder<8, 8, 8>();
class FunctionMortonEncode : public FunctionSpaceFillingCurveEncode
{
public:
    static constexpr auto name = "mortonEncode";
    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionMortonEncode>();
    }

    String getName() const override
    {
        return name;
    }

    static UInt64 expand(UInt64 ratio, UInt64 value)
    {
        switch (ratio) // NOLINT(bugprone-switch-missing-default-case)
        {
            case 1:
                return value;
            case 2:
                return MortonND_2D_Enc.Encode(0, value & MortonND_2D_Enc.InputMask());
            case 3:
                return MortonND_3D_Enc.Encode(0, 0, value & MortonND_3D_Enc.InputMask());
            case 4:
                return MortonND_4D_Enc.Encode(0, 0, 0, value & MortonND_4D_Enc.InputMask());
            case 5:
                return MortonND_5D_Enc.Encode(0, 0, 0, 0, value & MortonND_5D_Enc.InputMask());
            case 6:
                return MortonND_6D_Enc.Encode(0, 0, 0, 0, 0, value & MortonND_6D_Enc.InputMask());
            case 7:
                return MortonND_7D_Enc.Encode(0, 0, 0, 0, 0, 0, value & MortonND_7D_Enc.InputMask());
            case 8:
                return MortonND_8D_Enc.Encode(0, 0, 0, 0, 0, 0, 0, value & MortonND_8D_Enc.InputMask());
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
#undef ENCODE
#define ENCODE(ND, ...) \
    if (nd == (ND)) \
    { \
        for (size_t i = 0; i < input_rows_count; i++) \
        {               \
            vec_res[i] = MortonND_##ND##D::Encode(__VA_ARGS__); \
        } \
        return col_res; \
    }

#undef MASK
#define MASK(ND, IDX, ...) \
    (EXPAND(IDX, __VA_ARGS__))

DECLARE_AVX2_SPECIFIC_CODE(
using MortonND_2D = mortonnd::MortonNDBmi<2, uint64_t>;
using MortonND_3D = mortonnd::MortonNDBmi<3, uint64_t>;
using MortonND_4D = mortonnd::MortonNDBmi<4, uint64_t>;
using MortonND_5D = mortonnd::MortonNDBmi<5, uint64_t>;
using MortonND_6D = mortonnd::MortonNDBmi<6, uint64_t>;
using MortonND_7D = mortonnd::MortonNDBmi<7, uint64_t>;
using MortonND_8D = mortonnd::MortonNDBmi<8, uint64_t>;

class FunctionMortonEncode : public TargetSpecific::Default::FunctionMortonEncode
{
public:
    static UInt64 expand(UInt64 ratio, UInt64 value)
    {
        switch (ratio)
        {
            case 1:
                return value;
            case 2:
                return MortonND_2D::Encode(0, value);
            case 3:
                return MortonND_3D::Encode(0, 0, value);
            case 4:
                return MortonND_4D::Encode(0, 0, 0, value);
            case 5:
                return MortonND_5D::Encode(0, 0, 0, 0, value);
            case 6:
                return MortonND_6D::Encode(0, 0, 0, 0, 0, value);
            case 7:
                return MortonND_7D::Encode(0, 0, 0, 0, 0, 0, value);
            case 8:
                return MortonND_8D::Encode(0, 0, 0, 0, 0, 0, 0, value);
        }
        return value;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        EXECUTE()
    }
};
) // DECLARE_AVX2_SPECIFIC_CODE
#endif // MORTON_ND_BMI2_ENABLED

#undef ENCODE
#undef MASK
#undef EXTRACT_VECTOR
#undef EXPAND
#undef EXECUTE

class FunctionMortonEncode: public TargetSpecific::Default::FunctionMortonEncode
{
public:
    explicit FunctionMortonEncode(ContextPtr context) : selector(context)
    {
        selector.registerImplementation<TargetArch::Default,
                                        TargetSpecific::Default::FunctionMortonEncode>();

#if USE_MULTITARGET_CODE && defined(MORTON_ND_BMI2_ENABLED)
        selector.registerImplementation<TargetArch::AVX2,
                                        TargetSpecific::AVX2::FunctionMortonEncode>();
#endif
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        if (input_rows_count == 0)
            return ColumnUInt64::create();

        return selector.selectAndExecute(arguments, result_type, input_rows_count);
    }

    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionMortonEncode>(context);
    }

private:
    ImplementationSelector<IFunction> selector;
};

REGISTER_FUNCTION(MortonEncode)
{
    factory.registerFunction<FunctionMortonEncode>(FunctionDocumentation{
    .description=R"(
Calculates Morton encoding (ZCurve) for a list of unsigned integers

The function has two modes of operation:
- Simple
- Expanded

Simple: accepts up to 8 unsigned integers as arguments and produces a UInt64 code.
[example:simple]

Expanded: accepts a range mask (tuple) as a first argument and up to 8 unsigned integers as other arguments.
Each number in mask configures the amount of range expansion
1 - no expansion
2 - 2x expansion
3 - 3x expansion
....
Up to 8x expansion.
[example:range_expanded]
Note: tuple size must be equal to the number of the other arguments

Range expansion can be beneficial when you need a similar distribution for arguments with wildly different ranges (or cardinality)
For example: 'IP Address' (0...FFFFFFFF) and 'Country code' (0...FF)

Morton encoding for one argument is always the argument itself.
[example:identity]
Produces: `1`

You can expand one argument too:
[example:identity_expanded]
Produces: `32768`

The function also accepts columns as arguments:
[example:from_table]

But the range tuple must still be a constant:
[example:from_table_range]

Please note that you can fit only so much bits of information into Morton code as UInt64 has.
Two arguments will have a range of maximum 2^32 (64/2) each
Three arguments: range of max 2^21 (64/3) each
And so on, all overflow will be clamped to zero
)",
        .examples{
            {"simple", "SELECT mortonEncode(1, 2, 3)", ""},
            {"range_expanded", "SELECT mortonEncode((1,2), 1024, 16)", ""},
            {"identity", "SELECT mortonEncode(1)", ""},
            {"identity_expanded", "SELECT mortonEncode(tuple(2), 128)", ""},
            {"from_table", "SELECT mortonEncode(n1, n2) FROM table", ""},
            {"from_table_range", "SELECT mortonEncode((1,2), n1, n2) FROM table", ""},
            },
        .categories {"ZCurve", "Morton coding"}
    });
}

}
