#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnQBit.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeQBit.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/SerializationQBit.h>
#include <Common/FunctionDocumentation.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/assert_cast.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cmath>
#include <cstring>

/** quantizeBFloat16ToInt8 / dequantizeInt8ToBFloat16
  *
  * A scalar codec pair for compressing embedding components with a 256-level Gaussian
  * Lloyd-Max quantizer (the MSE-optimal scalar quantizer for a standard-normal source).
  *
  * It is intended for values that are approximately N(0, 1). A random orthogonal (randomized
  * Hadamard) rotation preserves the norm, so for a d-dimensional unit-norm embedding each rotated
  * coordinate has variance 1/d; multiply the rotated vector by sqrt(d) to reach unit variance
  * before quantizing. Apply over a vector with arrayMap, e.g.
  *     arrayMap(x -> quantizeBFloat16ToInt8(x), rotated_embedding)  -- rotated_embedding ~ N(0, 1)
  *
  * quantizeBFloat16ToInt8 maps a value to the index (0..255) of the Lloyd-Max cell it falls
  * into and stores it as Int8 (index - 128), so the sign bit equals the sign of the value (+0 and
  * -0 map to the positive and negative central cells respectively) and the top b bits form a valid
  * 2^b-level (embedded) quantizer -- truncating bits yields coarser Int4/Int2/binary codes.
  * dequantizeInt8ToBFloat16 maps the code back to the cell's reconstruction level.
  */
namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

/// 256-level Gaussian Lloyd-Max quantizer (for ~N(0,1) input).
/// Reconstruction levels (sorted ascending; symmetric; index 0..255).
// NOLINTBEGIN(modernize-use-std-numbers): these are quantizer table values that happen to lie near
// math constants such as 1/sqrt(3) and ln(2); they are data, not approximations of those constants.
constexpr Float32 LLOYD_MAX_LEVELS[256] = {
    -3.94331723f, -3.46399932f, -3.16034096f, -2.93314607f, -2.74992207f, -2.59575919f, -2.46251620f, -2.34523372f,
    -2.24065008f, -2.14649281f, -2.06110438f, -1.98322915f, -1.91188474f, -1.84628084f, -1.78576605f, -1.72979202f,
    -1.67788877f, -1.62964731f, -1.58470730f, -1.54274811f, -1.50348227f, -1.46665067f, -1.43201890f, -1.39937446f,
    -1.36852460f, -1.33929452f, -1.31152588f, -1.28507552f, -1.25981428f, -1.23562592f, -1.21240610f, -1.19006145f,
    -1.16850859f, -1.14767324f, -1.12748941f, -1.10789853f, -1.08884872f, -1.07029404f, -1.05219386f, -1.03451219f,
    -1.01721718f, -1.00028055f, -0.98367719f, -0.96738473f, -0.95138317f, -0.93565460f, -0.92018290f, -0.90495349f,
    -0.88995319f, -0.87516997f, -0.86059283f, -0.84621168f, -0.83201723f, -0.81800086f, -0.80415460f, -0.79047101f,
    -0.77694313f, -0.76356447f, -0.75032892f, -0.73723071f, -0.72426444f, -0.71142496f, -0.69870743f, -0.68610723f,
    -0.67362001f, -0.66124158f, -0.64896799f, -0.63679545f, -0.62472037f, -0.61273927f, -0.60084886f, -0.58904597f,
    -0.57732756f, -0.56569073f, -0.55413266f, -0.54265067f, -0.53124215f, -0.51990462f, -0.50863566f, -0.49743295f,
    -0.48629424f, -0.47521736f, -0.46420020f, -0.45324075f, -0.44233703f, -0.43148712f, -0.42068918f, -0.40994142f,
    -0.39924207f, -0.38858944f, -0.37798188f, -0.36741778f, -0.35689557f, -0.34641372f, -0.33597074f, -0.32556517f,
    -0.31519559f, -0.30486060f, -0.29455885f, -0.28428900f, -0.27404974f, -0.26383980f, -0.25365792f, -0.24350287f,
    -0.23337343f, -0.22326841f, -0.21318664f, -0.20312698f, -0.19308828f, -0.18306942f, -0.17306929f, -0.16308682f,
    -0.15312092f, -0.14317053f, -0.13323459f, -0.12331206f, -0.11340192f, -0.10350313f, -0.09361469f, -0.08373558f,
    -0.07386480f, -0.06400137f, -0.05414428f, -0.04429256f, -0.03444523f, -0.02460130f, -0.01475981f, -0.00491977f,
    0.00491977f, 0.01475981f, 0.02460130f, 0.03444523f, 0.04429256f, 0.05414428f, 0.06400137f, 0.07386480f,
    0.08373558f, 0.09361469f, 0.10350313f, 0.11340192f, 0.12331206f, 0.13323459f, 0.14317053f, 0.15312092f,
    0.16308682f, 0.17306929f, 0.18306942f, 0.19308828f, 0.20312698f, 0.21318664f, 0.22326841f, 0.23337343f,
    0.24350287f, 0.25365792f, 0.26383980f, 0.27404974f, 0.28428900f, 0.29455885f, 0.30486060f, 0.31519559f,
    0.32556517f, 0.33597074f, 0.34641372f, 0.35689557f, 0.36741778f, 0.37798188f, 0.38858944f, 0.39924207f,
    0.40994142f, 0.42068918f, 0.43148712f, 0.44233703f, 0.45324075f, 0.46420020f, 0.47521736f, 0.48629424f,
    0.49743295f, 0.50863566f, 0.51990462f, 0.53124215f, 0.54265067f, 0.55413266f, 0.56569073f, 0.57732756f,
    0.58904597f, 0.60084886f, 0.61273927f, 0.62472037f, 0.63679545f, 0.64896799f, 0.66124158f, 0.67362001f,
    0.68610723f, 0.69870743f, 0.71142496f, 0.72426444f, 0.73723071f, 0.75032892f, 0.76356447f, 0.77694313f,
    0.79047101f, 0.80415460f, 0.81800086f, 0.83201723f, 0.84621168f, 0.86059283f, 0.87516997f, 0.88995319f,
    0.90495349f, 0.92018290f, 0.93565460f, 0.95138317f, 0.96738473f, 0.98367719f, 1.00028055f, 1.01721718f,
    1.03451219f, 1.05219386f, 1.07029404f, 1.08884872f, 1.10789853f, 1.12748941f, 1.14767324f, 1.16850859f,
    1.19006145f, 1.21240610f, 1.23562592f, 1.25981428f, 1.28507552f, 1.31152588f, 1.33929452f, 1.36852460f,
    1.39937446f, 1.43201890f, 1.46665067f, 1.50348227f, 1.54274811f, 1.58470730f, 1.62964731f, 1.67788877f,
    1.72979202f, 1.78576605f, 1.84628084f, 1.91188474f, 1.98322915f, 2.06110438f, 2.14649281f, 2.24065008f,
    2.34523372f, 2.46251620f, 2.59575919f, 2.74992207f, 2.93314607f, 3.16034096f, 3.46399932f, 3.94331723f,
};

/// 255 internal decision boundaries (sorted). The cell index of x is the number of boundaries < x.
constexpr Float32 LLOYD_MAX_BOUNDARIES[255] = {
    -3.70365827f, -3.31217014f, -3.04674351f, -2.84153407f, -2.67284063f, -2.52913769f, -2.40387496f, -2.29294190f,
    -2.19357144f, -2.10379860f, -2.02216677f, -1.94755695f, -1.87908279f, -1.81602345f, -1.75777904f, -1.70384040f,
    -1.65376804f, -1.60717730f, -1.56372770f, -1.52311519f, -1.48506647f, -1.44933479f, -1.41569668f, -1.38394953f,
    -1.35390956f, -1.32541020f, -1.29830070f, -1.27244490f, -1.24772010f, -1.22401601f, -1.20123378f, -1.17928502f,
    -1.15809091f, -1.13758133f, -1.11769397f, -1.09837363f, -1.07957138f, -1.06124395f, -1.04335303f, -1.02586469f,
    -1.00874887f, -0.99197887f, -0.97553096f, -0.95938395f, -0.94351889f, -0.92791875f, -0.91256819f, -0.89745334f,
    -0.88256158f, -0.86788140f, -0.85340225f, -0.83911445f, -0.82500904f, -0.81107773f, -0.79731280f, -0.78370707f,
    -0.77025380f, -0.75694669f, -0.74377981f, -0.73074757f, -0.71784470f, -0.70506619f, -0.69240733f, -0.67986362f,
    -0.66743079f, -0.65510478f, -0.64288172f, -0.63075791f, -0.61872982f, -0.60679406f, -0.59494741f, -0.58318677f,
    -0.57150915f, -0.55991170f, -0.54839166f, -0.53694641f, -0.52557339f, -0.51427014f, -0.50303431f, -0.49186359f,
    -0.48075580f, -0.46970878f, -0.45872048f, -0.44778889f, -0.43691208f, -0.42608815f, -0.41531530f, -0.40459174f,
    -0.39391575f, -0.38328566f, -0.37269983f, -0.36215668f, -0.35165464f, -0.34119223f, -0.33076795f, -0.32038038f,
    -0.31002809f, -0.29970972f, -0.28942392f, -0.27916937f, -0.26894477f, -0.25874886f, -0.24858040f, -0.23843815f,
    -0.22832092f, -0.21822753f, -0.20815681f, -0.19810763f, -0.18807885f, -0.17806935f, -0.16807806f, -0.15810387f,
    -0.14814572f, -0.13820256f, -0.12827333f, -0.11835699f, -0.10845253f, -0.09855891f, -0.08867513f, -0.07880019f,
    -0.06893308f, -0.05907282f, -0.04921842f, -0.03936890f, -0.02952327f, -0.01968056f, -0.00983979f, 0.00000000f,
    0.00983979f, 0.01968056f, 0.02952327f, 0.03936890f, 0.04921842f, 0.05907282f, 0.06893308f, 0.07880019f,
    0.08867513f, 0.09855891f, 0.10845253f, 0.11835699f, 0.12827333f, 0.13820256f, 0.14814572f, 0.15810387f,
    0.16807806f, 0.17806935f, 0.18807885f, 0.19810763f, 0.20815681f, 0.21822753f, 0.22832092f, 0.23843815f,
    0.24858040f, 0.25874886f, 0.26894477f, 0.27916937f, 0.28942392f, 0.29970972f, 0.31002809f, 0.32038038f,
    0.33076795f, 0.34119223f, 0.35165464f, 0.36215668f, 0.37269983f, 0.38328566f, 0.39391575f, 0.40459174f,
    0.41531530f, 0.42608815f, 0.43691208f, 0.44778889f, 0.45872048f, 0.46970878f, 0.48075580f, 0.49186359f,
    0.50303431f, 0.51427014f, 0.52557339f, 0.53694641f, 0.54839166f, 0.55991170f, 0.57150915f, 0.58318677f,
    0.59494741f, 0.60679406f, 0.61872982f, 0.63075791f, 0.64288172f, 0.65510478f, 0.66743079f, 0.67986362f,
    0.69240733f, 0.70506619f, 0.71784470f, 0.73074757f, 0.74377981f, 0.75694669f, 0.77025380f, 0.78370707f,
    0.79731280f, 0.81107773f, 0.82500904f, 0.83911445f, 0.85340225f, 0.86788140f, 0.88256158f, 0.89745334f,
    0.91256819f, 0.92791875f, 0.94351889f, 0.95938395f, 0.97553096f, 0.99197887f, 1.00874887f, 1.02586469f,
    1.04335303f, 1.06124395f, 1.07957138f, 1.09837363f, 1.11769397f, 1.13758133f, 1.15809091f, 1.17928502f,
    1.20123378f, 1.22401601f, 1.24772010f, 1.27244490f, 1.29830070f, 1.32541020f, 1.35390956f, 1.38394953f,
    1.41569668f, 1.44933479f, 1.48506647f, 1.52311519f, 1.56372770f, 1.60717730f, 1.65376804f, 1.70384040f,
    1.75777904f, 1.81602345f, 1.87908279f, 1.94755695f, 2.02216677f, 2.10379860f, 2.19357144f, 2.29294190f,
    2.40387496f, 2.52913769f, 2.67284063f, 2.84153407f, 3.04674351f, 3.31217014f, 3.70365827f,
};
// NOLINTEND(modernize-use-std-numbers)

/// Direct lookup table keyed by the raw BFloat16 bit pattern. BFloat16 has only 65536 possible
/// values, so the whole quantizer is precomputed once: at runtime quantization is a single load
/// indexed by the bits, with no Float32 conversion or boundary search. Built lazily on first use.
const std::array<Int8, 65536> & quantizeCodeLUT()
{
    static const std::array<Int8, 65536> lut = []
    {
        std::array<Int8, 65536> table{};
        for (UInt32 bits = 0; bits <= 0xFFFFu; ++bits)
        {
            const Float32 x = static_cast<Float32>(BFloat16::fromBits(static_cast<UInt16>(bits)));
            size_t index = 0;
            if (std::isnan(x))
                index = 128;
            else if (x == 0.0f)
                /// The central decision boundary is exactly 0; break the tie by sign so the sign
                /// bit always matches the input (+0 -> positive central cell, -0 -> negative).
                index = std::signbit(x) ? 127 : 128;
            else
                index = static_cast<size_t>(
                    std::lower_bound(std::begin(LLOYD_MAX_BOUNDARIES), std::end(LLOYD_MAX_BOUNDARIES), x)
                    - std::begin(LLOYD_MAX_BOUNDARIES));
            table[bits] = static_cast<Int8>(static_cast<Int16>(index) - 128);
        }
        return table;
    }();
    return lut;
}

/// Precomputed BFloat16 reconstruction levels (the bf16 representations of the 256 Lloyd-Max levels).
/// Dequantization is then a direct BFloat16 (i.e. UInt16) copy from this table, with no Float32
/// conversion at runtime. Built lazily on first use.
const std::array<BFloat16, 256> & dequantizeLevels()
{
    static const std::array<BFloat16, 256> levels = []
    {
        std::array<BFloat16, 256> table{};
        for (size_t i = 0; i < 256; ++i)
            table[i] = BFloat16(LLOYD_MAX_LEVELS[i]);
        return table;
    }();
    return levels;
}

/// Apply an elementwise codec to every element of an Array column, preserving the offsets.
template <typename SrcColumn, typename DstColumn, typename MapFunc>
ColumnPtr executeOverArray(const ColumnArray & col_array, MapFunc && map)
{
    const auto & src_data = assert_cast<const SrcColumn &>(col_array.getData()).getData();
    const size_t elements_count = src_data.size();

    auto res_data_column = DstColumn::create(elements_count);
    auto & res_data = res_data_column->getData();
    for (size_t i = 0; i < elements_count; ++i)
        res_data[i] = map(src_data[i]);

    return ColumnArray::create(std::move(res_data_column), col_array.getOffsetsPtr());
}

/// Reconstruct each vector from its bit-transposed QBit planes, apply an elementwise codec to every
/// element, then re-transpose the result into a QBit of the destination element type. Used for both
/// quantize (BFloat16 -> Int8) and dequantize (Int8 -> BFloat16); the two directions differ only in the
/// codec and the element types (and hence the number of bit planes: 16 for BFloat16, 8 for Int8). This
/// mirrors the Array <-> QBit conversion path in FunctionsConversion.
template <typename SrcElem, typename DstElem, typename MapFunc>
ColumnPtr executeOverQBit(const ColumnQBit & col_qbit, size_t dimension, size_t stride, MapFunc && map)
{
    /// The transpose/untranspose kernels operate on an unsigned word of the element's width. The 8-bit word
    /// must be `uint8_t`, not ClickHouse's `UInt8` (`char8_t`), to satisfy the kernels' bit intrinsics.
    using SrcWord = std::conditional_t<sizeof(SrcElem) == 1, uint8_t, UInt16>;
    using DstWord = std::conditional_t<sizeof(DstElem) == 1, uint8_t, UInt16>;

    constexpr size_t src_bits = sizeof(SrcElem) * 8;
    constexpr size_t dst_bits = sizeof(DstElem) * 8;

    const size_t rows = col_qbit.size();
    const ColumnTuple & src_tuple = col_qbit.getNestedData();

    const size_t num_strides = dimension / stride;
    const size_t bytes_per_fixedstring = DataTypeQBit::bitsToBytes(stride);
    const size_t padded_stride = bytes_per_fixedstring * 8;

    const auto untranspose = SerializationQBit::resolveUntransposeBitPlane<SrcWord>();

    /// The destination QBit stores `dst_bits` bit planes per stride group, grouped as [group][plane].
    const size_t num_dst_columns = dst_bits * num_strides;
    MutableColumns dst_columns(num_dst_columns);
    for (size_t i = 0; i < num_dst_columns; ++i)
    {
        auto column = ColumnFixedString::create(bytes_per_fixedstring);
        column->reserve(rows);
        dst_columns[i] = std::move(column);
    }

    /// Reusable scratch buffer for one stride group's reconstructed elements. The untranspose kernel ORs
    /// bits in, so it must be zeroed before each group.
    VectorWithMemoryTracking<SrcElem> reconstructed(padded_stride);

    for (size_t row = 0; row < rows; ++row)
    {
        /// Append a default (zero) FixedString for this row to every destination plane and remember where
        /// to transpose into. Padding elements (only present when stride is not a multiple of 8, i.e. the
        /// non-strided case) stay zero, matching the Array -> QBit conversion path.
        VectorWithMemoryTracking<char *> row_ptrs(num_dst_columns);
        for (size_t j = 0; j < num_dst_columns; ++j)
        {
            auto & fixed_string_column = assert_cast<ColumnFixedString &>(*dst_columns[j]);
            fixed_string_column.insertDefault();
            auto & chars = fixed_string_column.getChars();
            row_ptrs[j] = reinterpret_cast<char *>(&chars[chars.size() - bytes_per_fixedstring]);
        }

        for (size_t group = 0; group < num_strides; ++group)
        {
            /// The value 0 has an all-zero bit pattern for BFloat16 and Int8, matching the zero word the kernel ORs into.
            std::memset(reconstructed.data(), 0, reconstructed.size() * sizeof(SrcElem));

            /// Untranspose stride group `group`'s bit planes (tuple columns [group * src_bits, group * src_bits + src_bits)).
            for (size_t bit = 0; bit < src_bits; ++bit)
            {
                const auto & fixed_string_column = assert_cast<const ColumnFixedString &>(src_tuple.getColumn(group * src_bits + bit));
                const UInt8 * src = reinterpret_cast<const UInt8 *>(fixed_string_column.getChars().data()) + row * bytes_per_fixedstring;
                const SrcWord mask = static_cast<SrcWord>(SrcWord(1) << (src_bits - 1 - bit));
                untranspose(src, reinterpret_cast<SrcWord *>(reconstructed.data()), padded_stride, mask);
            }

            /// Apply the codec to each real dimension and transpose it into this group's destination planes.
            for (size_t i = 0; i < stride; ++i)
            {
                DstElem mapped = map(reconstructed[i]);
                DstWord w = 0;
                std::memcpy(&w, &mapped, sizeof(DstElem));
                SerializationQBit::transposeBits<DstWord>(w, i, padded_stride, row_ptrs.data() + group * dst_bits);
            }
        }
    }

    ColumnPtr dst_tuple = ColumnTuple::create(std::move(dst_columns));
    return ColumnQBit::create(dst_tuple, dimension, stride);
}

class FunctionQuantizeBFloat16ToInt8 : public IFunction
{
public:
    static constexpr auto name = "quantizeBFloat16ToInt8";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionQuantizeBFloat16ToInt8>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }
    bool isInjective(const ColumnsWithTypeAndName &) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const DataTypePtr & arg = arguments[0];

        if (WhichDataType(arg).isBFloat16())
            return std::make_shared<DataTypeInt8>();

        if (const auto * array_type = checkAndGetDataType<DataTypeArray>(arg.get()))
        {
            if (WhichDataType(array_type->getNestedType()).isBFloat16())
                return std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt8>());
        }
        else if (const auto * qbit_type = checkAndGetDataType<DataTypeQBit>(arg.get()))
        {
            if (WhichDataType(qbit_type->getElementType()).isBFloat16())
                return std::make_shared<DataTypeQBit>(
                    std::make_shared<DataTypeInt8>(), qbit_type->getDimension(), qbit_type->getStride());
        }

        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Argument of function {} must be BFloat16, Array(BFloat16) or QBit(BFloat16, ...), got {}", getName(), arg->getName());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const IColumn * col = arguments[0].column.get();
        const std::array<Int8, 65536> & lut = quantizeCodeLUT();
        auto quantize = [&lut](BFloat16 v) -> Int8 { return lut[std::bit_cast<UInt16>(v)]; };

        if (const auto * col_bfloat16 = checkAndGetColumn<ColumnBFloat16>(col))
        {
            auto col_res = ColumnInt8::create(input_rows_count);
            const auto & src = col_bfloat16->getData();
            auto & dst = col_res->getData();
            for (size_t i = 0; i < input_rows_count; ++i)
                dst[i] = quantize(src[i]);
            return col_res;
        }

        if (const auto * col_array = checkAndGetColumn<ColumnArray>(col))
            return executeOverArray<ColumnBFloat16, ColumnInt8>(*col_array, quantize);

        if (const auto * col_qbit = checkAndGetColumn<ColumnQBit>(col))
            return executeOverQBit<BFloat16, Int8>(*col_qbit, col_qbit->getDimension(), col_qbit->getStride(), quantize);

        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Argument of function {} has unexpected type {}", getName(), arguments[0].type->getName());
    }
};

class FunctionDequantizeInt8ToBFloat16 : public IFunction
{
public:
    static constexpr auto name = "dequantizeInt8ToBFloat16";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionDequantizeInt8ToBFloat16>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }
    bool isInjective(const ColumnsWithTypeAndName &) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const DataTypePtr & arg = arguments[0];

        if (WhichDataType(arg).isInt8())
            return std::make_shared<DataTypeBFloat16>();

        if (const auto * array_type = checkAndGetDataType<DataTypeArray>(arg.get()))
        {
            if (WhichDataType(array_type->getNestedType()).isInt8())
                return std::make_shared<DataTypeArray>(std::make_shared<DataTypeBFloat16>());
        }
        else if (const auto * qbit_type = checkAndGetDataType<DataTypeQBit>(arg.get()))
        {
            if (WhichDataType(qbit_type->getElementType()).isInt8())
                return std::make_shared<DataTypeQBit>(
                    std::make_shared<DataTypeBFloat16>(), qbit_type->getDimension(), qbit_type->getStride());
        }

        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Argument of function {} must be Int8, Array(Int8) or QBit(Int8, ...), got {}", getName(), arg->getName());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const IColumn * col = arguments[0].column.get();
        const std::array<BFloat16, 256> & levels = dequantizeLevels();
        /// Direct copy of the precomputed BFloat16 level -- no Float32 conversion at runtime.
        auto dequantize = [&levels](Int8 code) -> BFloat16 { return levels[static_cast<size_t>(static_cast<Int16>(code) + 128)]; };

        if (const auto * col_int8 = checkAndGetColumn<ColumnInt8>(col))
        {
            auto col_res = ColumnBFloat16::create(input_rows_count);
            const auto & src = col_int8->getData();
            auto & dst = col_res->getData();
            for (size_t i = 0; i < input_rows_count; ++i)
                dst[i] = dequantize(src[i]);
            return col_res;
        }

        if (const auto * col_array = checkAndGetColumn<ColumnArray>(col))
            return executeOverArray<ColumnInt8, ColumnBFloat16>(*col_array, dequantize);

        if (const auto * col_qbit = checkAndGetColumn<ColumnQBit>(col))
            return executeOverQBit<Int8, BFloat16>(*col_qbit, col_qbit->getDimension(), col_qbit->getStride(), dequantize);

        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Argument of function {} has unexpected type {}", getName(), arguments[0].type->getName());
    }
};

}

REGISTER_FUNCTION(QuantizeLloydMax)
{
    {
        FunctionDocumentation::Description description = R"(
Quantizes a `BFloat16` value to `Int8` using a 256-level Gaussian Lloyd-Max quantizer
(the MSE-optimal scalar quantizer for a standard-normal source).

Intended for values that are approximately distributed as `N(0, 1)`. A random orthogonal
(randomized Hadamard) rotation preserves the norm, so for a `d`-dimensional unit-norm embedding
each rotated coordinate has variance `1/d`; scale the rotated vector by `sqrt(d)` to reach unit
variance before quantizing.

The argument may be a single `BFloat16`, an `Array(BFloat16)`, or a `QBit(BFloat16, ...)`; the
function is applied to every element and returns `Int8`, `Array(Int8)`, or a `QBit(Int8, ...)` of
the same dimension and stride respectively. Passing a whole vector (`Array` or `QBit`) is preferred
over `arrayMap(x -> quantizeBFloat16ToInt8(x), ...)`.

The result is the index `0..255` of the Lloyd-Max cell, stored as `Int8` as `index - 128`, so
the sign bit equals the sign of the value (`+0`/`-0` map to the positive/negative central cells)
and the top `b` bits form a valid embedded `2^b`-level quantizer: bit-truncation of the code
yields coarser Int4/Int2/binary codes.

Use [`dequantizeInt8ToBFloat16`](#dequantizeInt8ToBFloat16) to reconstruct the value.
)";
        FunctionDocumentation::Syntax syntax = "quantizeBFloat16ToInt8(x)";
        FunctionDocumentation::Arguments arguments = {{"x", "Value(s) to quantize (expected to be ~N(0,1)).", {"BFloat16", "Array(BFloat16)", "QBit(BFloat16)"}}};
        FunctionDocumentation::ReturnedValue returned_value = {"The Lloyd-Max cell index minus 128.", {"Int8", "Array(Int8)", "QBit(Int8)"}};
        FunctionDocumentation::Examples examples = {
            {"Quantize a scalar", "SELECT quantizeBFloat16ToInt8(2.0::BFloat16)", "116"},
            {"Quantize an array", "SELECT quantizeBFloat16ToInt8([0.1, -0.5, 2.0]::Array(BFloat16))", "[10,-49,116]"}};
        FunctionDocumentation::IntroducedIn introduced_in = {26, 7};
        FunctionDocumentation::Category category = FunctionDocumentation::Category::QBit;
        FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};
        factory.registerFunction<FunctionQuantizeBFloat16ToInt8>(documentation);
    }
    {
        FunctionDocumentation::Description description = R"(
Reconstructs a `BFloat16` value from an `Int8` code produced by
[`quantizeBFloat16ToInt8`](#quantizeBFloat16ToInt8), by mapping the code back to the
reconstruction level of its Gaussian Lloyd-Max cell.

The argument may be a single `Int8`, an `Array(Int8)`, or a `QBit(Int8, ...)`; the function is
applied to every element and returns `BFloat16`, `Array(BFloat16)`, or a `QBit(BFloat16, ...)` of
the same dimension and stride respectively.
)";
        FunctionDocumentation::Syntax syntax = "dequantizeInt8ToBFloat16(x)";
        FunctionDocumentation::Arguments arguments = {{"x", "Code(s) produced by quantizeBFloat16ToInt8.", {"Int8", "Array(Int8)", "QBit(Int8)"}}};
        FunctionDocumentation::ReturnedValue returned_value = {"The reconstruction level of the cell.", {"BFloat16", "Array(BFloat16)", "QBit(BFloat16)"}};
        FunctionDocumentation::Examples examples = {
            {"Round trip", "SELECT round(toFloat32(dequantizeInt8ToBFloat16(quantizeBFloat16ToInt8(0.5::BFloat16))), 4)", "0.4961"}};
        FunctionDocumentation::IntroducedIn introduced_in = {26, 7};
        FunctionDocumentation::Category category = FunctionDocumentation::Category::QBit;
        FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};
        factory.registerFunction<FunctionDequantizeInt8ToBFloat16>(documentation);
    }
}

}
