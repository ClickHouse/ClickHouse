#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnFixedArray.h>
#include <Columns/ColumnFunction.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFixedArray.h>
#include <DataTypes/DataTypeFunction.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/castColumn.h>
#include <Common/PODArray.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/StringUtils.h>
#include <base/arithmeticOverflow.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>

#include <algorithm>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int SIZES_OF_ARRAYS_DONT_MATCH;
    extern const int TOO_LARGE_ARRAY_SIZE;
}

namespace
{

/** einsum(subscripts, [combiner,] tensor1, ..., tensorN) - Einstein summation.
  *
  * The subscripts are in the notation of NumPy: `ik,kj->ij` is the matrix multiplication.
  * Every tensor is an `Array` or `FixedArray`, nested as many times as there are letters in its subscripts.
  * The result is indexed by the letters after `->`, and it is the sum over the letters that are not there
  * of the product of the elements of the tensors at the corresponding indices.
  * If `->` is omitted, the result is indexed by the letters that appear only once, in the alphabetical order.
  *
  * The combiner is a lambda function that is applied to the elements of the tensors instead of the product:
  * `einsum('ij,ij->ij', (x, y) -> x - y, a, b)` is the difference of two matrices.
  * The lambda form of `einsum`, `einsum((i, j, k) -> a[i][k] * b[k][j])`, is rewritten to this form by the analyzer.
  */

/// The maximum number of different letters in subscripts: a-z and A-Z.
constexpr size_t max_einsum_indices = 52;

/// The maximum number of terms (products of elements of the tensors) in a single row,
/// to protect from the queries that would compute practically forever.
constexpr size_t max_einsum_terms_in_row = 1ULL << 40;

/// The number of terms evaluated at once by the generic implementation.
constexpr size_t einsum_block_size = 65536;


/// Parsed subscripts.
struct EinsumSpec
{
    /// Indices are numbered in the order of the first appearance.
    VectorWithMemoryTracking<VectorWithMemoryTracking<size_t>> inputs;
    VectorWithMemoryTracking<size_t> output;
    /// The indices that are not in the output: the result is the sum over them.
    VectorWithMemoryTracking<size_t> contracted;
    VectorWithMemoryTracking<char> letters;

    size_t numIndices() const { return letters.size(); }
};

EinsumSpec parseEinsumSpec(std::string_view str)
{
    EinsumSpec spec;
    std::array<int, 128> index_of_letter{};
    index_of_letter.fill(-1);
    std::array<size_t, 128> occurrences{};

    auto get_index = [&](char letter)
    {
        auto & index = index_of_letter[static_cast<UInt8>(letter)];
        if (index < 0)
        {
            if (spec.letters.size() == max_einsum_indices)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Too many different indices in einsum subscripts '{}'", str);
            index = static_cast<int>(spec.letters.size());
            spec.letters.push_back(letter);
        }
        return static_cast<size_t>(index);
    };

    std::optional<VectorWithMemoryTracking<size_t>> output;
    spec.inputs.emplace_back();

    for (size_t pos = 0; pos < str.size(); ++pos)
    {
        char c = str[pos];
        if (isWhitespaceASCII(c))
            continue;

        if (isAlphaASCII(c))
        {
            if (output)
            {
                if (index_of_letter[static_cast<UInt8>(c)] < 0)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Index '{}' in the output of einsum subscripts '{}' does not appear in the inputs", c, str);
                size_t index = get_index(c);
                if (std::find(output->begin(), output->end(), index) != output->end())
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Index '{}' appears more than once in the output of einsum subscripts '{}'", c, str);
                output->push_back(index);
            }
            else
            {
                spec.inputs.back().push_back(get_index(c));
                ++occurrences[static_cast<UInt8>(c)];
            }
        }
        else if (c == ',' && !output)
            spec.inputs.emplace_back();
        else if (c == '-' && !output && pos + 1 < str.size() && str[pos + 1] == '>')
        {
            output.emplace();
            ++pos;
        }
        else if (c == '.')
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Ellipsis in einsum subscripts '{}' is not supported, all the dimensions have to be named explicitly", str);
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected character '{}' in einsum subscripts '{}'", c, str);
    }

    if (output)
        spec.output = std::move(*output);
    else
    {
        /// The implicit output consists of the indices that appear once, in the alphabetical order.
        VectorWithMemoryTracking<char> once;
        for (char letter : spec.letters)
            if (occurrences[static_cast<UInt8>(letter)] == 1)
                once.push_back(letter);
        std::sort(once.begin(), once.end());
        for (char letter : once)
            spec.output.push_back(static_cast<size_t>(index_of_letter[static_cast<UInt8>(letter)]));
    }

    for (size_t index = 0; index < spec.numIndices(); ++index)
        if (std::find(spec.output.begin(), spec.output.end(), index) == spec.output.end())
            spec.contracted.push_back(index);

    return spec;
}


/// The type of a tensor: `Array` and `FixedArray` nested in each other.
struct TensorType
{
    /// The size of each dimension, if it is known from the type (`FixedArray`).
    VectorWithMemoryTracking<std::optional<size_t>> dims;
    DataTypePtr element;
};

TensorType getTensorType(const DataTypePtr & type)
{
    TensorType res;
    DataTypePtr current = type;
    while (true)
    {
        if (const auto * array_type = typeid_cast<const DataTypeArray *>(current.get()))
        {
            res.dims.push_back(std::nullopt);
            current = array_type->getNestedType();
        }
        else if (const auto * fixed_array_type = typeid_cast<const DataTypeFixedArray *>(current.get()))
        {
            res.dims.push_back(fixed_array_type->getArraySize());
            current = fixed_array_type->getNestedType();
        }
        else
            break;
    }
    res.element = current;
    return res;
}

/// The type in which the products and sums are calculated and returned.
DataTypePtr getAccumulatorType(const DataTypes & types, std::string_view function_name)
{
    bool has_float64 = false;
    bool has_float32 = false;
    bool has_signed = false;
    bool has_wide_integer = false;

    for (const auto & type : types)
    {
        WhichDataType which(type);
        if (which.isFloat64())
            has_float64 = true;
        else if (which.isFloat32() || which.isBFloat16())
            has_float32 = true;
        else if (which.isNativeInteger())
        {
            has_signed |= which.isNativeInt();
            has_wide_integer |= type->getSizeOfValueInMemory() > 2;
        }
        else
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Function {} can multiply and sum only numbers, got {}. Use a lambda function to combine the elements of other types",
                function_name, type->getName());
    }

    /// Integers up to 16 bits are represented exactly in Float32.
    if (has_float64 || (has_float32 && has_wide_integer))
        return std::make_shared<DataTypeFloat64>();
    if (has_float32)
        return std::make_shared<DataTypeFloat32>();
    if (has_signed)
        return std::make_shared<DataTypeInt64>();
    return std::make_shared<DataTypeUInt64>();
}


/// Everything about the arguments that does not depend on the values.
struct EinsumSignature
{
    EinsumSpec spec;
    bool has_combiner = false;
    VectorWithMemoryTracking<TensorType> tensors;
    /// The size of each index, if it is known from the types.
    VectorWithMemoryTracking<std::optional<size_t>> static_sizes;
    /// The products are calculated by multiplying numbers, without the combiner.
    bool is_product = false;
    /// The result is the sum or product of numbers, calculated in `result_element_type`.
    bool is_numeric = false;
    DataTypePtr value_type;
    DataTypePtr result_element_type;
    DataTypePtr result_type;
};


/// The layout of the values of a tensor in the column of its elements, for every row.
struct TensorRows
{
    /// The column of the elements, of the innermost type.
    ColumnPtr elements;
    /// The position of the first element of each row, the elements of a row are contiguous in the row-major order.
    PaddedPODArray<UInt64> offsets;
    /// The sizes of the dimensions for each row; `unknown_size` if a dimension is unknown because an outer one is empty.
    PaddedPODArray<UInt64> dims;
    size_t rank = 0;
    /// The tensor is the same for all rows.
    bool is_const = false;

    static constexpr UInt64 unknown_size = std::numeric_limits<UInt64>::max();

    size_t row(size_t i) const { return is_const ? 0 : i; }
    const UInt64 * rowDims(size_t i) const { return dims.data() + row(i) * rank; }
};

TensorRows getTensorRows(const ColumnPtr & column, size_t rank, size_t input_rows_count, size_t argument_index, std::string_view function_name)
{
    TensorRows res;
    res.rank = rank;

    ColumnPtr full = column->convertToFullColumnIfReplicated();
    size_t rows = input_rows_count;
    if (const auto * column_const = typeid_cast<const ColumnConst *>(full.get()))
    {
        res.is_const = true;
        full = column_const->getDataColumnPtr();
        rows = 1;
    }

    /// The columns of the levels: level 0 is the column of tensors, level `rank` is the column of elements.
    VectorWithMemoryTracking<const IColumn *> levels(rank + 1);
    levels[0] = full.get();
    for (size_t level = 0; level < rank; ++level)
    {
        if (const auto * column_array = typeid_cast<const ColumnArray *>(levels[level]))
            levels[level + 1] = &column_array->getData();
        else if (const auto * column_fixed_array = typeid_cast<const ColumnFixedArray *>(levels[level]))
            levels[level + 1] = &column_fixed_array->getData();
        else
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Unexpected column {} of argument {} of function {}",
                levels[level]->getName(), argument_index + 1, function_name);
    }

    res.elements = full;
    for (size_t level = 0; level < rank; ++level)
    {
        if (const auto * column_array = typeid_cast<const ColumnArray *>(res.elements.get()))
            res.elements = column_array->getDataPtr();
        else
            res.elements = assert_cast<const ColumnFixedArray &>(*res.elements).getDataPtr();
    }

    res.offsets.resize(rows);
    res.dims.resize(rows * rank);

    for (size_t row = 0; row < rows; ++row)
    {
        /// The range of elements of this row in the column of the current level.
        size_t begin = row;
        size_t count = 1;
        UInt64 * dims = res.dims.data() + row * rank;

        for (size_t level = 0; level < rank; ++level)
        {
            if (const auto * column_fixed_array = typeid_cast<const ColumnFixedArray *>(levels[level]))
            {
                size_t n = column_fixed_array->getArraySize();
                dims[level] = n;
                begin *= n;
                count *= n;
                continue;
            }

            const auto & offsets = assert_cast<const ColumnArray &>(*levels[level]).getOffsets();
            if (count == 0)
            {
                /// The tensor is empty, the sizes of the inner dimensions and the position do not matter.
                dims[level] = TensorRows::unknown_size;
                begin = 0;
                continue;
            }

            size_t size = offsets[begin] - offsets[static_cast<ssize_t>(begin) - 1];
            for (size_t i = begin + 1; i < begin + count; ++i)
            {
                if (offsets[i] - offsets[i - 1] != size)
                    throw Exception(ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH,
                        "Argument {} of function {} is not a tensor: the arrays of dimension {} have different sizes {} and {}",
                        argument_index + 1, function_name, level + 1, size, offsets[i] - offsets[i - 1]);
            }

            dims[level] = size;
            begin = offsets[static_cast<ssize_t>(begin) - 1];
            count *= size;
        }

        res.offsets[row] = begin;
    }

    return res;
}


/** The sizes of all indices for a row, and the strides of the tensors.
  * The stride of a tensor for an index is the sum of strides of its dimensions with this index:
  * for `ii` the stride is `n + 1`, which takes the diagonal.
  */
struct RowLayout
{
    VectorWithMemoryTracking<size_t> sizes;
    /// [tensor][index], zero if the tensor does not have the index.
    VectorWithMemoryTracking<VectorWithMemoryTracking<size_t>> strides;
    size_t num_output_elements = 1;
    size_t num_contracted_terms = 1;
};

void calculateRowLayout(const EinsumSpec & spec, const VectorWithMemoryTracking<TensorRows> & tensors, size_t row, RowLayout & layout, std::string_view function_name)
{
    size_t num_indices = spec.numIndices();
    layout.sizes.assign(num_indices, TensorRows::unknown_size);

    for (size_t tensor = 0; tensor < tensors.size(); ++tensor)
    {
        const UInt64 * dims = tensors[tensor].rowDims(row);
        const auto & subscripts = spec.inputs[tensor];
        for (size_t dim = 0; dim < subscripts.size(); ++dim)
        {
            if (dims[dim] == TensorRows::unknown_size)
                continue;

            auto & size = layout.sizes[subscripts[dim]];
            if (size == TensorRows::unknown_size)
                size = dims[dim];
            else if (size != dims[dim])
                throw Exception(ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH,
                    "Sizes of dimensions for index '{}' in function {} don't match: {} and {}",
                    spec.letters[subscripts[dim]], function_name, size, dims[dim]);
        }
    }

    /// An index whose size is unknown belongs only to empty tensors.
    for (auto & size : layout.sizes)
        if (size == TensorRows::unknown_size)
            size = 0;

    layout.strides.resize(tensors.size());
    for (size_t tensor = 0; tensor < tensors.size(); ++tensor)
    {
        auto & strides = layout.strides[tensor];
        strides.assign(num_indices, 0);
        const auto & subscripts = spec.inputs[tensor];
        size_t stride = 1;
        for (size_t dim = subscripts.size(); dim > 0; --dim)
        {
            strides[subscripts[dim - 1]] += stride;
            stride *= layout.sizes[subscripts[dim - 1]];
        }
    }

    auto multiply = [&](size_t & res, size_t size)
    {
        if (common::mulOverflow(res, size, res) || res > max_einsum_terms_in_row)
            throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Too many terms in function {}", function_name);
    };

    layout.num_output_elements = 1;
    for (size_t index : spec.output)
        multiply(layout.num_output_elements, layout.sizes[index]);

    layout.num_contracted_terms = 1;
    for (size_t index : spec.contracted)
        multiply(layout.num_contracted_terms, layout.sizes[index]);

    size_t total = layout.num_output_elements;
    multiply(total, layout.num_contracted_terms);
}


/** The kernels of the numeric implementation.
  * The values of integer types are calculated in UInt64: the results of addition and multiplication
  * modulo 2^64 are the same for signed and unsigned numbers, and the overflow of unsigned numbers is defined.
  */

/// A loop over one index: its size, the stride in each operand and in the output.
struct Loop
{
    size_t size;
    VectorWithMemoryTracking<size_t> operand_strides;
    size_t output_stride;
};

/** out[...] += sum over the loops of the product of operands, for all combinations of indices of the loops.
  * A loop with zero output stride is a summation. It is used both for the contractions and for copying between layouts.
  */
template <typename T>
class LoopNest
{
public:
    /// The loops are from the outermost to the innermost. If the innermost loop is a summation,
    /// the sum is accumulated in a register.
    LoopNest(VectorWithMemoryTracking<Loop> loops_, VectorWithMemoryTracking<const T *> operands_, T * output_)
        : loops(std::move(loops_)), operands(std::move(operands_)), output(output_), offsets(operands.size())
    {
    }

    void run()
    {
        for (const auto & loop : loops)
            if (loop.size == 0)
                return;

        std::fill(offsets.begin(), offsets.end(), 0);
        runLevel(0, 0);
    }

private:
    VectorWithMemoryTracking<Loop> loops;
    VectorWithMemoryTracking<const T *> operands;
    T * output;
    VectorWithMemoryTracking<size_t> offsets;

    void runLevel(size_t level, size_t output_offset)
    {
        if (loops.empty())
        {
            T product = 1;
            for (const auto * operand : operands)
                product *= operand[0];
            output[0] += product;
            return;
        }

        const Loop & loop = loops[level];
        if (level + 1 == loops.size())
        {
            runInnermost(loop, output_offset);
            return;
        }

        for (size_t i = 0; i < loop.size; ++i)
        {
            runLevel(level + 1, output_offset + i * loop.output_stride);
            for (size_t operand = 0; operand < operands.size(); ++operand)
                offsets[operand] += loop.operand_strides[operand];
        }
        for (size_t operand = 0; operand < operands.size(); ++operand)
            offsets[operand] -= loop.size * loop.operand_strides[operand];
    }

    void runInnermost(const Loop & loop, size_t output_offset)
    {
        T * out = output + output_offset;
        size_t size = loop.size;
        size_t out_stride = loop.output_stride;

        if (operands.size() == 1)
        {
            const T * a = operands[0] + offsets[0];
            size_t sa = loop.operand_strides[0];
            if (out_stride == 0)
            {
                T sum = 0;
                for (size_t i = 0; i < size; ++i)
                    sum += a[i * sa];
                *out += sum;
            }
            else
            {
                for (size_t i = 0; i < size; ++i)
                    out[i * out_stride] += a[i * sa];
            }
        }
        else if (operands.size() == 2)
        {
            const T * a = operands[0] + offsets[0];
            const T * b = operands[1] + offsets[1];
            size_t sa = loop.operand_strides[0];
            size_t sb = loop.operand_strides[1];
            if (out_stride == 0)
            {
                T sum = 0;
                for (size_t i = 0; i < size; ++i)
                    sum += a[i * sa] * b[i * sb];
                *out += sum;
            }
            else
            {
                for (size_t i = 0; i < size; ++i)
                    out[i * out_stride] += a[i * sa] * b[i * sb];
            }
        }
        else
        {
            for (size_t i = 0; i < size; ++i)
            {
                T product = 1;
                for (size_t operand = 0; operand < operands.size(); ++operand)
                    product *= operands[operand][offsets[operand] + i * loop.operand_strides[operand]];
                out[i * out_stride] += product;
            }
        }
    }
};

/// c[m][n] += a[m][k] * b[k][n], all matrices are contiguous in the row-major order.
template <typename T>
void multiplyMatrices(size_t m, size_t n, size_t k, const T * __restrict a, const T * __restrict b, T * __restrict c)
{
    /// The blocks of `b` are reused for all rows of `a` while they are in the cache.
    constexpr size_t block_n = 256;
    constexpr size_t block_k = 128;

    for (size_t jj = 0; jj < n; jj += block_n)
    {
        size_t j_end = std::min(n, jj + block_n);
        for (size_t pp = 0; pp < k; pp += block_k)
        {
            size_t p_end = std::min(k, pp + block_k);
            for (size_t i = 0; i < m; ++i)
            {
                T * __restrict c_row = c + i * n;
                for (size_t p = pp; p < p_end; ++p)
                {
                    T a_value = a[i * k + p];
                    const T * __restrict b_row = b + p * n;
                    for (size_t j = jj; j < j_end; ++j)
                        c_row[j] += a_value * b_row[j];
                }
            }
        }
    }
}

/// c[m][n] += sum over k of a[m][k] * b_transposed[n][k], for a small `n`, when the dot products are more efficient.
template <typename T>
void multiplyMatricesTransposed(size_t m, size_t n, size_t k, const T * __restrict a, const T * __restrict b_transposed, T * __restrict c)
{
    constexpr size_t lanes = 8;
    for (size_t i = 0; i < m; ++i)
    {
        const T * __restrict a_row = a + i * k;
        for (size_t j = 0; j < n; ++j)
        {
            const T * __restrict b_row = b_transposed + j * k;
            /// Independent accumulators can be calculated in a vector register.
            T sums[lanes] = {};
            size_t p = 0;
            for (; p + lanes <= k; p += lanes)
                for (size_t lane = 0; lane < lanes; ++lane)
                    sums[lane] += a_row[p + lane] * b_row[p + lane];
            T sum = 0;
            for (const T & lane_sum : sums)
                sum += lane_sum;
            for (; p < k; ++p)
                sum += a_row[p] * b_row[p];
            c[i * n + j] += sum;
        }
    }
}


/// A tensor of the numeric implementation for one row: the pointer to the first element and the stride for every index.
template <typename T>
struct StridedTensor
{
    const T * data;
    VectorWithMemoryTracking<size_t> strides;
};

template <typename T>
class NumericContraction
{
public:
    explicit NumericContraction(const VectorWithMemoryTracking<size_t> & sizes_) : sizes(sizes_) {}

    /** output[...] = the contraction of `operands` with the indices `output_indices`.
      * The output is contiguous in the row-major order of `output_indices` and is filled with zeros before the call.
      */
    void contract(const VectorWithMemoryTracking<StridedTensor<T>> & operands, const VectorWithMemoryTracking<size_t> & output_indices, T * output)
    {
        if (operands.size() == 2 && contractPairAsMatrices(operands[0], operands[1], output_indices, output))
            return;

        if (operands.size() <= 2)
        {
            contractWithLoopNest(operands, output_indices, output);
            return;
        }

        /// Contract the operands pairwise from left to right, keeping the indices that are needed later.
        StridedTensor<T> current = operands[0];
        PODArray<T> storage;
        for (size_t next = 1; next + 1 < operands.size(); ++next)
        {
            VectorWithMemoryTracking<size_t> intermediate_indices;
            for (size_t index = 0; index < sizes.size(); ++index)
            {
                if (!isUsed(current, index) && !isUsed(operands[next], index))
                    continue;

                bool needed = std::find(output_indices.begin(), output_indices.end(), index) != output_indices.end();
                for (size_t later = next + 1; later < operands.size() && !needed; ++later)
                    needed = isUsed(operands[later], index);
                if (needed)
                    intermediate_indices.push_back(index);
            }

            PODArray<T> intermediate(product(intermediate_indices), T{});
            contract({current, operands[next]}, intermediate_indices, intermediate.data());
            storage.swap(intermediate);
            current = {storage.data(), contiguousStrides(intermediate_indices)};
        }

        contract({current, operands.back()}, output_indices, output);
    }

private:
    const VectorWithMemoryTracking<size_t> & sizes;

    /// All sizes are positive here (otherwise there is nothing to calculate), so the stride of an index is zero
    /// if and only if the tensor does not have it.
    static bool isUsed(const StridedTensor<T> & tensor, size_t index) { return tensor.strides[index] != 0; }

    size_t product(const VectorWithMemoryTracking<size_t> & indices) const
    {
        size_t res = 1;
        for (size_t index : indices)
            res *= sizes[index];
        return res;
    }

    VectorWithMemoryTracking<size_t> contiguousStrides(const VectorWithMemoryTracking<size_t> & indices) const
    {
        VectorWithMemoryTracking<size_t> strides(sizes.size(), 0);
        size_t stride = 1;
        for (size_t i = indices.size(); i > 0; --i)
        {
            strides[indices[i - 1]] = stride;
            stride *= sizes[indices[i - 1]];
        }
        return strides;
    }

    /// Whether the tensor already has the layout of a contiguous tensor with `indices`.
    bool isContiguous(const StridedTensor<T> & tensor, const VectorWithMemoryTracking<size_t> & indices) const
    {
        auto expected = contiguousStrides(indices);
        for (size_t index = 0; index < sizes.size(); ++index)
            if (sizes[index] > 1 && tensor.strides[index] != expected[index])
                return false;
        return true;
    }

    /// The general implementation: a loop over all the indices of the operands.
    void contractWithLoopNest(const VectorWithMemoryTracking<StridedTensor<T>> & operands, const VectorWithMemoryTracking<size_t> & output_indices, T * output)
    {
        auto output_strides = contiguousStrides(output_indices);

        VectorWithMemoryTracking<Loop> loops;
        VectorWithMemoryTracking<const T *> pointers;
        for (const auto & operand : operands)
            pointers.push_back(operand.data);

        for (size_t index = 0; index < sizes.size(); ++index)
        {
            bool is_output = std::find(output_indices.begin(), output_indices.end(), index) != output_indices.end();
            bool is_used = is_output;
            for (const auto & operand : operands)
                is_used |= isUsed(operand, index);
            if (!is_used)
                continue;

            Loop loop{sizes[index], {}, output_strides[index]};
            for (const auto & operand : operands)
                loop.operand_strides.push_back(operand.strides[index]);
            loops.push_back(std::move(loop));
        }

        /// The output indices go first, in their order, so that the output is written sequentially.
        std::stable_sort(loops.begin(), loops.end(), [](const Loop & lhs, const Loop & rhs) { return lhs.output_stride > rhs.output_stride; });

        LoopNest<T>(std::move(loops), std::move(pointers), output).run();
    }

    /// Copies (and sums over the indices that are not in `indices`) the tensor into a contiguous tensor with `indices`.
    const T * toContiguous(const StridedTensor<T> & tensor, const VectorWithMemoryTracking<size_t> & indices, PODArray<T> & storage)
    {
        bool has_other_indices = false;
        for (size_t index = 0; index < sizes.size(); ++index)
            if (isUsed(tensor, index) && std::find(indices.begin(), indices.end(), index) == indices.end())
                has_other_indices = true;

        if (!has_other_indices && isContiguous(tensor, indices))
            return tensor.data;

        storage.assign(product(indices), T{});
        contractWithLoopNest({tensor}, indices, storage.data());
        return storage.data();
    }

    /** The contraction of two tensors as a batch of matrix multiplications:
      * the indices are grouped into the batch (in both tensors and in the output), the rows (only in the first tensor and in the output),
      * the columns (only in the second tensor and in the output) and the summation (in both tensors, not in the output).
      * The indices that are only in one tensor and not in the output are summed before the multiplication.
      */
    bool contractPairAsMatrices(const StridedTensor<T> & a, const StridedTensor<T> & b, const VectorWithMemoryTracking<size_t> & output_indices, T * output)
    {
        VectorWithMemoryTracking<size_t> batch;
        VectorWithMemoryTracking<size_t> rows;
        VectorWithMemoryTracking<size_t> columns;
        VectorWithMemoryTracking<size_t> summation;

        for (size_t index = 0; index < sizes.size(); ++index)
        {
            bool in_a = isUsed(a, index);
            bool in_b = isUsed(b, index);
            bool in_output = std::find(output_indices.begin(), output_indices.end(), index) != output_indices.end();
            if (in_a && in_b && in_output)
                batch.push_back(index);
            else if (in_a && in_b)
                summation.push_back(index);
            else if (in_a && in_output)
                rows.push_back(index);
            else if (in_b && in_output)
                columns.push_back(index);
        }

        size_t summation_size = product(summation);
        if (summation.empty() || summation_size < 4)
            return false;

        size_t batch_size = product(batch);
        size_t rows_size = product(rows);
        size_t columns_size = product(columns);
        bool use_dot_products = columns_size < 8;

        auto concat = [](std::initializer_list<const VectorWithMemoryTracking<size_t> *> parts)
        {
            VectorWithMemoryTracking<size_t> res;
            for (const auto * part : parts)
                res.insert(res.end(), part->begin(), part->end());
            return res;
        };

        PODArray<T> a_storage;
        PODArray<T> b_storage;
        const T * a_matrices = toContiguous(a, concat({&batch, &rows, &summation}), a_storage);
        const T * b_matrices = use_dot_products
            ? toContiguous(b, concat({&batch, &columns, &summation}), b_storage)
            : toContiguous(b, concat({&batch, &summation, &columns}), b_storage);

        auto result_indices = concat({&batch, &rows, &columns});
        PODArray<T> result_storage;
        T * result = output;
        if (result_indices != output_indices)
        {
            result_storage.assign(batch_size * rows_size * columns_size, T{});
            result = result_storage.data();
        }

        for (size_t i = 0; i < batch_size; ++i)
        {
            const T * a_matrix = a_matrices + i * rows_size * summation_size;
            const T * b_matrix = b_matrices + i * summation_size * columns_size;
            T * c_matrix = result + i * rows_size * columns_size;
            if (use_dot_products)
                multiplyMatricesTransposed(rows_size, columns_size, summation_size, a_matrix, b_matrix, c_matrix);
            else
                multiplyMatrices(rows_size, columns_size, summation_size, a_matrix, b_matrix, c_matrix);
        }

        if (result != output)
            contractWithLoopNest({StridedTensor<T>{result, contiguousStrides(result_indices)}}, output_indices, output);

        return true;
    }
};


/// Builds the column of the result from the column of its elements in the row-major order.
ColumnPtr wrapResult(MutableColumnPtr elements, const EinsumSignature & signature, const PaddedPODArray<UInt64> & output_dims, size_t input_rows_count)
{
    size_t rank = signature.spec.output.size();
    ColumnPtr res = std::move(elements);

    const auto * fixed_array_type = typeid_cast<const DataTypeFixedArray *>(signature.result_type.get());
    if (fixed_array_type || rank == 0)
    {
        for (size_t dim = rank; dim > 0; --dim)
            res = ColumnFixedArray::create(res, *signature.static_sizes[signature.spec.output[dim - 1]]);
        return res;
    }

    for (size_t dim = rank; dim > 0; --dim)
    {
        auto offsets_column = ColumnArray::ColumnOffsets::create();
        auto & offsets = offsets_column->getData();
        UInt64 offset = 0;
        for (size_t row = 0; row < input_rows_count; ++row)
        {
            const UInt64 * dims = output_dims.data() + row * rank;
            size_t num_arrays = 1;
            for (size_t outer = 0; outer + 1 < dim; ++outer)
                num_arrays *= dims[outer];
            for (size_t i = 0; i < num_arrays; ++i)
            {
                offset += dims[dim - 1];
                offsets.push_back(offset);
            }
        }
        res = ColumnArray::create(res, std::move(offsets_column));
    }
    return res;
}


class FunctionEinsum : public IFunction
{
public:
    static constexpr auto name = "einsum";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionEinsum>(); }

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isHigherOrderFunction() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0}; }
    bool useDefaultImplementationForConstants() const override { return true; }
    /// The tensors are processed directly, the sizes of FixedArray determine the type of the result.
    bool useDefaultImplementationForFixedArray() const override { return false; }

    void getLambdaArgumentTypes(DataTypes & arguments) const override
    {
        if (arguments.size() < 3)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} with a lambda function needs the subscripts, the lambda function and at least one tensor", getName());

        const auto * lambda_type = typeid_cast<const DataTypeFunction *>(arguments[1].get());
        if (!lambda_type)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "The lambda function must be the second argument of function {}, after the subscripts", getName());

        size_t num_tensors = arguments.size() - 2;
        if (lambda_type->getArgumentTypes().size() != num_tensors)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "The lambda function of function {} must have one argument for each of {} tensors, it has {}",
                getName(), num_tensors, lambda_type->getArgumentTypes().size());

        DataTypes element_types;
        for (size_t i = 0; i < num_tensors; ++i)
            element_types.push_back(getTensorType(arguments[i + 2]).element);

        arguments[1] = std::make_shared<DataTypeFunction>(element_types);
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        return analyze(arguments).result_type;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        EinsumSignature signature = analyze(arguments);
        size_t first_tensor = signature.has_combiner ? 2 : 1;
        size_t num_tensors = signature.tensors.size();

        VectorWithMemoryTracking<TensorRows> tensors;
        tensors.reserve(num_tensors);
        for (size_t i = 0; i < num_tensors; ++i)
            tensors.push_back(getTensorRows(
                arguments[first_tensor + i].column, signature.tensors[i].dims.size(), input_rows_count, first_tensor + i, getName()));

        if (signature.is_product)
        {
            WhichDataType which(signature.result_element_type);
            if (which.isFloat32())
                return executeNumeric<Float32, Float32>(signature, tensors, input_rows_count);
            if (which.isFloat64())
                return executeNumeric<Float64, Float64>(signature, tensors, input_rows_count);
            if (which.isInt64())
                return executeNumeric<Int64, UInt64>(signature, tensors, input_rows_count);
            return executeNumeric<UInt64, UInt64>(signature, tensors, input_rows_count);
        }

        return executeGeneric(signature, arguments, tensors, input_rows_count);
    }

private:
    EinsumSignature analyze(const ColumnsWithTypeAndName & arguments) const
    {
        if (arguments.size() < 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} needs the subscripts and at least one tensor, got {} arguments", getName(), arguments.size());

        if (typeid_cast<const DataTypeFunction *>(arguments[0].type.get()))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "The lambda form of function {} with the indices as the arguments of the lambda function is supported only by the analyzer. "
                "Use the subscripts, like einsum('ik,kj->ij', a, b), or enable the analyzer", getName());

        const auto * spec_column = arguments[0].column ? checkAndGetColumnConst<ColumnString>(arguments[0].column.get()) : nullptr;
        if (!spec_column)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                "The first argument of function {} must be a constant String with the subscripts, like 'ik,kj->ij'", getName());

        EinsumSignature signature;
        signature.spec = parseEinsumSpec(spec_column->getValue<String>());
        const auto & spec = signature.spec;

        const DataTypeFunction * combiner_type = typeid_cast<const DataTypeFunction *>(arguments[1].type.get());
        signature.has_combiner = combiner_type != nullptr;
        size_t first_tensor = signature.has_combiner ? 2 : 1;
        size_t num_tensors = arguments.size() - first_tensor;

        if (num_tensors != spec.inputs.size())
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "The subscripts '{}' of function {} are for {} tensors, but {} tensors are passed",
                spec_column->getValue<String>(), getName(), spec.inputs.size(), num_tensors);

        signature.static_sizes.assign(spec.numIndices(), std::nullopt);
        DataTypes element_types;
        for (size_t i = 0; i < num_tensors; ++i)
        {
            const auto & argument = arguments[first_tensor + i];
            auto tensor_type = getTensorType(argument.type);
            const auto & subscripts = spec.inputs[i];

            if (tensor_type.dims.size() != subscripts.size())
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Argument {} of function {} has type {} with {} dimensions, but the subscripts have {} indices for it",
                    first_tensor + i + 1, getName(), argument.type->getName(), tensor_type.dims.size(), subscripts.size());

            for (size_t dim = 0; dim < subscripts.size(); ++dim)
            {
                if (!tensor_type.dims[dim])
                    continue;
                auto & size = signature.static_sizes[subscripts[dim]];
                if (size && *size != *tensor_type.dims[dim])
                    throw Exception(ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH,
                        "Sizes of dimensions for index '{}' in function {} don't match: {} and {}",
                        spec.letters[subscripts[dim]], getName(), *size, *tensor_type.dims[dim]);
                size = tensor_type.dims[dim];
            }

            element_types.push_back(tensor_type.element);
            signature.tensors.push_back(std::move(tensor_type));
        }

        bool has_contraction = !spec.contracted.empty();
        if (signature.has_combiner)
        {
            signature.value_type = removeLowCardinality(combiner_type->getReturnType());
            signature.is_numeric = has_contraction;
            signature.result_element_type = has_contraction
                ? getAccumulatorType({signature.value_type}, getName())
                : signature.value_type;
        }
        else if (num_tensors == 1 && !has_contraction)
        {
            /// Only rearranges the elements: transposition, diagonal. Works for any type.
            signature.value_type = removeLowCardinality(element_types[0]);
            signature.result_element_type = signature.value_type;
        }
        else
        {
            signature.is_product = true;
            signature.is_numeric = true;
            signature.result_element_type = getAccumulatorType(element_types, getName());
            signature.value_type = signature.result_element_type;
        }

        /// The result is FixedArray if the sizes of all its dimensions are known.
        bool is_static = true;
        for (size_t index : spec.output)
            is_static &= signature.static_sizes[index].has_value();

        DataTypePtr result = signature.result_element_type;
        for (size_t dim = spec.output.size(); dim > 0; --dim)
        {
            size_t index = spec.output[dim - 1];
            if (is_static)
                result = std::make_shared<DataTypeFixedArray>(*signature.static_sizes[index], result);
            else
                result = std::make_shared<DataTypeArray>(result);
        }
        signature.result_type = result;
        return signature;
    }

    static PaddedPODArray<UInt64> getOutputDims(const EinsumSignature & signature, const VectorWithMemoryTracking<RowLayout> & layouts)
    {
        size_t rank = signature.spec.output.size();
        PaddedPODArray<UInt64> output_dims(layouts.size() * rank);
        for (size_t row = 0; row < layouts.size(); ++row)
            for (size_t dim = 0; dim < rank; ++dim)
                output_dims[row * rank + dim] = layouts[row].sizes[signature.spec.output[dim]];
        return output_dims;
    }

    /// The products of numbers. `T` is the type of the result, `ComputeT` is the type in which it is calculated.
    template <typename T, typename ComputeT>
    ColumnPtr executeNumeric(
        const EinsumSignature & signature,
        const VectorWithMemoryTracking<TensorRows> & tensors,
        size_t input_rows_count) const
    {
        static_assert(sizeof(T) == sizeof(ComputeT));
        const auto & spec = signature.spec;
        auto result_element_type = std::make_shared<DataTypeNumber<T>>();

        /// The elements of all tensors in the type of the result.
        VectorWithMemoryTracking<ColumnPtr> elements;
        for (size_t i = 0; i < tensors.size(); ++i)
            elements.push_back(castColumn({tensors[i].elements, signature.tensors[i].element, ""}, result_element_type));

        VectorWithMemoryTracking<RowLayout> layouts(input_rows_count);
        size_t total_output = 0;
        for (size_t row = 0; row < input_rows_count; ++row)
        {
            calculateRowLayout(spec, tensors, row, layouts[row], getName());
            total_output += layouts[row].num_output_elements;
        }

        auto result_column = ColumnVector<T>::create(total_output, T{});
        auto * result_data = reinterpret_cast<ComputeT *>(result_column->getData().data());

        size_t output_offset = 0;
        for (size_t row = 0; row < input_rows_count; ++row)
        {
            const auto & layout = layouts[row];
            VectorWithMemoryTracking<StridedTensor<ComputeT>> operands;
            for (size_t i = 0; i < tensors.size(); ++i)
            {
                const auto & data = assert_cast<const ColumnVector<T> &>(*elements[i]).getData();
                operands.push_back({reinterpret_cast<const ComputeT *>(data.data()) + tensors[i].offsets[tensors[i].row(row)], layout.strides[i]});
            }

            if (layout.num_output_elements && layout.num_contracted_terms)
                NumericContraction<ComputeT>(layout.sizes).contract(operands, spec.output, result_data + output_offset);
            output_offset += layout.num_output_elements;
        }

        return wrapResult(std::move(result_column), signature, getOutputDims(signature, layouts), input_rows_count);
    }

    /** The generic implementation: the elements of the tensors are gathered for blocks of terms,
      * then the combiner is applied to them (if any), and the results are summed over the contracted indices (if any).
      */
    ColumnPtr executeGeneric(
        const EinsumSignature & signature,
        const ColumnsWithTypeAndName & arguments,
        const VectorWithMemoryTracking<TensorRows> & tensors,
        size_t input_rows_count) const
    {
        const auto & spec = signature.spec;
        size_t num_tensors = tensors.size();
        size_t num_indices = spec.numIndices();

        const ColumnFunction * combiner = nullptr;
        ColumnPtr combiner_holder;
        if (signature.has_combiner)
        {
            combiner_holder = arguments[1].column->convertToFullColumnIfConst();
            combiner = typeid_cast<const ColumnFunction *>(combiner_holder.get());
            if (!combiner)
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "The second argument of function {} must be a lambda function", getName());
        }

        VectorWithMemoryTracking<RowLayout> layouts(input_rows_count);
        size_t total_output = 0;
        for (size_t row = 0; row < input_rows_count; ++row)
        {
            calculateRowLayout(spec, tensors, row, layouts[row], getName());
            total_output += layouts[row].num_output_elements;
        }

        /// The terms of a row are enumerated with the output indices first and the contracted indices last,
        /// so that the terms of each element of the result are adjacent.
        VectorWithMemoryTracking<size_t> loop_indices = spec.output;
        loop_indices.insert(loop_indices.end(), spec.contracted.begin(), spec.contracted.end());

        DataTypes element_types;
        for (const auto & tensor : signature.tensors)
            element_types.push_back(tensor.element);

        /// The elements of the result, or the sums for them.
        MutableColumnPtr result_elements = signature.result_element_type->createColumn();
        if (signature.is_numeric)
            result_elements->insertManyDefaults(total_output);

        /// The state of the enumeration of terms.
        size_t row = 0;
        size_t term_in_row = 0;
        size_t output_offset = 0;
        VectorWithMemoryTracking<size_t> counters(num_indices, 0);

        VectorWithMemoryTracking<ColumnUInt64::MutablePtr> positions(num_tensors);
        PaddedPODArray<UInt64> output_positions;
        IColumn::Offsets rows_offsets;

        auto skip_empty_rows = [&]
        {
            while (row < input_rows_count && layouts[row].num_output_elements * layouts[row].num_contracted_terms == 0)
            {
                output_offset += layouts[row].num_output_elements;
                ++row;
            }
        };

        skip_empty_rows();
        while (row < input_rows_count)
        {
            size_t first_row = row;
            for (auto & column : positions)
                column = ColumnUInt64::create();
            output_positions.clear();
            rows_offsets.clear();

            size_t block_terms = 0;
            while (row < input_rows_count && block_terms < einsum_block_size)
            {
                const auto & layout = layouts[row];
                size_t terms_in_row = layout.num_output_elements * layout.num_contracted_terms;
                size_t count = std::min(terms_in_row - term_in_row, einsum_block_size - block_terms);

                for (size_t term = 0; term < count; ++term)
                {
                    for (size_t tensor = 0; tensor < num_tensors; ++tensor)
                    {
                        UInt64 position = tensors[tensor].offsets[tensors[tensor].row(row)];
                        for (size_t index = 0; index < num_indices; ++index)
                            position += counters[index] * layout.strides[tensor][index];
                        positions[tensor]->getData().push_back(position);
                    }
                    output_positions.push_back(output_offset + (term_in_row + term) / layout.num_contracted_terms);

                    /// The next combination of indices, the last index changes fastest.
                    for (size_t i = loop_indices.size(); i > 0; --i)
                    {
                        size_t index = loop_indices[i - 1];
                        if (++counters[index] < layout.sizes[index])
                            break;
                        counters[index] = 0;
                    }
                }

                block_terms += count;
                term_in_row += count;
                rows_offsets.push_back(block_terms);

                if (term_in_row == terms_in_row)
                {
                    term_in_row = 0;
                    output_offset += layout.num_output_elements;
                    ++row;
                    skip_empty_rows();
                    for (size_t i = rows_offsets.size(); row > first_row + i; ++i)
                        rows_offsets.push_back(block_terms);
                }
            }

            ColumnsWithTypeAndName values;
            for (size_t tensor = 0; tensor < num_tensors; ++tensor)
                values.emplace_back(tensors[tensor].elements->index(*positions[tensor], block_terms), element_types[tensor], "");

            ColumnPtr value;
            if (combiner)
            {
                /// The captured columns of the lambda function are repeated for every term of their row.
                size_t num_rows_in_block = rows_offsets.size();
                auto replicated = IColumn::mutate(combiner->cut(first_row, num_rows_in_block)->replicate(rows_offsets));
                auto & replicated_function = typeid_cast<ColumnFunction &>(*replicated);
                replicated_function.appendArguments(values);
                value = replicated_function.reduce().column->convertToFullColumnIfConst()->convertToFullColumnIfLowCardinality();
            }
            else
            {
                value = values[0].column->convertToFullColumnIfLowCardinality();
            }

            if (!signature.is_numeric)
            {
                result_elements->insertRangeFrom(*value, 0, block_terms);
                continue;
            }

            accumulate(*value, signature, output_positions, *result_elements);
        }

        PaddedPODArray<UInt64> output_dims(input_rows_count * spec.output.size());
        for (size_t r = 0; r < input_rows_count; ++r)
            for (size_t dim = 0; dim < spec.output.size(); ++dim)
                output_dims[r * spec.output.size() + dim] = layouts[r].sizes[spec.output[dim]];

        return wrapResult(std::move(result_elements), signature, output_dims, input_rows_count);
    }

    /// Adds the values to the sums at the positions.
    static void accumulate(const IColumn & value, const EinsumSignature & signature, const PaddedPODArray<UInt64> & output_positions, IColumn & sums)
    {
        auto converted = castColumn({value.getPtr(), signature.value_type, ""}, signature.result_element_type);

        auto add = [&]<typename T>()
        {
            const auto & src = assert_cast<const ColumnVector<T> &>(*converted).getData();
            auto & dst = assert_cast<ColumnVector<T> &>(sums).getData();
            for (size_t i = 0; i < output_positions.size(); ++i)
            {
                auto & sum = dst[output_positions[i]];
                /// The overflow of integers wraps around, like in the numeric implementation.
                if constexpr (std::is_integral_v<T>)
                    sum = static_cast<T>(static_cast<UInt64>(sum) + static_cast<UInt64>(src[i]));
                else
                    sum += src[i];
            }
        };

        WhichDataType which(signature.result_element_type);
        if (which.isFloat32())
            add.template operator()<Float32>();
        else if (which.isFloat64())
            add.template operator()<Float64>();
        else if (which.isInt64())
            add.template operator()<Int64>();
        else
            add.template operator()<UInt64>();
    }
};

}

REGISTER_FUNCTION(Einsum)
{
    FunctionDocumentation::Description description = R"(
Einstein summation over tensors: arrays and multidimensional arrays (`Array` or `FixedArray`).

The function has two forms.

**The form with a lambda function of indices**: `einsum((i, j, ...) -> expression)`.
The arguments of the lambda function are the indices, and the expression uses them only as subscripts of arrays: `a[i][k]` or `a[i, k]`.
The range of each index is determined by the size of the arrays where it is used.
An index that occurs in two or more factors of a product (for example, `k` in `a[i][k] * b[k][j]`) is a summation index:
the result is the sum of the expression over all its values.
The other indices are the dimensions of the result, in the order of the arguments of the lambda function.
An index that is repeated in the subscripts of a single array (`a[i][i]`) takes the diagonal, without the summation.
The subscripts can also be constant: `image[i, j, 1]`.
This form is supported only with the analyzer, which rewrites it to the next form.

**The form with subscripts**, like in NumPy: `einsum('ik,kj->ij', a, b)`.
Each tensor gets as many letters as it has dimensions. The letters after `->` are the dimensions of the result,
and the result is the sum, over all the other letters, of the product of the elements of the tensors.
If `->` is omitted, the dimensions of the result are the letters that appear only once, in the alphabetical order.
A lambda function can be passed after the subscripts to combine the elements of the tensors instead of multiplying them:
`einsum('ij,ij->ij', (x, y) -> x - y, a, b)`.

The products and sums are calculated in `Float32` if all the numbers are `Float32`, `BFloat16` or small integers,
in `Float64` if there are other floating point numbers, and in `Int64` or `UInt64` for integers.
The result is a `FixedArray` if the sizes of all its dimensions are known from the types of the arguments, and an `Array` otherwise.
All arrays of the same dimension of a tensor must have the same size, otherwise an exception is thrown.
)";
    FunctionDocumentation::Syntax syntax = "einsum(lambda) | einsum(subscripts, [combiner,] tensor1[, tensor2, ...])";
    FunctionDocumentation::Arguments arguments = {
        {"lambda", "A lambda function with the indices as the arguments, whose expression uses the indices as array subscripts.", {"Lambda function"}},
        {"subscripts", "The subscripts of the tensors and of the result in the notation of NumPy, like `'ik,kj->ij'`.", {"const String"}},
        {"combiner", "Optional. A lambda function that combines the elements of the tensors instead of multiplying them.", {"Lambda function"}},
        {"tensor1, tensor2, ...", "The tensors.", {"Array", "FixedArray"}},
    };
    FunctionDocumentation::ReturnedValue returned_value = {"The resulting tensor, or a number if the result has no dimensions.", {"FixedArray", "Array", "(U)Int64", "Float*"}};
    FunctionDocumentation::Examples examples = {
        {"Matrix multiplication",
         "SELECT einsum((i, j, k) -> a[i][k] * b[k][j]) AS c, toTypeName(c)\n"
         "FROM (SELECT [[1, 2], [3, 4]]::FixedArray(2, 2, Float32) AS a, [[5, 6], [7, 8]]::FixedArray(2, 2, Float32) AS b)",
         "[[19,22],[43,50]]\tFixedArray(2, 2, Float32)"},
        {"Matrix multiplication with subscripts", "SELECT einsum('ik,kj->ij', [[1, 2], [3, 4]], [[5, 6], [7, 8]])", "[[19,22],[43,50]]"},
        {"Dot product", "SELECT einsum((i) -> a[i] * b[i]) FROM (SELECT [1, 2, 3] AS a, [4, 5, 6] AS b)", "32"},
        {"Transposition", "SELECT einsum((i, j) -> a[j][i]) FROM (SELECT [[1, 2, 3], [4, 5, 6]] AS a)", "[[1,4],[2,5],[3,6]]"},
        {"Diagonal and trace", "SELECT einsum((i) -> a[i][i]) AS diagonal, einsum('ii', a) AS trace FROM (SELECT [[1, 2], [3, 4]] AS a)", "[1,4]\t5"},
        {"Grayscale image",
         "SELECT einsum((i, j) -> 0.299 * image[i, j, 1] + 0.587 * image[i, j, 2] + 0.114 * image[i, j, 3]) AS gray\n"
         "FROM (SELECT [[[255, 0, 0], [0, 255, 0]]]::FixedArray(1, 2, 3, UInt8) AS image)",
         "[[76.24499999999999,149.685]]"},
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 11};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionEinsum>(documentation);
}

}
