#include <cstddef>
#include <stdexcept>
#include <string>
#include <sys/syscall.h>
#include <base/types.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Common/FunctionDocumentation.h>
#include <Common/logger_useful.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Core/TypeId.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/IDataType.h>
#include <config.h>


#if defined(__AMX_BF16__)
#include <immintrin.h>
#endif

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int SIZES_OF_ARRAYS_DONT_MATCH;
}

namespace
{

size_t constexpr tile_size = 16;

DECLARE_DEFAULT_CODE(
    void doMultiplyTile(
        const ColumnFloat32 & nested_vectors_data,
        const ColumnFloat32 & nested_normals_data,
        ColumnFloat32 & col_res,
        size_t vector_count,
        size_t normal_count,
        size_t dimension,
        size_t vectors_data_index,
        size_t normals_data_index,
        size_t coordinate_index)
    {
        for (size_t nt = vectors_data_index; nt < vectors_data_index + tile_size && nt < vector_count; ++nt)
            for (size_t mt = normals_data_index; mt < normals_data_index + tile_size && mt < normal_count; ++mt)
                for (size_t kt = coordinate_index; kt < coordinate_index + tile_size && kt < dimension; ++kt)
                {
                    auto vector_element = nested_vectors_data.getFloat32(nt * dimension + kt);
                    auto normal_element = nested_normals_data.getFloat32(mt * dimension + kt);

                    col_res.getElement(nt * normal_count + mt) += vector_element * normal_element;
                }
    }
) // DECLARE_DEFAULT_CODE

DECLARE_AMXBF16_SPECIFIC_CODE(
    static std::vector<uint16_t> bufA(2 * tile_size * tile_size), bufB(2 * tile_size * tile_size);

    // Config for AMX unit
    struct TileConfig
    {
        uint8_t palette_id; // must be 1
        uint8_t start_row; // must be 0
        uint8_t reserved_0[14];
        uint16_t colsb[16]; // actual row length in bytes
        uint8_t rows[16]; // actual row count
    };

    void doMultiplyConvertedTile(size_t K, const uint16_t* A0, const uint16_t* A1,
        const uint16_t* B0, const uint16_t* B1,
        float* C, size_t ldc)
    {
        _tile_stream_loadd(0, C, ldc * 4);
        _tile_stream_loadd(1, C + 16, ldc * 4);
        _tile_stream_loadd(2, C + 16 * ldc, ldc * 4);
        _tile_stream_loadd(3, C + 16 * ldc + 16, ldc * 4);

        for (size_t k = 0; k < K; k += tile_size)
        {
            _tile_stream_loadd(4, A0 + k * 16, 64);
            _tile_stream_loadd(5, A1 + k * 16, 64);
            _tile_loadd(6, B0 + k * 16, 64);
            _tile_loadd(7, B1 + k * 16, 64);
            _tile_dpbf16ps(0, 4, 6);
            _tile_dpbf16ps(1, 4, 7);
            _tile_dpbf16ps(2, 5, 6);
            _tile_dpbf16ps(3, 5, 7);
        }

        _tile_stored(0, C, ldc * 4);
        _tile_stored(1, C + 16, ldc * 4);
        _tile_stored(2, C + 16 * ldc, ldc * 4);
        _tile_stored(3, C + 16 * ldc + 16, ldc * 4);
    }

    void convert(const float* src, uint16_t* dst)
    {
        __m512 s0 = _mm512_loadu_ps(src + 0 * 16);
        __m512 s1 = _mm512_loadu_ps(src + 1 * 16);
        _mm512_storeu_si512(dst, _mm512_cvtne2ps_pbh(s1, s0));
    }

    void convert(size_t K, const float* A, size_t lda, uint16_t* buf)
    {
        for (size_t k = 0; k < K; k += tile_size, A += tile_size)
            for (size_t i = 0; i < 16; ++i, buf += tile_size)
                convert(A + i * lda, buf);
    }
    void doMultiplyTile(
        const ColumnFloat32 & nested_vectors_data,
        const ColumnFloat32 & nested_normals_data,
        ColumnFloat32 & col_res,
        size_t vector_count,
        size_t normal_count,
        size_t dimension,
        size_t vectors_data_index,
        size_t normals_data_index,
        size_t coordinate_index)
    {
        // Should be executed once
        static auto load_config = [&] {
            TileConfig tileinfo;
            tileinfo.palette_id = 1;
            tileinfo.start_row = 0;
            tileinfo.rows[0] = tile_size * 4;
            tileinfo.colsb[0] = tile_size;

            tileinfo.rows[1] = tile_size * 4;
            tileinfo.colsb[1] = tile_size;

            tileinfo.rows[2] = tile_size * 4;
            tileinfo.colsb[2] = tile_size;

            _tile_loadconfig(&tileinfo);

            const int ARCH_REQ_XCOMP_PERM = 0x1023;
            const int XFEATURE_XTILEDATA = 18;
            syscall(SYS_arch_prctl, ARCH_REQ_XCOMP_PERM, XFEATURE_XTILEDATA);

            return 0;
        }();
        (void)load_config;

        auto* vector_data = nested_vectors_data.getData().data() + vectors_data_index * dimension + coordinate_index;
        auto* normals_data = nested_normals_data.getData().data() + normals_data_index * dimension + coordinate_index;

        convert(dimension, normals_data + 0, vector_count, bufB.data());
        convert(dimension, normals_data + tile_size, vector_count, bufB.data() + tile_size * tile_size);
        convert(dimension, vector_data, normal_count, bufA.data());
        convert(dimension, vector_data + tile_size, normal_count, bufA.data() + tile_size * tile_size);

        doMultiplyConvertedTile(
            dimension,
            bufA.data(), bufA.data() + tile_size * tile_size,
            bufB.data(), bufB.data() + tile_size * tile_size,
            col_res.getData().data() + vectors_data_index * normal_count + normals_data_index,
            normal_count);
        _tile_release();
    }
) // DECLARE_AMX_SPECIFIC_CODE

DECLARE_AVX2_SPECIFIC_CODE(
    void doMultiplyTile(
        const ColumnFloat32 & nested_vectors_data,
        const ColumnFloat32 & nested_normals_data,
        ColumnFloat32 & col_res,
        size_t vector_count,
        size_t normal_count,
        size_t dimension,
        size_t vectors_data_index,
        size_t normals_data_index,
        size_t coordinate_index)
    {
        for (size_t nt = vectors_data_index; nt < vectors_data_index + tile_size && nt < vector_count; ++nt)
            for (size_t mt = normals_data_index; mt < normals_data_index + tile_size && mt < normal_count; mt += 8)
            {
                __m256 sum = _mm256_setzero_ps();
                for (size_t kt = coordinate_index; kt < coordinate_index + tile_size && kt < dimension; ++kt)
                {
                    __m256 va = _mm256_broadcast_ss(&nested_vectors_data.getData()[nt * dimension + kt]);
                    __m256 vb = _mm256_loadu_ps(&nested_normals_data.getData()[mt * dimension + kt]);
                    sum = _mm256_fmadd_ps(va, vb, sum);
                }
                _mm256_storeu_ps(&col_res.getData()[nt * normal_count + mt], sum);
            }
    }

) // DECLARE_AVX2_SPECIFIC_CODE

DECLARE_AVX512BW_SPECIFIC_CODE(
    void doMultiplyTile(
        const ColumnFloat32 & nested_vectors_data,
        const ColumnFloat32 & nested_normals_data,
        ColumnFloat32 & col_res,
        size_t vector_count,
        size_t normal_count,
        size_t dimension,
        size_t vectors_data_index,
        size_t normals_data_index,
        size_t coordinate_index)
    {
        for (size_t nt = vectors_data_index; nt < vectors_data_index + tile_size && nt < vector_count; ++nt)
            for (size_t mt = normals_data_index; mt < normals_data_index + tile_size && mt < normal_count; mt += 16)
            {
                __m512 sum = _mm512_setzero_ps();
                for (size_t kt = coordinate_index; kt < coordinate_index + tile_size && kt < dimension; ++kt)
                {
                    __m256 va256 = _mm256_broadcast_ss(&nested_vectors_data.getData()[nt * dimension + kt]);
                    __m512 va = _mm512_broadcast_f32x8(va256);
                    __m512 vb = _mm512_loadu_ps(&nested_normals_data.getData()[mt * dimension + kt]);
                    sum = _mm512_fmadd_ps(va, vb, sum);
                }
                _mm512_storeu_ps(&col_res.getData()[nt * normal_count + mt], sum);
            }
    }

) // DECLARE_AVX512_SPECIFIC_CODE

class FunctionPartitionByHyperplanes : public IFunction
{
private:
    static void multiplyTile(
        const ColumnFloat32 & nested_vectors_data,
        const ColumnFloat32 & nested_normals_data,
        ColumnFloat32 & col_res,
        size_t vector_count,
        size_t normal_count,
        size_t dimension,
        size_t vectors_data_index,
        size_t normals_data_index,
        size_t coordinate_index)
    {
#if USE_MULTITARGET_CODE
        if (isArchSupported(TargetArch::AMXBF16))
        {
            TargetSpecific::AMXBF16::doMultiplyTile(
                nested_vectors_data,
                nested_normals_data,
                col_res,
                vector_count,
                normal_count,
                dimension,
                vectors_data_index,
                normals_data_index,
                coordinate_index);
            return;
        }

        if (isArchSupported(TargetArch::AVX512BW))
        {
            TargetSpecific::AVX512BW::doMultiplyTile(
                nested_vectors_data,
                nested_normals_data,
                col_res,
                vector_count,
                normal_count,
                dimension,
                vectors_data_index,
                normals_data_index,
                coordinate_index);
            return;
        }

        if (isArchSupported(TargetArch::AVX2))
        {
            TargetSpecific::AVX2::doMultiplyTile(
                nested_vectors_data,
                nested_normals_data,
                col_res,
                vector_count,
                normal_count,
                dimension,
                vectors_data_index,
                normals_data_index,
                coordinate_index);
            return;
        }
#endif

        TargetSpecific::Default::doMultiplyTile(
            nested_vectors_data,
            nested_normals_data,
            col_res,
            vector_count,
            normal_count,
            dimension,
            vectors_data_index,
            normals_data_index,
            coordinate_index);
    }

    /*
    The implementation uses tiled matrix multiplication.
    A tile is a part of a matrix of size tile_size x tile_size.
    The algorithm iterates over the tiles of the first and second matrix.
    Classical matrix multiplication is used inside each tile, but since the size of the matrices inside the tile is small,
    both tiles can fit entirely in the cache, which significantly reduces the number of cache misses.
    */
    static void executeInternal(
        const ColumnFloat32 & nested_vectors_data,
        const ColumnFloat32 & nested_normals_data,
        size_t vector_count,
        size_t normal_count,
        size_t dimension,
        ColumnFloat32 & col_res)
    {
        for (size_t i = 0; i < vector_count; i += tile_size)
        {
            for (size_t j = 0; j < normal_count; j += tile_size)
            {
                for (size_t k = 0; k < dimension; k += tile_size)
                    multiplyTile(
                        nested_vectors_data,
                        nested_normals_data,
                        col_res,
                        vector_count,
                        normal_count,
                        dimension,
                        i,
                        j,
                        k);
            }
        }
    }

    static void checkDimension(const ColumnArray::Offsets & offsets, size_t dimension)
    {
        size_t prev_offset = 0;
        for (const auto offset : offsets)
        {
            if (offset - prev_offset != dimension)
                throw Exception(ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH, "All vectors must have equal size");
            prev_offset = offset;
        }
    }

    static ColumnPtr createFixedStringResult(
        const IColumn * nested_offsets_data,
        const ColumnFloat32::MutablePtr & col_res,
        size_t vector_count,
        size_t normal_count)
    {
        auto res = ColumnFixedString::create((normal_count / 8) + (normal_count % 8 != 0));
        auto& res_chars = res->getChars();
        res_chars.reserve(vector_count * normal_count / 8);
        char temp = 0;
        for (size_t i = 0; i < vector_count; ++i)
        {
            for (size_t j = 0; j < normal_count; ++j)
            {
                temp = temp << 1;
                auto res_val = col_res->getData()[i * normal_count + j];
                auto offset = nested_offsets_data->getFloat32(j);
                if (res_val > offset)
                    temp |= 1;
                if ((j + 1) % 8 == 0)
                {
                    res_chars.push_back(temp);
                    temp = 0;
                }
            }
            if (normal_count % 8 != 0)
            {
                res_chars.push_back(temp);
                temp = 0;
            }
        }
        return res;
    }

public:
    static constexpr auto name = "partitionByHyperplanes";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionPartitionByHyperplanes>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args{
            {"vectors", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isArray), nullptr, "Array"},
            {"normals", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isArray), isColumnConst, "const Array"},
        };

        FunctionArgumentDescriptors optional_args{
            {"offsets", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isArray), isColumnConst, "const Array"},
        };

        validateFunctionArgumentTypes(*this, arguments, mandatory_args, optional_args);

        return std::make_shared<DataTypeFixedString>(1);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t /*input_rows_count*/) const override
    {
        const ColumnArray * vectors = typeid_cast<const ColumnArray *>(arguments[0].column.get());
        if (!vectors)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "First argument of function {} must be Array", getName());
        const auto & nested_vectors_data = typeid_cast<const ColumnFloat32 &>(vectors->getData());

        const size_t dimension = vectors->getOffsets().front();
        const size_t vector_count = vectors->getOffsets().size();

        const ColumnArray * normals = typeid_cast<const ColumnArray *>(arguments[1].column.get());
        if (!normals)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Second argument of function {} must be Array", getName());
        size_t normal_count = normals->getOffsets().size();
        const auto & nested_normals_data = [&] -> const DB::ColumnVector<Float32> &
        {
            if (normals->getData().getDataType() == TypeIndex::Array)
            {
                const auto & normals_data = typeid_cast<const ColumnArray &>(normals->getData());
                normal_count = normals_data.getOffsets().size();
                return typeid_cast<const ColumnFloat32 &>(normals_data.getData());
            }
            return typeid_cast<const ColumnFloat32 &>(normals->getData());
        }();

        auto offsets = ColumnConst::create(ColumnFloat32::create(1, 0), normal_count);
        const IColumn * nested_offsets_data = offsets.get();
        if (arguments.size() >= 3)
        {
            const ColumnConst * offsets_const = typeid_cast<const ColumnConst *>(arguments[2].column.get());
            if (!offsets_const)
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Third argument of function {} must be Array", getName());
            nested_offsets_data = offsets_const;
        }

        if (vector_count == 0)
            return result_type->createColumn();

        checkDimension(vectors->getOffsets(), dimension);
        checkDimension(normals->getOffsets(), dimension);

        auto col_res = ColumnFloat32::create(vector_count * normal_count);
        executeInternal(
            nested_vectors_data,
            nested_normals_data,
            vector_count,
            normal_count,
            dimension,
            *col_res);

        return createFixedStringResult(nested_offsets_data, col_res, vector_count, normal_count);
    }
};

}

REGISTER_FUNCTION(partitionByHyperplanes)
{
        factory.registerFunction<FunctionPartitionByHyperplanes>(FunctionDocumentation{
        .description=R"(
This function partitions a given point in vector space based on its position relative to one or more defined hyperplanes.
A hyperplane in an N-dimensional space is defined by a normal vector and an optional offset from the origin.
The function takes a vector representing a point and arrays of normal vectors (and optional offsets) representing the hyperplanes.
It returns a String, where each bit indicates the side of the corresponding hyperplane on which the point lies, with '0' indicating one side and '1' the other.
This method allows for efficient classification of points into multiple regions defined by these hyperplanes, suitable for complex geometric, spatial analysis, and machine learning applications.
Ensure all vectors and normals are in the same dimensional space for accurate results.
        )",
        .examples={{"partitionByHyperplanes", "SELECT partitionByHyperplanes([2.0, 3.0], [[1.0, -1.0], [-1.0, 2.0]], [[0.0, 0.0], [1.0, 0.0]])", ""}},
        .categories{"Geometric", "Classification"}
    });
}

}
