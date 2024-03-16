#include <cstddef>
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
#include "Common/FunctionDocumentation.h"
#include "Columns/ColumnArray.h"
#include "Columns/ColumnConst.h"
#include "Columns/ColumnsNumber.h"
#include "Columns/IColumn.h"
#include "Core/TypeId.h"
#include "DataTypes/IDataType.h"
#include "config.h"

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

class FunctionPartitionByHyperplanes : public IFunction
{
private:
    static size_t constexpr tile_size = 4;
#if defined(__AMX_BF16__)
    static std::array<double, tile_size * tile_size> temp_tile;
#endif

    template <class ColumnType, class ResultColumnType>
    static void multiplyTileScalar(
        const ColumnType & nested_vectors_data,
        const ColumnType & nested_normals_data,
        const IColumn * nested_offsets_data,
        ResultColumnType & col_res,
        size_t input_rows_count,
        size_t dimension,
        size_t vectors_data_index,
        size_t normals_data_index,
        size_t coordinate_index)
    {
        for (size_t nt = vectors_data_index; nt < vectors_data_index + tile_size && nt < input_rows_count; ++nt)
            for (size_t mt = normals_data_index; mt < normals_data_index + tile_size && mt < input_rows_count; ++mt)
                for (size_t kt = coordinate_index; kt < coordinate_index + tile_size && kt < dimension; ++kt)
                {
                    auto vector_element = nested_vectors_data.getFloat32(nt * dimension + kt);
                    auto normal_element = nested_normals_data.getFloat32(mt * dimension + kt);
                    auto offset_element = nested_offsets_data->getFloat32(mt * dimension + kt);

                    col_res.getElement(nt * input_rows_count + mt) += (vector_element - offset_element) * normal_element;
                }
    }

#if defined(__AMX_BF16__)
    template <class ColumnType>
    static void subVectorOffsetTile(
        const ColumnType & nested_vectors_data,
        const IColumn * nested_offsets_data,
        size_t input_rows_count,
        size_t dimension,
        size_t vectors_data_index,
        size_t offsets_data_index,
        size_t coordinate_index)
    {
        for (size_t i = 0; i < tile_size && vectors_data_index + i < input_rows_count && offsets_data_index + i < input_rows_count; ++i)
            for (size_t j = 0; j < tile_size && coordinate_index + j < dimension; ++j)
            {
                auto vector_element = nested_vectors_data.getFloat64((vectors_data_index + i) * dimension + coordinate_index + j);
                auto offset_element = nested_offsets_data->getFloat64((offsets_data_index + i) * dimension + coordinate_index + j);

                temp_tile[i * tile_size + j] = vector_element - offset_element;
            }
    }

    template <class ColumnType>
    static void amxTileMultiply()
    {
        if constexpr (std::is_same_v<ColumnType, ColumnFloat32>)
            _tile_dpbf16ps(0, 1, 2);
        else if (std::is_same_v<ColumnType, ColumnInt8>)
            _tile_dpbssd(0, 1, 2);
    }

    struct TileConfig
    {
        uint8_t palette_id;
        uint8_t start_row;
        uint8_t reserved_0[14];
        uint16_t colsb[16]; 
        uint8_t rows[16]; 
    };

    template <class ColumnType, class ResultColumnType>
    static void multiplyTileAMX(
        const ColumnType & nested_vectors_data,
        const ColumnType & nested_normals_data,
        const IColumn * nested_offsets_data,
        ResultColumnType & col_res,
        size_t input_rows_count,
        size_t dimension,
        size_t vectors_data_index,
        size_t normals_data_index,
        size_t coordinate_index)
    {
        subVectorOffsetTile(
            nested_vectors_data,
            nested_offsets_data,
            input_rows_count,
            dimension,
            vectors_data_index,
            normals_data_index);

        TileConfig tileinfo{
            .palette_id=1,
            .start_row=0,
        };
        tileinfo.rows[0] = tile_size;
        tileinfo.colsb[0] = tile_size;

        tileinfo.rows[1] = tile_size;
        tileinfo.colsb[1] = tile_size;

        tileinfo.rows[2] = tile_size;
        tileinfo.colsb[2] = tile_size;

        _tile_loadconfig(&tileinfo);

        _tile_loadd(
            0,
            col_res.getData().data() + vectors_data_index * input_rows_count + normals_data_index,
            input_rows_count * col_res.sizeOfValueIfFixed());
        _tile_loadd(
            1,
            temp_tile.data(),
            tile_size * nested_vectors_data.sizeOfValueIfFixed());
        _tile_loadd(
            2,
            nested_normals_data.getData().data() + normals_data_index * dimension + coordinate_index,
            dimension * nested_normals_data.sizeOfValueIfFixed());
        amxTileMultiply<ColumnType>();
        _tile_stored(
            0,
            col_res.getData().data() + vectors_data_index * input_rows_count + normals_data_index,
            input_rows_count * col_res.sizeOfValueIfFixed());
        _tile_release();
    }
#endif

    template <class ColumnType, class ResultColumnType>
    static void multiplyTile(
        const ColumnType & nested_vectors_data,
        const ColumnType & nested_normals_data,
        const IColumn * nested_offsets_data,
        ResultColumnType & col_res,
        size_t input_rows_count,
        size_t dimension,
        size_t vectors_data_index,
        size_t normals_data_index,
        size_t coordinate_index)
    {
#if defined(__AMX_BF16__) && defined(__AMX_TILE__)
        if (isArchSupported(TargetArch::AMXBF16) && isArchSupported(TargetArch::AMXTILE))
        {
            multiplyTileAMX(
                nested_vectors_data,
                nested_normals_data,
                nested_offsets_data,
                col_res,
                input_rows_count,
                dimension,
                vectors_data_index,
                normals_data_index,
                coordinate_index);
            return;
        }
#endif
        multiplyTileScalar(
            nested_vectors_data,
            nested_normals_data,
            nested_offsets_data,
            col_res,
            input_rows_count,
            dimension,
            vectors_data_index,
            normals_data_index,
            coordinate_index);
    }

    template <class ColumnType, class ResultColumnType>
    static void executeInternal(
        const ColumnType & nested_vectors_data,
        const ColumnType & nested_normals_data,
        const IColumn * nested_offsets_data,
        size_t input_rows_count,
        size_t dimension,
        ResultColumnType & col_res)
    {
        for (size_t i = 0; i < input_rows_count; i += tile_size)
        {
            for (size_t j = 0; j < input_rows_count; j += tile_size)
            {
                for (size_t k = 0; k < dimension; k += tile_size)
                {
                    multiplyTile(
                        nested_vectors_data,
                        nested_normals_data,
                        nested_offsets_data,
                        col_res,
                        input_rows_count,
                        dimension,
                        i,
                        j,
                        k);
                }
            }
        }
    }

    static void checkDimension(const ColumnArray::Offsets & offsets, size_t dimension)
    {
        size_t prev_offset = 0;
        for (const auto offset : offsets) {
            if (offset - prev_offset != dimension) {
                throw Exception(ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH, "All vectors must have equal size");
            }
            prev_offset = offset;
        }
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
            {"vectors", &isArray<IDataType>, nullptr, "Array"},
            {"normals", &isArray<IDataType>, nullptr, "Array"},
        };

        FunctionArgumentDescriptors optional_args{
            {"offsets", &isArray<IDataType>, nullptr, "Array"},
        };

        validateFunctionArgumentTypes(*this, arguments, mandatory_args, optional_args);

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const ColumnArray * vectors = typeid_cast<const ColumnArray *>(arguments[0].column.get());
        if (!vectors)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "First argument of function {} must be Array", getName());
        const auto & nested_vectors_data = typeid_cast<const ColumnFloat32 &>(vectors->getData());

        const ColumnArray * normals = typeid_cast<const ColumnArray *>(arguments[1].column.get());
        if (!normals)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Second argument of function {} must be Array", getName());
        const auto & nested_normals_data = typeid_cast<const ColumnFloat32 &>(normals->getData());

        const size_t vector_size = vectors->getOffsets().front();

        auto const_offsets = ColumnConst::create(ColumnFloat32::create(1, 0), input_rows_count * vector_size);
        const IColumn * nested_offsets_data = const_offsets.get();
        if (arguments.size() >= 3)
        {
            const ColumnArray * offsets = typeid_cast<const ColumnArray *>(arguments[2].column.get());
            if (!offsets)
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Third argument of function {} must be Array", getName());
            nested_offsets_data = &offsets->getData();
        }

        if (input_rows_count == 0)
            return result_type->createColumn();

        checkDimension(vectors->getOffsets(), vector_size);
        checkDimension(normals->getOffsets(), vector_size);

        auto col_res = ColumnFloat32::create(input_rows_count * input_rows_count);
        auto col_res_offsets = ColumnArray::ColumnOffsets::create();
        col_res_offsets->reserve(input_rows_count);
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            col_res_offsets->insertValue(input_rows_count * i);
        }

        executeInternal(
            nested_vectors_data,
            nested_normals_data,
            nested_offsets_data,
            input_rows_count,
            vector_size,
            *col_res);

        return ColumnArray::create(std::move(col_res), std::move(col_res_offsets));
    }
};

}

REGISTER_FUNCTION(partitionByHyperplanes)
{
    factory.registerFunction<FunctionPartitionByHyperplanes>(FunctionDocumentation{
        .description=R"(
Given a vector and a hyperplanes, represented as vectors of normals and optioal offsets.
Returns a String with every bit corresponding to a subspace, relative to the corresponding hyperplane.
All the vectors an normals should lie in space of the same dimension.
        )",
        .examples{{"partitionByHyperplanes", "SELECT(partitionByHyperplanes([1., 2., 3., 4., 5.], [5., 4., 3., 2., -6.]))", "[1]"}},
        .categories{"OtherFunctions"}
    });
}

}
