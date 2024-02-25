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

#include <iostream>

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
    static void executeInternalScalar(
        const ColumnFloat64 & nested_vectors_data,
        const ColumnArray::Offsets & vectors_offsets,
        const ColumnFloat64 & nested_normals_data,
        const ColumnArray::Offsets & normals_offsets, 
        const IColumn * nested_offsets_data,
        size_t input_rows_count,
        size_t dimension,
        ColumnString::Chars & dst_chars,
        ColumnString::Offsets & dst_offsets)
    {
        size_t current_offset = 0;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            if (vectors_offsets[i] - current_offset != dimension)
                throw Exception(ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH, "All vectors must have equal size");

            size_t current_offset_normals = 0;
            for (size_t j = 0; j < input_rows_count; ++j)
            {
                if (normals_offsets[j] - current_offset_normals != dimension)
                    throw Exception(ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH, "All normals must have equal size");

                double res = 0.0;
                for (size_t k = 0; k < dimension; ++k)
                {
                    const size_t vector_index = current_offset + k;
                    const size_t normal_index = current_offset_normals + k;

                    res += (nested_vectors_data.getFloat64(vector_index) - nested_offsets_data->getFloat64(normal_index))
                            * nested_normals_data.getFloat64(normal_index);
                }
                dst_chars[input_rows_count * i + j] = (res == 0) ? '1' : '0';

                current_offset_normals = normals_offsets[j];
            }

            current_offset = vectors_offsets[i];
            dst_offsets[i] = input_rows_count * (i + 1);
        }
    }

    static void executeInternal(
        const ColumnFloat64 & nested_vectors_data,
        const ColumnArray::Offsets & vectors_offsets,
        const ColumnFloat64 & nested_normals_data,
        const ColumnArray::Offsets & normals_offsets, 
        const IColumn * nested_offsets_data,
        size_t input_rows_count,
        size_t dimension,
        ColumnString::Chars & dst_chars,
        ColumnString::Offsets & dst_offsets)
    {
        if (isArchSupported(TargetArch::AMXBF16))
        {
            // TODO
            return;
        }

        executeInternalScalar(
            nested_vectors_data,
            vectors_offsets,
            nested_normals_data,
            normals_offsets,
            nested_offsets_data,
            input_rows_count,
            dimension,
            dst_chars,
            dst_offsets);
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
        const auto & nested_vectors_data = typeid_cast<const ColumnFloat64 &>(vectors->getData());

        const ColumnArray * normals = typeid_cast<const ColumnArray *>(arguments[1].column.get());
        if (!normals)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Second argument of function {} must be Array", getName());
        const auto & nested_normals_data = typeid_cast<const ColumnFloat64 &>(normals->getData());

        const size_t vector_size = vectors->getOffsets().front();

        auto const_offsets = ColumnConst::create(ColumnFloat64::create(1, 0.0), input_rows_count * vector_size);
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

        auto col_res = ColumnString::create();
        auto & dst_chars = col_res->getChars();
        dst_chars.resize(input_rows_count * input_rows_count);
        auto & dst_offsets = col_res->getOffsets();
        dst_offsets.resize(input_rows_count);

        executeInternal(
            nested_vectors_data,
            vectors->getOffsets(),
            nested_normals_data,
            normals->getOffsets(),
            nested_offsets_data,
            input_rows_count,
            vector_size,
            dst_chars,
            dst_offsets);

        return col_res;
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
        .examples{{"partitionByHyperplanes", "SELECT(partitionByHyperplanes([1, 2, 3, 4, 5], [5, 4, 3, 2, -6]))", "\"1\""}},
        .categories{"OtherFunctions"}
    });
}

}
