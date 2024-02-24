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
#include "Columns/ColumnFixedString.h"
#include "Columns/ColumnsNumber.h"
#include "Core/TypeId.h"
#include "DataTypes/IDataType.h"
#include "config.h"


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
    public:
        static constexpr auto name = "PartitionByHyperplanes";
        static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionPartitionByHyperplanes>(); }

        String getName() const override { return name; }

        bool isVariadic() const override { return false; }
        bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

        size_t getNumberOfArguments() const override { return 3; }
        bool useDefaultImplementationForConstants() const override { return true; }

        DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
        {
            FunctionArgumentDescriptors mandatory_args
            {
                {"vectors", &isArray<IDataType>, nullptr, "Array"},
                {"normals", &isArray<IDataType>, isColumnConst, "const Array"},
            };

            FunctionArgumentDescriptors optional_args
            {
                {"offsets", &isArray<IDataType>, isColumnConst, "const Array"},
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

            const size_t size = vectors->getOffsets().front();

            auto nested_offsets_data = [&] {
                if (arguments.size() < 3)
                    return ColumnConst::create(ColumnFloat64::create(1, 0.0), input_rows_count * size)->getDataColumnPtr();

                const ColumnArray * offsets = typeid_cast<const ColumnArray *>(arguments[2].column.get());
                if (!offsets)
                    throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Third argument of function {} must be Array", getName());
                return offsets->getDataPtr();
            }();

            if (input_rows_count == 0)
                return result_type->createColumn();

            auto col_res = ColumnFixedString::create(size);
            auto& dst_chars = col_res->getChars();
            dst_chars.resize(input_rows_count * input_rows_count);

            size_t current_offset = 0;
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                if (vectors->getOffsets()[i] - current_offset != size)
                    throw Exception(ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH, "All vectors must have equal size");

                size_t current_offset_normals = 0;
                for (size_t j = 0; j < input_rows_count; ++j)
                {
                    if (normals->getOffsets()[j] - current_offset_normals != size)
                        throw Exception(ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH, "All normals must have equal size");

                    double res = 0.0;
                    for (size_t k = 0; k < size; ++k)
                    {
                        const size_t vector_index = current_offset + k;
                        const size_t normal_index = current_offset_normals + k;

                        res += (nested_vectors_data.getFloat64(vector_index) - nested_offsets_data->getFloat64(vector_index))
                                * nested_normals_data.getFloat64(normal_index);
                    }
                    dst_chars[current_offset + j] = (res == 0);

                    current_offset_normals = normals->getOffsets()[j];
                }

                current_offset = vectors->getOffsets()[i];
            }

            return col_res;
        }
    };

}

REGISTER_FUNCTION(PartitionByHyperplanes)
{
    factory.registerFunction<FunctionPartitionByHyperplanes>(FunctionDocumentation{
        .description=R"(
Given a vector and a hyperplanes, represented as vectors of normals and optioal offsets.
Returns a String with every bit corresponding to a subspace, relative to the corresponding hyperplane.
All the vectors an normals should lie in space of the same dimension.
        )",
        .examples{{"partitionByHyperplanes", "SELECT(partitionByHyperplanes([1, 2, 3, 4, 5], [5, 4, 3, 2, -38]))", "\"1\""}},
        .categories{"OtherFunctions"}
    });
}

}
