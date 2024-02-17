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
#include "Columns/ColumnArray.h"
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
                {"vec", &isFloat<IDataType>, nullptr, "Froat"},
                {"normals", &isArray<IDataType>, isColumnConst, "const Array"},
            };

            FunctionArgumentDescriptors optional_args
            {
                {"offsets", &isFloat<IDataType>, isColumnConst, "const Array"},
            };

            validateFunctionArgumentTypes(*this, arguments, mandatory_args, optional_args);

            return std::make_shared<DataTypeString>();
        }

        ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
        {
            const ColumnPtr column = arguments[0].column;
            const ColumnFloat64 * vec = typeid_cast<const ColumnFloat64 *>(arguments[0].column.get());
            if (!vec)
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "First argument of function {} must be Int64", getName());

            const ColumnArray * normals = typeid_cast<const ColumnArray *>(arguments[1].column.get());

            const ColumnFloat64 * offsets = typeid_cast<const ColumnFloat64 *>(arguments[0].column.get());
            if (!offsets)
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Third argument of function {} must be Int64", getName());

            auto col_res = ColumnFixedString::create(input_rows_count);
            auto& dst_chars = col_res->getChars();
            dst_chars.resize(input_rows_count);
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                double res = 0.0;
                auto normal = (*normals)[i].get<Array>();
                for (size_t j = 0; j < input_rows_count; ++j)
                {
                    res += ((*vec)[i].get<Float64>() - (*offsets)[i].get<Float64>()) * normal[j].get<Float64>();
                }
                dst_chars[i] = res == 0;
            }
            return ColumnConst::create(std::move(col_res), input_rows_count);
        }
    };

}

REGISTER_FUNCTION(PartitionByHyperplanes)
{
    factory.registerFunction<FunctionPartitionByHyperplanes>();
}

}
