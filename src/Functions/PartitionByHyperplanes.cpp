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
                {"vec", &isArray<IDataType>, nullptr, "Array"},
                {"normals", &isString<IDataType>, isColumnConst, "const Array"},
            };

            FunctionArgumentDescriptors optional_args
            {
                {"offsets", &isString<IDataType>, isColumnConst, "const Array"},
            };

            validateFunctionArgumentTypes(*this, arguments, mandatory_args, optional_args);

            return std::make_shared<DataTypeString>();
        }

        ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
        {
            const ColumnPtr column = arguments[0].column;
            const ColumnArray * vec = typeid_cast<const ColumnArray *>(arguments[0].column.get());
            if (!vec)
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "First argument of function {} must be array", getName());

            
        }
    };

}

REGISTER_FUNCTION(PartitionByHyperplanes)
{
    factory.registerFunction<FunctionPartitionByHyperplanes>();
}

}
