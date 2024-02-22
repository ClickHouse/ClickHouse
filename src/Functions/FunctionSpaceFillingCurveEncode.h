#pragma once
#include <Functions/IFunction.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
}

class FunctionSpaceFillingCurveEncode: public IFunction
{
public:
    bool isVariadic() const override
    {
        return true;
    }

    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DB::DataTypes & arguments) const override
    {
        size_t vector_start_index = 0;
        if (arguments.empty())
            throw Exception(ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION,
                            "At least one UInt argument is required for function {}",
                            getName());
        if (WhichDataType(arguments[0]).isTuple())
        {
            vector_start_index = 1;
            const auto * type_tuple = typeid_cast<const DataTypeTuple *>(arguments[0].get());
            auto tuple_size = type_tuple->getElements().size();
            if (tuple_size != (arguments.size() - 1))
                throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                                "Illegal argument {} for function {}, tuple size should be equal to number of UInt arguments",
                                arguments[0]->getName(), getName());
            for (size_t i = 0; i < tuple_size; i++)
            {
                if (!WhichDataType(type_tuple->getElement(i)).isNativeUInt())
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                                    "Illegal type {} of argument in tuple for function {}, should be a native UInt",
                                    type_tuple->getElement(i)->getName(), getName());
            }
        }

        for (size_t i = vector_start_index; i < arguments.size(); i++)
        {
            const auto & arg = arguments[i];
            if (!WhichDataType(arg).isNativeUInt())
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                                "Illegal type {} of argument of function {}, should be a native UInt",
                                arg->getName(), getName());
        }
        return std::make_shared<DataTypeUInt64>();
    }
};

}
