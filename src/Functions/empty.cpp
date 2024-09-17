#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringOrArrayToT.h>
#include <Functions/EmptyImpl.h>
#include <Columns/ColumnObject.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

struct NameEmpty
{
    static constexpr auto name = "empty";
};

using FunctionEmpty = FunctionStringOrArrayToT<EmptyImpl<false>, NameEmpty, UInt8, false>;

/// Implements the empty function for JSON type.
class ExecutableFunctionJSONEmpty : public IExecutableFunction
{
public:
    std::string getName() const override { return NameEmpty::name; }

private:
    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const override
    {
        const ColumnWithTypeAndName & elem = arguments[0];
        const auto * object_column = typeid_cast<const ColumnObject *>(elem.column.get());
        if (!object_column)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected column type in function {}. Expected Object column, got {}", getName(), elem.column->getName());

        auto res = DataTypeUInt8().createColumn();
        auto & data = typeid_cast<ColumnUInt8 &>(*res).getData();
        const auto & typed_paths = object_column->getTypedPaths();
        size_t size = object_column->size();
        /// If object column has at least 1 typed path, it will never be empty, because these paths always have values.
        if (!typed_paths.empty())
        {
            data.resize_fill(size, 0);
            return res;
        }

        const auto & dynamic_paths = object_column->getDynamicPaths();
        const auto & shared_data = object_column->getSharedDataPtr();
        data.reserve(size);
        for (size_t i = 0; i != size; ++i)
        {
            bool empty = true;
            /// Check if there is no paths in shared data.
            if (!shared_data->isDefaultAt(i))
            {
                empty = false;
            }
            /// Check that all dynamic paths have NULL value in this row.
            else
            {
                for (const auto & [path, column] : dynamic_paths)
                {
                    if (!column->isNullAt(i))
                    {
                        empty = false;
                        break;
                    }
                }
            }

            data.push_back(empty);
        }

        return res;
    }
};

class FunctionEmptyJSON final : public IFunctionBase
{
public:
    FunctionEmptyJSON(const DataTypes & argument_types_, const DataTypePtr & return_type_) : argument_types(argument_types_), return_type(return_type_) {}

    String getName() const override { return NameEmpty::name; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    const DataTypes & getArgumentTypes() const override { return argument_types; }
    const DataTypePtr & getResultType() const override { return return_type; }

    ExecutableFunctionPtr prepare(const ColumnsWithTypeAndName &) const override
    {
        return std::make_unique<ExecutableFunctionJSONEmpty>();
    }

private:
    DataTypes argument_types;
    DataTypePtr return_type;
};

class FunctionEmptyOverloadResolver final : public IFunctionOverloadResolver
{
public:
    static constexpr auto name = NameEmpty::name;

    static FunctionOverloadResolverPtr create(ContextPtr)
    {
        return std::make_unique<FunctionEmptyOverloadResolver>();
    }

    String getName() const override { return NameEmpty::name; }
    size_t getNumberOfArguments() const override { return 1; }

    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type) const override
    {
        DataTypes argument_types;
        argument_types.reserve(arguments.size());
        for (const auto & arg : arguments)
            argument_types.push_back(arg.type);

        if (argument_types.size() == 1 && isObject(argument_types[0]))
            return std::make_shared<FunctionEmptyJSON>(argument_types, return_type);

        return std::make_shared<FunctionToFunctionBaseAdaptor>(std::make_shared<FunctionEmpty>(), argument_types, return_type);
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isStringOrFixedString(arguments[0])
            && !isArray(arguments[0])
            && !isMap(arguments[0])
            && !isUUID(arguments[0])
            && !isIPv6(arguments[0])
            && !isIPv4(arguments[0])
            && !isObject(arguments[0]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}", arguments[0]->getName(), getName());

        return std::make_shared<DataTypeUInt8>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeUInt8>();
    }
};

}

REGISTER_FUNCTION(Empty)
{
    factory.registerFunction<FunctionEmptyOverloadResolver>();
}

}

