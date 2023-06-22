#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeEnum.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Interpreters/parseColumnsListForTableFunction.h>
#include <Interpreters/Context.h>
#include <IO/WriteBufferFromVector.h>
#include <Formats/StructureToCapnProtoSchema.h>
#include <Formats/StructureToProtobufSchema.h>

#include <Common/randomSeed.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

template <class Impl>
class FunctionStructureToFormatSchema : public IFunction
{
public:

    static constexpr auto name = Impl::name;
    FunctionStructureToFormatSchema(ContextPtr context_) : context(std::move(context_))
    {
    }

    static FunctionPtr create(ContextPtr ctx)
    {
        return std::make_shared<FunctionStructureToFormatSchema>(std::move(ctx));
    }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 0; }
    bool isVariadic() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const  override { return {0, 1}; }
    bool useDefaultImplementationForConstants() const override { return false; }
    bool useDefaultImplementationForNulls() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.empty() || arguments.size() > 2)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, expected 1 or 2",
                getName(), arguments.size());

        if (!isString(arguments[0]))
        {
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of the first argument of function {}, expected constant string",
                arguments[0]->getName(),
                getName());
        }

        if (arguments.size() > 1 && !isString(arguments[1]))
        {
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of the second argument of function {}, expected constant string",
                arguments[1]->getName(),
                getName());
        }

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        if (arguments.empty() || arguments.size() > 2)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, expected 1 or 2",
                getName(), arguments.size());

        String structure = arguments[0].column->getDataAt(0).toString();
        String message_name = arguments.size() == 2 ? arguments[1].column->getDataAt(0).toString() : "Message";
        auto columns_list = parseColumnsListFromString(structure, context);
        auto col_res = ColumnString::create();
        auto & data = assert_cast<ColumnString &>(*col_res).getChars();
        WriteBufferFromVector buf(data);
        Impl::writeSchema(buf, message_name, columns_list.getAll());
        buf.finalize();
        auto & offsets = assert_cast<ColumnString &>(*col_res).getOffsets();
        offsets.push_back(data.size());
        return ColumnConst::create(std::move(col_res), input_rows_count);
    }

private:
    ContextPtr context;
};


REGISTER_FUNCTION(StructureToCapnProtoSchema)
{
    factory.registerFunction<FunctionStructureToFormatSchema<StructureToCapnProtoSchema>>(
        {
            R"(

)",
            Documentation::Examples{
                {"random", "SELECT structureToCapnProtoSchema()"},
            },
            Documentation::Categories{"Other"}
        },
        FunctionFactory::CaseSensitive);
}


REGISTER_FUNCTION(StructureToProtobufSchema)
{
    factory.registerFunction<FunctionStructureToFormatSchema<StructureToProtobufSchema>>(
        {
            R"(

)",
            Documentation::Examples{
                {"random", "SELECT structureToCapnProtoSchema()"},
            },
            Documentation::Categories{"Other"}
        },
        FunctionFactory::CaseSensitive);
}

}
