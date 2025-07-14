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
}

template <class Impl>
class FunctionStructureToFormatSchema : public IFunction
{
public:

    static constexpr auto name = Impl::name;
    explicit FunctionStructureToFormatSchema(ContextPtr context_) : context(std::move(context_))
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
    factory.registerFunction<FunctionStructureToFormatSchema<StructureToCapnProtoSchema>>(FunctionDocumentation
        {
            .description=R"(
Function that converts ClickHouse table structure to CapnProto format schema
)",
            .examples{
                {"random", "SELECT structureToCapnProtoSchema('s String, x UInt32', 'MessageName') format TSVRaw", "struct MessageName\n"
"{\n"
"    s @0 : Data;\n"
"    x @1 : UInt32;\n"
"}"},
            },
            .categories{"Other"}
        });
}


REGISTER_FUNCTION(StructureToProtobufSchema)
{
    factory.registerFunction<FunctionStructureToFormatSchema<StructureToProtobufSchema>>(FunctionDocumentation
        {
            .description=R"(
Function that converts ClickHouse table structure to Protobuf format schema
)",
            .examples{
                {"random", "SELECT structureToCapnProtoSchema('s String, x UInt32', 'MessageName') format TSVRaw", "syntax = \"proto3\";\n"
"\n"
"message MessageName\n"
"{\n"
"    bytes s = 1;\n"
"    uint32 x = 2;\n"
"}"},
            },
            .categories{"Other"}
        });
}

}
