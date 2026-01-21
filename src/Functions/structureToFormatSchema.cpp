#include <Columns/ColumnConst.h>
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


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{
    enum class Impl
    {
        Protobuf,
        CapnProto,
    };
}

class FunctionStructureToFormatSchema : public IFunction
{
private:
    Impl impl;

public:
    explicit FunctionStructureToFormatSchema(Impl impl_, ContextPtr context_) : impl(impl_), context(std::move(context_))
    {
    }

    String getName() const override
    {
        switch (impl)
        {
            case Impl::Protobuf: return StructureToProtobufSchema::name;
            case Impl::CapnProto: return StructureToCapnProtoSchema::name;
        }
    }

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
        {
            auto buf = WriteBufferFromVector<ColumnString::Chars>(data);
            switch (impl)
            {
                case Impl::Protobuf:
                    StructureToProtobufSchema::writeSchema(buf, message_name, columns_list.getAll());
                    break;
                case Impl::CapnProto:
                    StructureToCapnProtoSchema::writeSchema(buf, message_name, columns_list.getAll());
                    break;
            }
            buf.finalize();
        }
        auto & offsets = assert_cast<ColumnString &>(*col_res).getOffsets();
        offsets.push_back(data.size());
        return ColumnConst::create(std::move(col_res), input_rows_count);
    }

private:
    ContextPtr context;
};


REGISTER_FUNCTION(StructureToCapnProtoSchema)
{
    factory.registerFunction(StructureToCapnProtoSchema::name,
        [](ContextPtr context){ return std::make_shared<FunctionStructureToFormatSchema>(Impl::CapnProto, std::move(context)); },
        FunctionDocumentation
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
            .category = FunctionDocumentation::Category::Other
        });
}


REGISTER_FUNCTION(StructureToProtobufSchema)
{
    factory.registerFunction(StructureToProtobufSchema::name,
        [](ContextPtr context){ return std::make_shared<FunctionStructureToFormatSchema>(Impl::Protobuf, std::move(context)); },
        FunctionDocumentation
        {
            .description=R"(
Function that converts ClickHouse table structure to Protobuf format schema
)",
            .examples{
                {"random", "SELECT structureToProtobufSchema('s String, x UInt32', 'MessageName') format TSVRaw", "syntax = \"proto3\";\n"
"\n"
"message MessageName\n"
"{\n"
"    bytes s = 1;\n"
"    uint32 x = 2;\n"
"}"},
            },
            .category = FunctionDocumentation::Category::Other
        });
}

}
