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
        {
            auto buf = WriteBufferFromVector<ColumnString::Chars>(data);
            Impl::writeSchema(buf, message_name, columns_list.getAll());
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
    factory.registerFunction<FunctionStructureToFormatSchema<StructureToCapnProtoSchema>>(FunctionDocumentation
        {
            .description=R"(
Function that converts ClickHouse table structure to CapnProto format schema
)",
            .syntax="structureToCapnProtoSchema(structure, root_struct_name)",
            .arguments={
                {"structure", "Table structure in a format column1_name column1_type, column2_name column2_type, ...."},
                {"root_struct_name", "Name for root struct in CapnProto schema. Default value - Message"}
            },
            .returned_value="CapnProto schema. String.",
            .examples{
                {
                    "Example 1",
                    "SELECT structureToCapnProtoSchema('column1 String, column2 UInt32, column3 Array(String)') FORMAT RawBLOB",
                    R"(
@0xf96402dd754d0eb7;

struct Message
{
    column1 @0 : Data;
    column2 @1 : UInt32;
    column3 @2 : List(Data);
}                
                    )"
               },
               {
                    "Example 2",
                    "SELECT structureToCapnProtoSchema('column1 Nullable(String), column2 Tuple(element1 UInt32, element2 Array(String)), column3 Map(String, String)') FORMAT RawBLOB",
                    R"(
@0xd1c8320fecad2b7f;

struct Message
{
    struct Column1
    {
        union
        {
            value @0 : Data;
            null @1 : Void;
        }
    }
    column1 @0 : Column1;
    struct Column2
    {
        element1 @0 : UInt32;
        element2 @1 : List(Data);
    }
    column2 @1 : Column2;
    struct Column3
    {
        struct Entry
        {
            key @0 : Data;
            value @1 : Data;
        }
        entries @0 : List(Entry);
    }
    column3 @2 : Column3;
}                    
                    )"
               },
               {
                    "Example 3",
                    "SELECT structureToCapnProtoSchema('column1 String, column2 UInt32', 'Root') FORMAT RawBLOB",
                    R"(
@0x96ab2d4ab133c6e1;

struct Root
{
    column1 @0 : Data;
    column2 @1 : UInt32;
}                    
                    )"
               }
            },
            .category=FunctionDocumentation::Category::Other
        });
}


REGISTER_FUNCTION(StructureToProtobufSchema)
{
    factory.registerFunction<FunctionStructureToFormatSchema<StructureToProtobufSchema>>(FunctionDocumentation
        {
            .description=R"(
Function that converts ClickHouse table structure to Protobuf format schema
)",
            .syntax="structureToProtobufSchema(structure, root_struct_name)",
            .arguments={
                {"structure", "Table structure in a format column1_name column1_type, column2_name column2_type, ...."},
                {"root_struct_name", "Name for root struct in Protobuf schema. Default value - Message"}
            },
            .returned_value="Protobuf schema. String.",
            .examples{
                {
                    "Example 1",
                    "SELECT structureToProtobufSchema('column1 String, column2 UInt32, column3 Array(String)') FORMAT RawBLOB",
                    R"(
syntax = "proto3";

message Message
{
    bytes column1 = 1;
    uint32 column2 = 2;
    repeated bytes column3 = 3;
}                    
                    )"
                },
                {
                    "Example 2",
                    "SELECT structureToProtobufSchema('column1 Nullable(String), column2 Tuple(element1 UInt32, element2 Array(String)), column3 Map(String, String)') FORMAT RawBLOB",
                    R"(
syntax = "proto3";

message Message
{
    bytes column1 = 1;
    message Column2
    {
        uint32 element1 = 1;
        repeated bytes element2 = 2;
    }
    Column2 column2 = 2;
    map<string, bytes> column3 = 3;
}                    
                    )"
                },
                {
                    "Example 3",
                    "SELECT structureToProtobufSchema('column1 String, column2 UInt32', 'Root') FORMAT RawBLOB",
                    R"(
syntax = "proto3";

message Root
{
    bytes column1 = 1;
    uint32 column2 = 2;
}                    
                    )"
                }
            },
            .category=FunctionDocumentation::Category::Other
        });
}

}
