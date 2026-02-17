#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Core/Field.h>
#include <Columns/ColumnString.h>
#include <Functions/FunctionHelpers.h>

namespace DB
{

namespace
{

/// Enumerate stream paths of data type.
class FunctionGetTypeSerializationStreams : public IFunction
{
public:
    static constexpr auto name = "getTypeSerializationStreams";
    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionGetTypeSerializationStreams>();
    }

    String getName() const override
    {
        return name;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override
    {
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto type = getType(arguments[0]);

        SerializationPtr serialization = type->getDefaultSerialization();
        auto col_res = ColumnArray::create(ColumnString::create());
        ColumnString & col_res_strings = typeid_cast<ColumnString &>(col_res->getData());
        ColumnFixedSizeHelper::Offsets & col_res_offsets = typeid_cast<ColumnArray::Offsets &>(col_res->getOffsets());

        ISerialization::EnumerateStreamsSettings settings;
        settings.enumerate_virtual_streams = true;
        serialization->enumerateStreams(
            settings,
            [&](const ISerialization::SubstreamPath & substream_path) { col_res_strings.insert(substream_path.toString()); },
            ISerialization::SubstreamData(serialization));
        col_res_offsets.push_back(col_res_strings.size());
        return ColumnConst::create(std::move(col_res), input_rows_count);
    }

private:
    static DataTypePtr getType(const ColumnWithTypeAndName & argument)
    {
        const IColumn * arg_column = argument.column.get();
        const ColumnString * arg_string = checkAndGetColumnConstData<ColumnString>(arg_column);
        if (!arg_string)
            return argument.type;

        return DataTypeFactory::instance().get(arg_string->getDataAt(0).toString());
    }
};

}

REGISTER_FUNCTION(GetTypeSerializationStreams)
{
    FunctionDocumentation::Description description = R"(
Enumerates stream paths of a data type.
This function is intended for developmental use.
    )";
    FunctionDocumentation::Syntax syntax = "getTypeSerializationStreams(col)";
    FunctionDocumentation::Arguments arguments = {
        {"col", "Column or string representation of a data-type from which the data type will be detected.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns an array with all the serialization sub-stream paths.", {"Array(String)"}};
    FunctionDocumentation::Examples examples = {
        {"tuple", "SELECT getTypeSerializationStreams(tuple('a', 1, 'b', 2))", "['{TupleElement(1), Regular}','{TupleElement(2), Regular}','{TupleElement(3), Regular}','{TupleElement(4), Regular}']"},
        {"map", "SELECT getTypeSerializationStreams('Map(String, Int64)')", "['{ArraySizes}','{ArrayElements, TupleElement(keys), Regular}','{ArrayElements, TupleElement(values), Regular}']"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {22, 6};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionGetTypeSerializationStreams>(documentation);
}

}
