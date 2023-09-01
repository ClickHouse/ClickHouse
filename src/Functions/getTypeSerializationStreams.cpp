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
        ColumnVectorHelper::Offsets & col_res_offsets = typeid_cast<ColumnArray::Offsets &>(col_res->getOffsets());
        serialization->enumerateStreams([&](const ISerialization::SubstreamPath & substream_path)
        {
            col_res_strings.insert(substream_path.toString());
        });
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

        try
        {
            DataTypePtr type = DataTypeFactory::instance().get(arg_string->getDataAt(0).toString());
            return type;
        }
        catch (const DB::Exception &)
        {
            return argument.type;
        }
    }
};

}

REGISTER_FUNCTION(GetTypeSerializationStreams)
{
    factory.registerFunction<FunctionGetTypeSerializationStreams>();
}

}
