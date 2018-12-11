#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/GatherUtils/GatherUtils.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/getLeastSupertype.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Interpreters/castColumn.h>
#include <IO/WriteHelpers.h>
#include <Common/typeid_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

class FunctionArrayResize : public IFunction
{
public:
    static constexpr auto name = "arrayResize";
    static FunctionPtr create(const Context & context) { return std::make_shared<FunctionArrayResize>(context); }
    FunctionArrayResize(const Context & context) : context(context) {}

    String getName() const override { return name; }

    String getSignature() const override
    {
        return "f(what Array(T), size Integer) -> Array(T)"
            " OR f(what Array(T), size Integer, fill Array(U)) -> Array(leastSupertype(T, U))";
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        const auto & return_type = block.getByPosition(result).type;

        if (return_type->onlyNull())
        {
            block.getByPosition(result).column = return_type->createColumnConstWithDefaultValue(input_rows_count);
            return;
        }

        auto result_column = return_type->createColumn();

        auto array_column = block.getByPosition(arguments[0]).column;
        auto size_column = block.getByPosition(arguments[1]).column;

        if (!block.getByPosition(arguments[0]).type->equals(*return_type))
            array_column = castColumn(block.getByPosition(arguments[0]), return_type, context);

        const DataTypePtr & return_nested_type = typeid_cast<const DataTypeArray &>(*return_type).getNestedType();
        size_t size = array_column->size();

        ColumnPtr appended_column;
        if (arguments.size() == 3)
        {
            appended_column = block.getByPosition(arguments[2]).column;
            if (!block.getByPosition(arguments[2]).type->equals(*return_nested_type))
                appended_column = castColumn(block.getByPosition(arguments[2]), return_nested_type, context);
        }
        else
            appended_column = return_nested_type->createColumnConstWithDefaultValue(size);

        std::unique_ptr<GatherUtils::IArraySource> array_source;
        std::unique_ptr<GatherUtils::IValueSource> value_source;

        bool is_const = false;

        if (auto const_array_column = typeid_cast<const ColumnConst *>(array_column.get()))
        {
            is_const = true;
            array_column = const_array_column->getDataColumnPtr();
        }

        if (auto argument_column_array = typeid_cast<const ColumnArray *>(array_column.get()))
            array_source = GatherUtils::createArraySource(*argument_column_array, is_const, size);
        else
            throw Exception{"First arguments for function " + getName() + " must be array.", ErrorCodes::LOGICAL_ERROR};


        bool is_appended_const = false;
        if (auto const_appended_column = typeid_cast<const ColumnConst *>(appended_column.get()))
        {
            is_appended_const = true;
            appended_column = const_appended_column->getDataColumnPtr();
        }

        value_source = GatherUtils::createValueSource(*appended_column, is_appended_const, size);

        auto sink = GatherUtils::createArraySink(typeid_cast<ColumnArray &>(*result_column), size);

        if (size_column->isColumnConst())
            GatherUtils::resizeConstantSize(*array_source, *value_source, *sink, size_column->getInt(0));
        else
            GatherUtils::resizeDynamicSize(*array_source, *value_source, *sink, *size_column);

        block.getByPosition(result).column = std::move(result_column);
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForNulls() const override { return false; }

private:
    const Context & context;
};


void registerFunctionArrayResize(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayResize>();
}

}
