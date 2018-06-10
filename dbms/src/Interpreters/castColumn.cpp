#include <Interpreters/castColumn.h>
#include <Interpreters/ExpressionActions.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>


namespace DB
{

ColumnPtr castColumn(const ColumnWithTypeAndName & arg, const DataTypePtr & type, const Context & context)
{
    if (arg.type->equals(*type))
        return arg.column;

    Block temporary_block
    {
        arg,
        {
            DataTypeString().createColumnConst(arg.column->size(), type->getName()),
            std::make_shared<DataTypeString>(),
            ""
        },
        {
            nullptr,
            type,
            ""
        }
    };

    FunctionBuilderPtr func_builder_cast = FunctionFactory::instance().get("CAST", context);

    ColumnsWithTypeAndName arguments{ temporary_block.getByPosition(0), temporary_block.getByPosition(1) };
    auto func_cast = func_builder_cast->build(arguments);

    func_cast->execute(temporary_block, {0, 1}, 2, arg.column->size());
    return temporary_block.getByPosition(2).column;
}

}
