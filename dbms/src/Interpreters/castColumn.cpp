#include <Interpreters/castColumn.h>
#include <Interpreters/ExpressionActions.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>


namespace DB
{

ColumnPtr castColumn(const ColumnWithTypeAndName & arg, const DataTypePtr & type, const Context & context)
{
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

    FunctionPtr func_cast = FunctionFactory::instance().get("CAST", context);

    {
        DataTypePtr unused_return_type;
        ColumnsWithTypeAndName arguments{ temporary_block.getByPosition(0), temporary_block.getByPosition(1) };
        std::vector<ExpressionAction> unused_prerequisites;

        /// Prepares function to execution. TODO It is not obvious.
        func_cast->getReturnTypeAndPrerequisites(arguments, unused_return_type, unused_prerequisites);
    }

    func_cast->execute(temporary_block, {0, 1}, 2);
    return temporary_block.getByPosition(2).column;
}

}
