#include <Core/Field.h>
#include <Interpreters/castColumn.h>
#include <Interpreters/ExpressionActions.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/IFunctionAdaptors.h>
#include <Functions/FunctionsConversion.h>


namespace DB
{

ColumnPtr castColumn(const ColumnWithTypeAndName & arg, const DataTypePtr & type)
{
    if (arg.type->equals(*type))
        return arg.column;

    ColumnsWithTypeAndName arguments
    {
        arg,
        {
            DataTypeString().createColumnConst(arg.column->size(), type->getName()),
            std::make_shared<DataTypeString>(),
            ""
        }
    };

    FunctionOverloadResolverPtr func_builder_cast =
        std::make_shared<FunctionOverloadResolverAdaptor>(CastOverloadResolver::createImpl(false));

    auto func_cast = func_builder_cast->build(arguments);
    return func_cast->execute(arguments, type, arg.column->size());
}

}
