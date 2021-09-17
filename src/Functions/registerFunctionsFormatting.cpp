namespace DB
{

class FunctionFactory;

void registerFunctionBitmaskToList(FunctionFactory &);
void registerFunctionFormatReadableSize(FunctionFactory &);
void registerFunctionFormatReadableQuantity(FunctionFactory &);
void registerFunctionFormatReadableTimeDelta(FunctionFactory &);

void registerFunctionsFormatting(FunctionFactory & factory)
{
    registerFunctionBitmaskToList(factory);
    registerFunctionFormatReadableSize(factory);
    registerFunctionFormatReadableQuantity(factory);
    registerFunctionFormatReadableTimeDelta(factory);
}

}
