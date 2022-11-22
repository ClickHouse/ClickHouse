namespace DB
{

class FunctionFactory;

void registerFunctionsBitToArray(FunctionFactory &);
void registerFunctionFormatReadableSize(FunctionFactory &);
void registerFunctionFormatReadableQuantity(FunctionFactory &);
void registerFunctionFormatReadableTimeDelta(FunctionFactory &);

void registerFunctionsFormatting(FunctionFactory & factory)
{
    registerFunctionsBitToArray(factory);
    registerFunctionFormatReadableSize(factory);
    registerFunctionFormatReadableQuantity(factory);
    registerFunctionFormatReadableTimeDelta(factory);
}

}
