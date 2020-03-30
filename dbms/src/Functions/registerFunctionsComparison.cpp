namespace DB
{

class FunctionFactory;

void registerFunctionEquals(FunctionFactory & factory);
void registerFunctionNotEquals(FunctionFactory & factory);
void registerFunctionLess(FunctionFactory & factory);
void registerFunctionGreater(FunctionFactory & factory);
void registerFunctionLessOrEquals(FunctionFactory & factory);
void registerFunctionGreaterOrEquals(FunctionFactory & factory);


void registerFunctionsComparison(FunctionFactory & factory)
{
    registerFunctionEquals(factory);
    registerFunctionNotEquals(factory);
    registerFunctionLess(factory);
    registerFunctionGreater(factory);
    registerFunctionLessOrEquals(factory);
    registerFunctionGreaterOrEquals(factory);
}

}
