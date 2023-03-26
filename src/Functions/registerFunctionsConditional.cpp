namespace DB
{

class FunctionFactory;

void registerFunctionIf(FunctionFactory & factory);
void registerFunctionMultiIf(FunctionFactory & factory);
void registerFunctionCaseWithExpression(FunctionFactory & factory);


void registerFunctionsConditional(FunctionFactory & factory)
{
    registerFunctionIf(factory);
    registerFunctionMultiIf(factory);
    registerFunctionCaseWithExpression(factory);
}

}
