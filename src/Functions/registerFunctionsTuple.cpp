namespace DB
{

class FunctionFactory;

void registerFunctionTuple(FunctionFactory &);
void registerFunctionTupleElement(FunctionFactory &);
void registerFunctionUntuple(FunctionFactory &);

void registerFunctionsTuple(FunctionFactory & factory)
{
    registerFunctionTuple(factory);
    registerFunctionTupleElement(factory);
    registerFunctionUntuple(factory);
}

}
