namespace DB
{

class FunctionFactory;

void registerFunctionTuple(FunctionFactory &);
void registerFunctionTupleElement(FunctionFactory &);
void registerFunctionNamedTupleItems(FunctionFactory &);

void registerFunctionsTuple(FunctionFactory & factory)
{
    registerFunctionTuple(factory);
    registerFunctionTupleElement(factory);
    registerFunctionNamedTupleItems(factory);
}

}
