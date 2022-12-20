namespace DB
{

class FunctionFactory;

void registerFunctionTuple(FunctionFactory &);
void registerFunctionTupleElement(FunctionFactory &);
void registerFunctionTupleToNameValuePairs(FunctionFactory &);

void registerFunctionsTuple(FunctionFactory & factory)
{
    registerFunctionTuple(factory);
    registerFunctionTupleElement(factory);
    registerFunctionTupleToNameValuePairs(factory);
}

}
