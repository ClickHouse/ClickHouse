namespace DB
{

    class FunctionFactory;

    void registerFunctionZCurve(FunctionFactory & factory);

    void registerFunctionsZOrder(FunctionFactory & factory)
    {
        registerFunctionZCurve(factory);
    }

}

