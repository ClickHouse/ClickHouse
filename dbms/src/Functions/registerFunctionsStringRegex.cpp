namespace DB
{

void registerFunctionsStringRegex(FunctionFactory & factory)
{
    factory.registerFunction<FunctionMatch>();
    factory.registerFunction<FunctionLike>();
    factory.registerFunction<FunctionNotLike>();
    factory.registerFunction<FunctionExtract>();

    factory.registerFunction<FunctionReplaceOne>();
    factory.registerFunction<FunctionReplaceAll>();
    factory.registerFunction<FunctionReplaceRegexpOne>();
    factory.registerFunction<FunctionReplaceRegexpAll>();

    factory.registerFunction<FunctionMultiMatchAny>();
    factory.registerFunction<FunctionMultiMatchAnyIndex>();
    factory.registerFunction<FunctionMultiMatchAllIndices>();
    factory.registerFunction<FunctionMultiFuzzyMatchAny>();
    factory.registerFunction<FunctionMultiFuzzyMatchAnyIndex>();
    factory.registerFunction<FunctionMultiFuzzyMatchAllIndices>();
}

}

