namespace DB
{

class FunctionFactory;

void registerFunctionLike(FunctionFactory &);
void registerFunctionNotLike(FunctionFactory &);
void registerFunctionMatch(FunctionFactory &);
void registerFunctionExtract(FunctionFactory &);
void registerFunctionReplaceOne(FunctionFactory &);
void registerFunctionReplaceAll(FunctionFactory &);
void registerFunctionReplaceRegexpOne(FunctionFactory &);
void registerFunctionReplaceRegexpAll(FunctionFactory &);
void registerFunctionMultiMatchAny(FunctionFactory &);
void registerFunctionMultiMatchAnyIndex(FunctionFactory &);
void registerFunctionMultiMatchAllIndices(FunctionFactory &);
void registerFunctionMultiFuzzyMatchAny(FunctionFactory &);
void registerFunctionMultiFuzzyMatchAnyIndex(FunctionFactory &);
void registerFunctionMultiFuzzyMatchAllIndices(FunctionFactory &);
void registerFunctionExtractGroups(FunctionFactory &);
void registerFunctionExtractAllGroups(FunctionFactory &);

void registerFunctionsStringRegexp(FunctionFactory & factory)
{
    registerFunctionLike(factory);
    registerFunctionNotLike(factory);
    registerFunctionMatch(factory);
    registerFunctionExtract(factory);
    registerFunctionReplaceOne(factory);
    registerFunctionReplaceAll(factory);
    registerFunctionReplaceRegexpOne(factory);
    registerFunctionReplaceRegexpAll(factory);
    registerFunctionMultiMatchAny(factory);
    registerFunctionMultiMatchAnyIndex(factory);
    registerFunctionMultiMatchAllIndices(factory);
    registerFunctionMultiFuzzyMatchAny(factory);
    registerFunctionMultiFuzzyMatchAnyIndex(factory);
    registerFunctionMultiFuzzyMatchAllIndices(factory);
    registerFunctionExtractGroups(factory);
    registerFunctionExtractAllGroups(factory);
}

}

