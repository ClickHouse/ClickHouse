namespace DB
{

class FunctionFactory;

void registerFunctionPosition(FunctionFactory &);
void registerFunctionPositionUTF8(FunctionFactory &);
void registerFunctionPositionCaseInsensitive(FunctionFactory &);
void registerFunctionPositionCaseInsensitiveUTF8(FunctionFactory &);

void registerFunctionMultiSearchAny(FunctionFactory &);
void registerFunctionMultiSearchAnyUTF8(FunctionFactory &);
void registerFunctionMultiSearchAnyCaseInsensitive(FunctionFactory &);
void registerFunctionMultiSearchAnyCaseInsensitiveUTF8(FunctionFactory &);

void registerFunctionMultiSearchFirstIndex(FunctionFactory &);
void registerFunctionMultiSearchFirstIndexUTF8(FunctionFactory &);
void registerFunctionMultiSearchFirstIndexCaseInsensitive(FunctionFactory &);
void registerFunctionMultiSearchFirstIndexCaseInsensitiveUTF8(FunctionFactory &);

void registerFunctionMultiSearchFirstPosition(FunctionFactory &);
void registerFunctionMultiSearchFirstPositionUTF8(FunctionFactory &);
void registerFunctionMultiSearchFirstPositionCaseInsensitive(FunctionFactory &);
void registerFunctionMultiSearchFirstPositionCaseInsensitiveUTF8(FunctionFactory &);

void registerFunctionMultiSearchAllPositions(FunctionFactory &);
void registerFunctionMultiSearchAllPositionsUTF8(FunctionFactory &);
void registerFunctionMultiSearchAllPositionsCaseInsensitive(FunctionFactory &);
void registerFunctionMultiSearchAllPositionsCaseInsensitiveUTF8(FunctionFactory &);

void registerFunctionHasToken(FunctionFactory &);
void registerFunctionHasTokenCaseInsensitive(FunctionFactory &);


void registerFunctionsStringSearch(FunctionFactory & factory)
{
    registerFunctionPosition(factory);
    registerFunctionPositionUTF8(factory);
    registerFunctionPositionCaseInsensitive(factory);
    registerFunctionPositionCaseInsensitiveUTF8(factory);

    registerFunctionMultiSearchAny(factory);
    registerFunctionMultiSearchAnyUTF8(factory);
    registerFunctionMultiSearchAnyCaseInsensitive(factory);
    registerFunctionMultiSearchAnyCaseInsensitiveUTF8(factory);

    registerFunctionMultiSearchFirstIndex(factory);
    registerFunctionMultiSearchFirstIndexUTF8(factory);
    registerFunctionMultiSearchFirstIndexCaseInsensitive(factory);
    registerFunctionMultiSearchFirstIndexCaseInsensitiveUTF8(factory);

    registerFunctionMultiSearchFirstPosition(factory);
    registerFunctionMultiSearchFirstPositionUTF8(factory);
    registerFunctionMultiSearchFirstPositionCaseInsensitive(factory);
    registerFunctionMultiSearchFirstPositionCaseInsensitiveUTF8(factory);

    registerFunctionMultiSearchAllPositions(factory);
    registerFunctionMultiSearchAllPositionsUTF8(factory);
    registerFunctionMultiSearchAllPositionsCaseInsensitive(factory);
    registerFunctionMultiSearchAllPositionsCaseInsensitiveUTF8(factory);

    registerFunctionHasToken(factory);
    registerFunctionHasTokenCaseInsensitive(factory);
}

}
