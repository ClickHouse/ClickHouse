namespace DB

{
class FunctionFactory;

void registerFunctionYandexConsistentHash(FunctionFactory & factory);
void registerFunctionJumpConsistentHash(FunctionFactory & factory);
#if !defined(ARCADIA_BUILD)
void registerFunctionSumburConsistentHash(FunctionFactory & factory);
#endif


void registerFunctionsConsistentHashing(FunctionFactory & factory)
{
    registerFunctionYandexConsistentHash(factory);
    registerFunctionJumpConsistentHash(factory);
#if !defined(ARCADIA_BUILD)
    registerFunctionSumburConsistentHash(factory);
#endif
}

}
