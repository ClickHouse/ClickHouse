namespace DB

{
class FunctionFactory;

void registerFunctionYandexConsistentHash(FunctionFactory & factory);
void registerFunctionJumpConsistentHash(FunctionFactory & factory);
void registerFunctionSumburConsistentHash(FunctionFactory & factory);


void registerFunctionsConsistentHashing(FunctionFactory & factory)
{
    registerFunctionYandexConsistentHash(factory);
    registerFunctionJumpConsistentHash(factory);

#if !defined(ARCADIA_BUILD)
    registerFunctionSumburConsistentHash(factory);
#endif
}

}
