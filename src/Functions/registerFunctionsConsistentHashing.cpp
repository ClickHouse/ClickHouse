namespace DB
{
class FunctionFactory;

void registerFunctionYandexConsistentHash(FunctionFactory & factory);
void registerFunctionJumpConsistentHash(FunctionFactory & factory);

void registerFunctionsConsistentHashing(FunctionFactory & factory)
{
    registerFunctionYandexConsistentHash(factory);
    registerFunctionJumpConsistentHash(factory);
}

}
