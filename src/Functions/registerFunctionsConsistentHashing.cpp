namespace DB
{
class FunctionFactory;

void registerFunctionKostikConsistentHash(FunctionFactory & factory);
void registerFunctionJumpConsistentHash(FunctionFactory & factory);

void registerFunctionsConsistentHashing(FunctionFactory & factory)
{
    registerFunctionKostikConsistentHash(factory);
    registerFunctionJumpConsistentHash(factory);
}

}
