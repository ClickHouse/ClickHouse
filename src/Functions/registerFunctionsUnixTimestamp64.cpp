namespace DB
{

class FunctionFactory;

void registerToUnixTimestamp64Milli(FunctionFactory &);
void registerToUnixTimestamp64Micro(FunctionFactory &);
void registerToUnixTimestamp64Nano(FunctionFactory &);

void registerFromUnixTimestamp64Milli(FunctionFactory &);
void registerFromUnixTimestamp64Micro(FunctionFactory &);
void registerFromUnixTimestamp64Nano(FunctionFactory &);

void registerFunctionsUnixTimestamp64(FunctionFactory & factory)
{
    registerToUnixTimestamp64Milli(factory);
    registerToUnixTimestamp64Micro(factory);
    registerToUnixTimestamp64Nano(factory);

    registerFromUnixTimestamp64Milli(factory);
    registerFromUnixTimestamp64Micro(factory);
    registerFromUnixTimestamp64Nano(factory);
}

}
