namespace DB
{

class FunctionFactory;

void registerDateTimeToSnowflake(FunctionFactory &);
void registerSnowflakeToDateTime(FunctionFactory &);

void registerDateTime64ToSnowflake(FunctionFactory &);
void registerSnowflakeToDateTime64(FunctionFactory &);


void registerFunctionsSnowflake(FunctionFactory & factory)
{
    registerDateTimeToSnowflake(factory);
    registerSnowflakeToDateTime(factory);

    registerDateTime64ToSnowflake(factory);
    registerSnowflakeToDateTime64(factory);
}

}
