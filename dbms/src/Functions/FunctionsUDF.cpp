#include <Functions/FunctionsUDF.h>

#include <Functions/FunctionFactory.h>
#include <Interpreters/UDFHeaders.h>


namespace DB
{

struct NameUDF final {
    static constexpr char name[] {"udf"};
};

struct NameUDSF final {
    static constexpr char name[] {"udsf"};
};

void registerFunctionsUDF(FunctionFactory & factory)
{
    factory.registerFunction<FunctionUDF<NameUDF, header_code, udf_code>>();
    factory.registerFunction<FunctionUDF<NameUDSF, header_code, udsf_code>>();
}

}
