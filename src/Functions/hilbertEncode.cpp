#include <Functions/hilbertEncode.h>
#include <Functions/FunctionFactory.h>


namespace DB {

REGISTER_FUNCTION(HilbertEncode)
{
    factory.registerFunction<FunctionHilbertEncode>(FunctionDocumentation{
    .description=R"(

)",
        .examples{
            },
        .categories {}
    });
}

}
