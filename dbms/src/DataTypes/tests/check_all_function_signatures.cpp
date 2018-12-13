#include <iostream>

#include <Functions/FunctionFactory.h>
#include <Functions/registerFunctions.h>
#include <DataTypes/FunctionSignature.h>
#include <Interpreters/Context.h>


namespace DB
{
    namespace ErrorCodes
    {
        extern const int SYNTAX_ERROR;
    }
}


using namespace DB;

int main(int, char **)
try
{
    registerFunctions();

    const auto & factory = FunctionFactory::instance();
    auto names = factory.getAllRegisteredNames();

    Context context = Context::createGlobal();

    for (const auto & name : names)
    {
        const auto & function = factory.get(name, context);
        std::string signature = function->getSignature();

        std::cerr << name << ": " << signature << "\n";

        FunctionSignature checker(signature);
    }

    return 0;
}
catch (...)
{
    std::cerr << getCurrentExceptionMessage(true) << std::endl;
    return 1;
}
