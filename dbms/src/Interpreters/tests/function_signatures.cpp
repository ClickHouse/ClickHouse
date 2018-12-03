#include <iostream>

#include <DataTypes/DataTypeFactory.h>

#include <Interpreters/FunctionSignature.h>


using namespace DB;

int main(int argc, char ** argv)
try
{
    std::string signature(argc >= 2 ? argv[1] : "f(T, T) -> T");

    std::cerr << signature << "\n";

    Tokens tokens(signature.data(), signature.data() + signature.size());
    TokenIterator it(tokens);

    auto & type_factory = DataTypeFactory::instance();

    FunctionSignature::FunctionSignaturePtr res;
    if (FunctionSignature::parseFunctionSignature(it, res))
    {
        std::cerr << "Parsed successfully.\n";

        ColumnsWithTypeAndName args
        {
            { nullptr, type_factory.get("UInt8"), "" },
            { nullptr, type_factory.get("String"), "" },
        };

        auto return_type = res->check(args);

        if (!return_type)
            std::cerr << "Check fail.\n";
        else
            std::cerr << "Return type: " << return_type->getName() << "\n";
    }
    else
        std::cerr << "Parse failed.\n";

    return 0;
}
catch (...)
{
    std::cerr << getCurrentExceptionMessage(true) << std::endl;
    return 1;
}
