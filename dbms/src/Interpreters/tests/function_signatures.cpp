#include <iostream>
#include <Interpreters/FunctionSignature.h>


using namespace DB;

int main(int, char **)
try
{
    std::string signature("f(T, T) -> T");

    std::cerr << signature << "\n";

    Tokens tokens(signature.data(), signature.data() + signature.size());
    TokenIterator it(tokens);

    FunctionSignature::FunctionSignaturePtr res;
    if (FunctionSignature::parseFunctionSignature(it, res))
        std::cerr << "Success.\n";
    else
        std::cerr << "Fail.\n";

    return 0;
}
catch (...)
{
    std::cerr << getCurrentExceptionMessage(true) << std::endl;
    return 1;
}
