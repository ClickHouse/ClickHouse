#include <iostream>
#include <Interpreters/FunctionSignature.h>


using namespace DB;

int main(int, char **)
try
{
    std::string signature("f(T, T) -> T");
    Tokens tokens(signature.data(), signature.data() + signature.size());
    TokenIterator it(tokens);

    FunctionSignature::FunctionSignaturePtr res;
    std::cerr << FunctionSignature::parseFunctionSignature(it, res) << "\n";

    return 0;
}
catch (...)
{
    tryLogCurrentException(__PRETTY_FUNCTION__);
    return 1;
}
