#include <iostream>

#include <DataTypes/DataTypeFactory.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ASTLiteral.h>

#include <Interpreters/FunctionSignature.h>


namespace DB
{
    namespace ErrorCodes
    {
        extern const int SYNTAX_ERROR;
    }

    namespace FunctionSignature
    {

    }
}


using namespace DB;

int main(int argc, char ** argv)
try
{
    if (argc < 3)
    {
        std::cerr << R"(Usage: ./function_signatures "signature" "Type [value], Type [value]..."\n)";
        return 1;
    }

    std::string signature = argv[1];
    std::string types_and_constants = argv[2];

    ColumnsWithTypeAndName args;
    {
        Tokens tokens(types_and_constants.data(), types_and_constants.data() + types_and_constants.size());
        TokenIterator pos(tokens);

        auto & type_factory = DataTypeFactory::instance();
        ParserIdentifierWithOptionalParameters type_parser;
        ParserLiteral value_parser;

        while (!pos->isEnd())
        {
            ASTPtr type_ast;
            Expected expected;
            if (!type_parser.parse(pos, type_ast, expected))
                throw Exception("Cannot parse type", ErrorCodes::SYNTAX_ERROR);
            DataTypePtr type = type_factory.get(type_ast);

            ASTPtr value_ast;
            if (value_parser.parse(pos, value_ast, expected))
                args.emplace_back(type->createColumnConst(1, typeid_cast<const ASTLiteral &>(*value_ast).value), type, "");
            else
                args.emplace_back(nullptr, type, "");

            ParserToken(TokenType::Comma).ignore(pos);
        }
    }

    {
        Tokens tokens(signature.data(), signature.data() + signature.size());
        TokenIterator it(tokens);

        FunctionSignature::FunctionSignaturePtr res;
        if (FunctionSignature::parseFunctionSignature(it, res))
        {
            std::cerr << "Parsed successfully.\n";

            auto return_type = res->check(args);

            if (!return_type)
                std::cerr << "Check fail.\n";
            else
                std::cerr << "Return type: " << return_type->getName() << "\n";
        }
        else
            std::cerr << "Parse failed.\n";
    }

    return 0;
}
catch (...)
{
    std::cerr << getCurrentExceptionMessage(true) << std::endl;
    return 1;
}
