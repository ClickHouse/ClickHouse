#include <iostream>

#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/getLeastSupertype.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ASTLiteral.h>

#include <DataTypes/FunctionSignature.h>


namespace DB
{
    namespace ErrorCodes
    {
        extern const int SYNTAX_ERROR;
    }
}


using namespace DB;

int main(int argc, char ** argv)
try
{
    if (argc < 3)
    {
        std::cerr << R"HEREDOC(
Usage: ./function_signatures "signature" "Type [value], Type [value]..."

Example: ./function_signatures "f(T1 : Array(U1), ...) -> U2" "Array(String), Array(UInt8)"
Example: ./function_signatures "f(T1, Array(T1), ...) -> leastSupertype(Array(T1), ...)" "UInt8, Array(UInt8), Int8, Array(Int8)"
Example: ./function_signatures "multiIf(cond1 UInt8, then1 T1, ..., else U) -> leastSupertype(T1, ..., U)" "UInt8, Int8, UInt8, UInt16, Float32"
Example: ./function_signatures "toFixedString(String, const N UnsignedInteger) -> FixedString(N)" "String, UInt8 3"

)HEREDOC";
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
        FunctionSignature checker(signature);

        std::string reason;
        auto return_type = checker.check(args, reason);

        if (!return_type)
            std::cerr << "Check fail, reason: " << reason << "\n";
        else
            std::cerr << "Return type: " << return_type->getName() << "\n";
    }

    return 0;
}
catch (...)
{
    std::cerr << getCurrentExceptionMessage(true) << std::endl;
    return 1;
}
