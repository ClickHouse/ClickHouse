#include <iostream>

#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/getLeastSupertype.h>
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
        class TypeMatcherUnsignedInteger : public ITypeMatcher
        {
        public:
            std::string toString() const override { return "UnsignedInteger"; }
            bool match(const DataTypePtr & type, Variables &, size_t, size_t, std::string &) const override { return isUnsignedInteger(type); }
            size_t getIndex() const override { return 0; }
        };

        class TypeMatcherArray : public ITypeMatcher
        {
        private:
            TypeMatcherPtr child_matcher;
        public:
            TypeMatcherArray(const TypeMatcherPtr & child_matcher) : child_matcher(child_matcher) {}

            std::string toString() const override { return "Array(" + child_matcher->toString() + ")"; }

            bool match(const DataTypePtr & type, Variables & variables, size_t iteration, size_t arg_num, std::string & out_reason) const override
            {
                if (!isArray(type))
                {
                    out_reason = "type " + type->getName() + " is not an Array";
                    return false;
                }

                if (child_matcher)
                {
                    const DataTypeArray & arr = typeid_cast<const DataTypeArray &>(*type);
                    if (child_matcher->match(arr.getNestedType(), variables, iteration, arg_num, out_reason))
                        return true;
                    out_reason = "nested type of Array doesn't match " + child_matcher->toString() + (out_reason.empty() ? "" : ": " + out_reason);
                    return false;
                }
                else
                    return true;
            }

            size_t getIndex() const override
            {
                return child_matcher ? child_matcher->getIndex() : 0;
            }
        };

        void registerTypeMatchers()
        {
            auto & factory = TypeMatcherFactory::instance();

            factory.registerElement("UnsignedInteger",
                [](const TypeMatchers & children)
                {
                    if (!children.empty())
                        throw Exception("UnsignedInteger type matcher cannot have arguments", ErrorCodes::LOGICAL_ERROR);
                    return std::make_shared<TypeMatcherUnsignedInteger>();
                });

            factory.registerElement("Array",
                [](const TypeMatchers & children)
                {
                    if (children.size() > 1)
                        throw Exception("Array type matcher cannot have more than one argument", ErrorCodes::LOGICAL_ERROR);
                    return std::make_shared<TypeMatcherArray>(children.empty() ? nullptr : children.front());
                });
        }

        class TypeFunctionLeastSupertype : public ITypeFunction
        {
        public:
            Value apply(const Values & args) const override
            {
                DataTypes types;
                types.reserve(args.size());
                for (const Value & arg : args)
                    types.emplace_back(arg.type());
                return getLeastSupertype(types);
            }

            std::string name() const override { return "leastSupertype"; }
        };

        class TypeFunctionArray : public ITypeFunction
        {
        public:
            Value apply(const Values & args) const override
            {
                if (args.size() != 1)
                    throw Exception("Wrong number of arguments for type function Array", ErrorCodes::LOGICAL_ERROR);
                return DataTypePtr(std::make_shared<DataTypeArray>(args.front().type()));
            }

            std::string name() const override { return "Array"; }
        };

        class TypeFunctionFixedString : public ITypeFunction
        {
        public:
            Value apply(const Values & args) const override
            {
                if (args.size() != 1)
                    throw Exception("Wrong number of arguments for type function FixedString", ErrorCodes::LOGICAL_ERROR);
                return DataTypePtr(std::make_shared<DataTypeFixedString>(args.front().field().safeGet<UInt64>()));
            }

            std::string name() const override { return "FixedString"; }
        };

        void registerTypeFunctions()
        {
            auto & factory = TypeFunctionFactory::instance();
            factory.registerElement<TypeFunctionLeastSupertype>();
            factory.registerElement<TypeFunctionArray>();
            factory.registerElement<TypeFunctionFixedString>();
        }
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
        FunctionSignature::registerTypeMatchers();
        FunctionSignature::registerTypeFunctions();

        Tokens tokens(signature.data(), signature.data() + signature.size());
        TokenIterator it(tokens);

        FunctionSignature::Variables vars;
        FunctionSignature::FunctionSignaturePtr res;
        if (FunctionSignature::parseFunctionSignature(it, res))
        {
            std::cerr << "Parsed successfully.\n";
            std::cerr << res->toString() << "\n";

            std::string reason;
            auto return_type = res->check(args, vars, reason);

            if (!return_type)
                std::cerr << "Check fail, reason: " << reason << "\n";
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
