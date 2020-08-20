#pragma once

#include <Parsers/New/AST/INode.h>

#include <Core/Field.h>

#include <Token.h>
#include <tree/TerminalNode.h>

#include <sstream>


namespace DB::AST
{

class Literal : public INode
{
    public:
        enum class LiteralType
        {
            NULL_LITERAL,
            NUMBER,
            STRING,
        };

        static PtrTo<Literal> createNull(antlr4::tree::TerminalNode * literal);
        static PtrTo<NumberLiteral> createNumber(antlr4::tree::TerminalNode * literal, bool minus = false);
        static PtrTo<NumberLiteral> createNumber(String&& literal); // checks first symbol for '-' character
        static PtrTo<StringLiteral> createString(antlr4::tree::TerminalNode * literal);
        static PtrTo<StringLiteral> createString(String&& literal); // without quotes

        ASTPtr convertToOld() const override;

        bool is(LiteralType what) const { return type == what; }

    protected:
        const String token; // STRING is stored without quotes

        Literal(LiteralType type, String&& token);

        template <typename T>
        std::optional<T> asNumber(bool minus) const
        {
            T number;
            std::stringstream ss(String(minus ? "-" : "+") + token);
            ss >> number;
            if (ss.fail())
                return {};
            return number;
        }

        template <typename T>
        std::optional<T> asString() const { return token; }

    private:
        LiteralType type;

        String dumpInfo() const override { return token; }
};

class NumberLiteral : public Literal
{
    public:
        NumberLiteral(antlr4::tree::TerminalNode * literal, bool minus_)
            : Literal(LiteralType::NUMBER, literal->getSymbol()->getText()), minus(minus_)
        {
        }
        NumberLiteral(String&& literal, bool minus_) : Literal(LiteralType::NUMBER, std::move(literal)), minus(minus_) {}


        template <typename T>
        std::optional<T> as() const
        {
            return asNumber<T>(minus);
        }

    private:
        const bool minus;
};

class StringLiteral : public Literal
{
    public:
        explicit StringLiteral(antlr4::tree::TerminalNode * literal)
            : Literal(LiteralType::STRING, literal->getSymbol()->getText().substr(1, literal->getSymbol()->getText().size() - 2))
        {
        }
        explicit StringLiteral(String&& literal) : Literal(LiteralType::STRING, std::move(literal)) {}

        template <typename T>
        T as() const
        {
            return asString<T>();
        }
};

}
