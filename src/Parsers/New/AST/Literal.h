#pragma once

#include <Parsers/New/AST/INode.h>

#include <Core/Field.h>
#include <Parsers/ASTSampleRatio.h>

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

        static PtrTo<Literal> createNull();
        static PtrTo<NumberLiteral> createNumber(antlr4::tree::TerminalNode * literal, bool negative = false);
        static PtrTo<NumberLiteral> createNumber(const String& literal); // checks first symbol for '-' character
        static PtrTo<StringLiteral> createString(antlr4::tree::TerminalNode * literal);
        static PtrTo<StringLiteral> createString(const String& literal); // without quotes

        ASTPtr convertToOld() const override;
        String toString() const override;

        bool is(LiteralType what) const { return type == what; }

    protected:
        String token; // STRING is stored without quotes and interpolated with escape-sequences.

        Literal(LiteralType type, const String & token);

        template <typename T>
        std::optional<T> asNumber(bool minus) const
        {
            T number;
            std::stringstream ss(String(minus ? "-" : "+") + token);
            if (token.size() > 2 && (token[1] == 'x' || token[1] == 'X')) ss >> std::hex >> number;
            else if (token.size() > 1 && (token[0] == '0')) ss >> std::oct >> number;
            else ss >> number;
            if (ss.fail() || !ss.eof())
                return {};
            return number;
        }

        auto asString() const { return token; }

    private:
        LiteralType type;

        String dumpInfo() const override { return token; }
};

class NumberLiteral : public Literal
{
    public:
        explicit NumberLiteral(antlr4::tree::TerminalNode * literal);
        explicit NumberLiteral(const String & literal);

        String toString() const override;

        void makeNegative() { minus = true; }
        bool isNegative() const { return minus; }

        template <typename T> std::optional<T> as() const { return asNumber<T>(minus); }

        ASTSampleRatio::Rational convertToOldRational() const;

    private:
        bool minus = false;
};

class StringLiteral : public Literal
{
    public:
        explicit StringLiteral(antlr4::tree::TerminalNode * literal);
        explicit StringLiteral(const String & literal) : Literal(LiteralType::STRING, literal) {}

        template <typename T>
        T as() const
        {
            return asString();
        }
};

}
