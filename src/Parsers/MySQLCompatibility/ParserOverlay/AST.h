#pragma once

#include <vector>

namespace MySQLParserOverlay
{
class AST; // fwd
using ASTPtr = std::shared_ptr<AST>;

class AST
{
public:
    enum class TOKEN_TYPE : uint8_t
    {
        // TYPES
        INT_NUMBER,
        FLOAT_NUMBER,
        DECIMAL_NUMBER,
        TRUE_SYMBOL,
        FALSE_SYMBOL,

        // OPERATORS
        PLUS_OPERATOR,
        MINUS_OPERATOR,
        MULT_OPERATOR,
        DIV_OPERATOR,
        DIV_SYMBOL,
        MOD_OPERATOR,
        MOD_SYMBOL,

        // COMPARE
        EQUAL_OPERATOR,
        NOT_EQUAL_OPERATOR,
        GREATER_OR_EQUAL_OPERATOR,
        GREATER_THAN_OPERATOR,
        LESS_OR_EQUAL_OPERATOR,
        LESS_THAN_OPERATOR,
        NULL_SAFE_EQUAL_OPERATOR,

        // LOGIC
        AND_SYMBOL,
        LOGICAL_AND_OPERATOR,
        XOR_SYMBOL,
        OR_SYMBOL,
        LOGICAL_OR_OPERATOR,
        NOT_SYMBOL,
        NOT2_SYMBOL,
        LOGICAL_NOT_OPERATOR,

        // DIRECTION
        ASC_SYMBOL,
        DESC_SYMBOL,
		
		// SHOW
		TABLES_SYMBOL,
		COLUMNS_SYMBOL,

        UNKNOWN
    };

public:
    AST() { }
    AST(const std::string & rule_name_) : rule_name(rule_name_) { }
    static bool FromQuery(const std::string & query, ASTPtr & result, std::string & error);

public:
    std::string PrintTree() const;
    std::string PrintTerminalPaths() const;

private:
    void PrintTreeImpl(std::stringstream & ss) const;
    void PrintTerminalPathsImpl(std::stringstream & ss, std::vector<std::string> & path) const;

public:
    std::string rule_name;
    std::vector<ASTPtr> children;
    std::vector<std::string> terminals;
    std::vector<TOKEN_TYPE> terminal_types;
};

}
