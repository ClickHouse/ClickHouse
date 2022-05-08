#include "Internals.h"

#include "AST.h"

#include <sstream>

namespace MySQLParserOverlay
{

static AST::TOKEN_TYPE castTokenTypeFromANTLR(size_t antlr_token_type)
{
    switch (antlr_token_type)
    {
        // TYPES
        case MySQLLexer::INT_NUMBER:
            return AST::TOKEN_TYPE::INT_NUMBER;
        case MySQLLexer::FLOAT_NUMBER:
            return AST::TOKEN_TYPE::FLOAT_NUMBER;
        case MySQLLexer::DECIMAL_NUMBER:
            return AST::TOKEN_TYPE::DECIMAL_NUMBER;
        case MySQLLexer::FALSE_SYMBOL:
            return AST::TOKEN_TYPE::FALSE_SYMBOL;
        case MySQLLexer::TRUE_SYMBOL:
            return AST::TOKEN_TYPE::TRUE_SYMBOL;

        // OPERATORS
        case MySQLLexer::PLUS_OPERATOR:
            return AST::TOKEN_TYPE::PLUS_OPERATOR;
        case MySQLLexer::MINUS_OPERATOR:
            return AST::TOKEN_TYPE::MINUS_OPERATOR;
        case MySQLLexer::MULT_OPERATOR:
            return AST::TOKEN_TYPE::MULT_OPERATOR;
        case MySQLLexer::DIV_OPERATOR:
            return AST::TOKEN_TYPE::DIV_OPERATOR;
        case MySQLLexer::DIV_SYMBOL:
            return AST::TOKEN_TYPE::DIV_SYMBOL;
        case MySQLLexer::MOD_OPERATOR:
            return AST::TOKEN_TYPE::MOD_OPERATOR;
        case MySQLLexer::MOD_SYMBOL:
            return AST::TOKEN_TYPE::MOD_SYMBOL;

        // COMPARE
        case MySQLLexer::GREATER_THAN_OPERATOR:
            return AST::TOKEN_TYPE::GREATER_THAN_OPERATOR;
        case MySQLLexer::GREATER_OR_EQUAL_OPERATOR:
            return AST::TOKEN_TYPE::GREATER_OR_EQUAL_OPERATOR;
        case MySQLLexer::LESS_THAN_OPERATOR:
            return AST::TOKEN_TYPE::LESS_THAN_OPERATOR;
        case MySQLLexer::LESS_OR_EQUAL_OPERATOR:
            return AST::TOKEN_TYPE::LESS_OR_EQUAL_OPERATOR;
        case MySQLLexer::EQUAL_OPERATOR:
            return AST::TOKEN_TYPE::EQUAL_OPERATOR;
        case MySQLLexer::NOT_EQUAL_OPERATOR:
            return AST::TOKEN_TYPE::NOT_EQUAL_OPERATOR;
        case MySQLLexer::NULL_SAFE_EQUAL_OPERATOR:
            return AST::TOKEN_TYPE::NULL_SAFE_EQUAL_OPERATOR;

        // LOGIC
        case MySQLLexer::NOT_SYMBOL:
            return AST::TOKEN_TYPE::NOT_SYMBOL;
        case MySQLLexer::NOT2_SYMBOL:
            return AST::TOKEN_TYPE::NOT2_SYMBOL;
        case MySQLLexer::LOGICAL_NOT_OPERATOR:
            return AST::TOKEN_TYPE::LOGICAL_NOT_OPERATOR;
        case MySQLLexer::AND_SYMBOL:
            return AST::TOKEN_TYPE::AND_SYMBOL;
        case MySQLLexer::LOGICAL_AND_OPERATOR:
            return AST::TOKEN_TYPE::LOGICAL_AND_OPERATOR;
        case MySQLLexer::OR_SYMBOL:
            return AST::TOKEN_TYPE::OR_SYMBOL;
        case MySQLLexer::LOGICAL_OR_OPERATOR:
            return AST::TOKEN_TYPE::LOGICAL_OR_OPERATOR;
        case MySQLLexer::XOR_SYMBOL:
            return AST::TOKEN_TYPE::XOR_SYMBOL;

        // DIRECTION
        case MySQLLexer::ASC_SYMBOL:
            return AST::TOKEN_TYPE::ASC_SYMBOL;
        case MySQLLexer::DESC_SYMBOL:
            return AST::TOKEN_TYPE::DESC_SYMBOL;

        // SHOW
        case MySQLLexer::TABLES_SYMBOL:
            return AST::TOKEN_TYPE::TABLES_SYMBOL;
        case MySQLLexer::COLUMNS_SYMBOL:
            return AST::TOKEN_TYPE::COLUMNS_SYMBOL;

        // AGGREGATE
        case MySQLLexer::COUNT_SYMBOL:
            return AST::TOKEN_TYPE::COUNT_SYMBOL;
        case MySQLLexer::AVG_SYMBOL:
            return AST::TOKEN_TYPE::AVG_SYMBOL;
        case MySQLLexer::MIN_SYMBOL:
            return AST::TOKEN_TYPE::MIN_SYMBOL;
        case MySQLLexer::MAX_SYMBOL:
            return AST::TOKEN_TYPE::MAX_SYMBOL;
        case MySQLLexer::STD_SYMBOL:
            return AST::TOKEN_TYPE::STD_SYMBOL;
        case MySQLLexer::VARIANCE_SYMBOL:
            return AST::TOKEN_TYPE::VARIANCE_SYMBOL;
        case MySQLLexer::STDDEV_SAMP_SYMBOL:
            return AST::TOKEN_TYPE::STDDEV_SAMP_SYMBOL;
        case MySQLLexer::VAR_SAMP_SYMBOL:
            return AST::TOKEN_TYPE::VAR_SAMP_SYMBOL;
        case MySQLLexer::SUM_SYMBOL:
            return AST::TOKEN_TYPE::SUM_SYMBOL;
        case MySQLLexer::GROUP_CONCAT_SYMBOL:
            return AST::TOKEN_TYPE::GROUP_CONCAT_SYMBOL;


        default:
            return AST::TOKEN_TYPE::UNKNOWN;
    }

    return AST::TOKEN_TYPE::UNKNOWN;
}

static bool buildFromANTLR(const MySQLParser & parser, const antlr4::RuleContext * antlr_tree, ASTPtr node)
{
    auto rule_index = antlr_tree->getRuleIndex();
    auto rule_name_name = parser.getRuleNames()[rule_index];
    node->rule_name = rule_name_name;
    for (const auto & x : antlr_tree->children)
    {
        if (antlrcpp::is<antlr4::tree::ErrorNode *>(x))
            return false;

        antlr4::tree::TerminalNode * antlr_terminal = nullptr;
        if ((antlr_terminal = dynamic_cast<antlr4::tree::TerminalNode *>(x)) != nullptr)
        {
            if (antlr_terminal->getSymbol() != nullptr)
            {
                node->terminals.push_back(antlr_terminal->getSymbol()->getText());
                node->terminal_types.push_back(castTokenTypeFromANTLR(antlr_terminal->getSymbol()->getType()));
            }
        }
    }

    for (const auto & x : antlr_tree->children)
    {
        antlr4::RuleContext * antlr_child = nullptr;
        if ((antlr_child = dynamic_cast<antlr4::RuleContext *>(x)) != nullptr)
        {
            ASTPtr ast_child = std::make_shared<AST>();
            node->children.push_back(ast_child);
            if (!buildFromANTLR(parser, antlr_child, ast_child))
                return false;
        }
    }

    return true;
}

bool AST::FromQuery(const std::string & query, ASTPtr & result, std::string & error)
{
    uint32_t settings = 0;
    // settings |= AnsiQuotes;

    auto analyzer = MySQLAnalyzer(query, settings);
    auto tree = analyzer.parse();

    result = std::make_shared<AST>();
    if (!analyzer.getParseError().empty())
    {
        error = analyzer.getParseError();
        result = nullptr;
        return;
    }

    if (!buildFromANTLR(analyzer.getParser(), tree, result))
    {
        error = "unkown internal error, syntax tree is invalid";
        result = nullptr;
        return false;
    }

    return true;
}

std::string AST::PrintTree() const
{
    std::stringstream ss;
    PrintTreeImpl(ss);
    return ss.str();
}

void AST::PrintTreeImpl(std::stringstream & ss) const
{
    ss << "(";
    ss << rule_name << " ";
    if (!terminals.empty())
    {
        ss << "terminals = [";
        for (int i = 0; i < terminals.size(); ++i)
        {
            ss << "(";
            ss << "'" << terminals[i] << "'";
            ss << ")";
        }
        ss << "]; ";
    }
    if (!children.empty())
    {
        ss << "children = ";
        for (const auto & x : children)
        {
            x->PrintTreeImpl(ss);
        }
    }
    ss << ")";
}

std::string AST::PrintTerminalPaths() const
{
    std::stringstream ss;
    std::vector<std::string> path;
    PrintTerminalPathsImpl(ss, path);

    return ss.str();
}

void AST::PrintTerminalPathsImpl(std::stringstream & ss, std::vector<std::string> & path) const
{
    path.push_back(rule_name);
    for (const auto & term : terminals)
    {
        ss << term << ":\n";
        ss << "=====\n";
        for (const auto & name : path)
        {
            ss << name << "\n";
        }
        ss << "=====\n";
    }

    for (const auto & child : children)
        child->PrintTerminalPathsImpl(ss, path);

    path.pop_back();
}

}
