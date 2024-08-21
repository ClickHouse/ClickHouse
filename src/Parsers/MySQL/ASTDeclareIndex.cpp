#include <Parsers/ASTIdentifier.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/MySQL/ASTDeclareIndex.h>
#include <Parsers/MySQL/ASTDeclareOption.h>
#include <Parsers/MySQL/ASTDeclareReference.h>

namespace DB
{

namespace MySQLParser
{

struct ParserIndexColumn : public IParserBase
{
protected:
    const char * getName() const override { return "index column"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override
    {
        ParserExpression p_expression;

        if (!p_expression.parse(pos, node, expected))
            return false;

        ParserKeyword("ASC").ignore(pos, expected);
        ParserKeyword("DESC").ignore(pos, expected);
        return true;
    }
};

ASTPtr ASTDeclareIndex::clone() const
{
    auto res = std::make_shared<ASTDeclareIndex>(*this);
    res->children.clear();

    if (index_columns)
    {
        res->index_columns = index_columns->clone();
        res->children.emplace_back(res->index_columns);
    }


    if (index_options)
    {
        res->index_options = index_options->clone();
        res->children.emplace_back(res->index_options);
    }


    if (reference_definition)
    {
        res->reference_definition = reference_definition->clone();
        res->children.emplace_back(res->reference_definition);
    }

    return res;
}

static inline bool parseDeclareOrdinaryIndex(IParser::Pos & pos, String & index_name, String & index_type, Expected & expected)
{
    ASTPtr temp_node;
    ParserKeyword k_key("KEY");
    ParserKeyword k_index("INDEX");

    ParserIdentifier p_identifier;

    if (ParserKeyword("SPATIAL").ignore(pos, expected))
    {
        if (!k_key.ignore(pos, expected))
            k_index.ignore(pos, expected);

        index_type = "SPATIAL";
        if (p_identifier.parse(pos, temp_node, expected))
            index_name = temp_node->as<ASTIdentifier>()->name();
    }
    else if (ParserKeyword("FULLTEXT").ignore(pos, expected))
    {
        if (!k_key.ignore(pos, expected))
            k_index.ignore(pos, expected);

        index_type = "FULLTEXT";
        if (p_identifier.parse(pos, temp_node, expected))
            index_name = temp_node->as<ASTIdentifier>()->name();
    }
    else
    {
        if (!k_key.ignore(pos, expected))
        {
            if (!k_index.ignore(pos, expected))
                return false;
        }

        index_type = "KEY_BTREE";   /// default index type
        if (p_identifier.parse(pos, temp_node, expected))
            index_name = temp_node->as<ASTIdentifier>()->name();

        if (ParserKeyword("USING").ignore(pos, expected))
        {
            if (!p_identifier.parse(pos, temp_node, expected))
                return false;

            index_type = "KEY_" + temp_node->as<ASTIdentifier>()->name();
        }
    }

    return true;
}

static inline bool parseDeclareConstraintIndex(IParser::Pos & pos, String & index_name, String & index_type, Expected & expected)
{
    ASTPtr temp_node;
    ParserIdentifier p_identifier;

    if (ParserKeyword("CONSTRAINT").ignore(pos, expected))
    {

        if (!ParserKeyword("PRIMARY").checkWithoutMoving(pos, expected) && !ParserKeyword("UNIQUE").checkWithoutMoving(pos, expected)
            && !ParserKeyword("FOREIGN").checkWithoutMoving(pos, expected))
        {
            if (!p_identifier.parse(pos, temp_node, expected))
                return false;

            index_name = temp_node->as<ASTIdentifier>()->name();
        }
    }

    if (ParserKeyword("UNIQUE").ignore(pos, expected))
    {
        if (!ParserKeyword("KEY").ignore(pos, expected))
            ParserKeyword("INDEX").ignore(pos, expected);

        if (p_identifier.parse(pos, temp_node, expected))
            index_name = temp_node->as<ASTIdentifier>()->name();  /// reset index_name

        index_type = "UNIQUE_BTREE"; /// default btree index_type
        if (ParserKeyword("USING").ignore(pos, expected))
        {
            if (!p_identifier.parse(pos, temp_node, expected))
                return false;

            index_type = "UNIQUE_" + temp_node->as<ASTIdentifier>()->name();
        }
    }
    else if (ParserKeyword("PRIMARY KEY").ignore(pos, expected))
    {
        index_type = "PRIMARY_KEY_BTREE"; /// default btree index_type
        if (ParserKeyword("USING").ignore(pos, expected))
        {
            if (!p_identifier.parse(pos, temp_node, expected))
                return false;

            index_type = "PRIMARY_KEY_" + temp_node->as<ASTIdentifier>()->name();
        }
    }
    else if (ParserKeyword("FOREIGN KEY").ignore(pos, expected))
    {
        index_type = "FOREIGN";
        if (p_identifier.parse(pos, temp_node, expected))
            index_name = temp_node->as<ASTIdentifier>()->name();  /// reset index_name
    }

    return true;
}

bool ParserDeclareIndex::parseImpl(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    String index_name;
    String index_type;
    ASTPtr index_columns;
    ASTPtr index_options;
    ASTPtr declare_reference;

    ParserDeclareOptions p_index_options{
        {
            OptionDescribe("KEY_BLOCK_SIZE", "key_block_size", std::make_unique<ParserLiteral>()),
            OptionDescribe("USING", "index_type", std::make_unique<ParserIdentifier>()),
            OptionDescribe("WITH PARSER", "index_parser", std::make_unique<ParserIdentifier>()),
            OptionDescribe("COMMENT", "comment", std::make_unique<ParserStringLiteral>()),
            OptionDescribe("VISIBLE", "visible", std::make_unique<ParserAlwaysTrue>()),
            OptionDescribe("INVISIBLE", "visible", std::make_unique<ParserAlwaysFalse>()),
        }
    };

    if (!parseDeclareOrdinaryIndex(pos, index_name, index_type, expected))
    {
        if (!parseDeclareConstraintIndex(pos, index_name, index_type, expected))
            return false;
    }

    ParserToken s_opening_round(TokenType::OpeningRoundBracket);
    ParserToken s_closing_round(TokenType::ClosingRoundBracket);

    if (!s_opening_round.ignore(pos, expected))
        return false;

    ParserList p_index_columns(std::make_unique<ParserIndexColumn>(), std::make_unique<ParserToken>(TokenType::Comma));

    if (!p_index_columns.parse(pos, index_columns, expected))
        return false;

    if (!s_closing_round.ignore(pos, expected))
        return false;

    if (index_type != "FOREIGN")
        p_index_options.parse(pos, index_options, expected);
    else
    {
        if (!ParserDeclareReference().parse(pos, declare_reference, expected))
            return false;
    }

    auto declare_index = std::make_shared<ASTDeclareIndex>();
    declare_index->index_name = index_name;
    declare_index->index_type = index_type;
    declare_index->index_columns = index_columns;
    declare_index->index_options = index_options;
    declare_index->reference_definition = declare_reference;

    if (declare_index->index_columns)
        declare_index->children.emplace_back(declare_index->index_columns);

    if (declare_index->index_options)
        declare_index->children.emplace_back(declare_index->index_options);

    if (declare_index->reference_definition)
        declare_index->children.emplace_back(declare_index->reference_definition);

    node = declare_index;
    return true;
}

}

}
