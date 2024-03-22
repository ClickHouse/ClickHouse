#include <Parsers/MySQL/ASTDeclareTableOptions.h>

#include <IO/ReadBufferFromMemory.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/MySQL/ASTDeclareOption.h>

namespace DB
{

namespace MySQLParser
{

template <bool allow_default = false>
struct ParserBoolOption : public IParserBase
{
protected:
    const char * getName() const override { return "bool option with default"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override
    {
        if constexpr (allow_default)
        {
            if (ParserKeyword(Keyword::DEFAULT).ignore(pos, expected))
            {
                node = std::make_shared<ASTIdentifier>("DEFAULT");
                return true;
            }
        }
        ParserLiteral p_literal;
        if (!p_literal.parse(pos, node, expected) || !node->as<ASTLiteral>())
            return false;

        return !(node->as<ASTLiteral>()->value.safeGet<UInt64>() != 0 && node->as<ASTLiteral>()->value.safeGet<UInt64>() != 1);
    }
};

struct ParserTablespaceName : public IParserBase
{
protected:
    const char * getName() const override { return "table space name"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override
    {
        ParserIdentifier p_identifier;

        if (!p_identifier.parse(pos, node, expected))
            return false;

        if (ParserKeyword(Keyword::STORAGE).ignore(pos, expected))
        {
            if (!ParserKeyword(Keyword::DISK).ignore(pos, expected))
            {
                if (!ParserKeyword(Keyword::MEMORY).ignore(pos, expected))
                    return false;
            }
        }

        return true;
    }
};

bool ParserDeclareTableOptions::parseImpl(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    return ParserDeclareOptions{
        {
            OptionDescribe("AUTO_INCREMENT", "auto_increment", std::make_shared<ParserLiteral>()),
            OptionDescribe("AVG_ROW_LENGTH", "avg_row_length", std::make_shared<ParserLiteral>()),
            OptionDescribe("CHARSET", "character_set", std::make_shared<ParserCharsetOrCollateName>()),
            OptionDescribe("DEFAULT CHARSET", "character_set", std::make_shared<ParserCharsetOrCollateName>()),
            OptionDescribe("CHARACTER SET", "character_set", std::make_shared<ParserCharsetOrCollateName>()),
            OptionDescribe("DEFAULT CHARACTER SET", "character_set", std::make_shared<ParserIdentifier>()),
            OptionDescribe("CHECKSUM", "checksum", std::make_shared<ParserBoolOption<false>>()),
            OptionDescribe("COLLATE", "collate", std::make_shared<ParserCharsetOrCollateName>()),
            OptionDescribe("DEFAULT COLLATE", "collate", std::make_shared<ParserIdentifier>()),
            OptionDescribe("COMMENT", "comment", std::make_shared<ParserStringLiteral>()),
            OptionDescribe("COMPRESSION", "compression", std::make_shared<ParserStringLiteral>()),
            OptionDescribe("CONNECTION", "connection", std::make_shared<ParserStringLiteral>()),
            OptionDescribe("DATA DIRECTORY", "data_directory", std::make_shared<ParserStringLiteral>()),
            OptionDescribe("INDEX DIRECTORY", "index_directory", std::make_shared<ParserStringLiteral>()),
            OptionDescribe("DELAY_KEY_WRITE", "delay_key_write", std::make_shared<ParserBoolOption<false>>()),
            OptionDescribe("ENCRYPTION", "encryption", std::make_shared<ParserStringLiteral>()),
            OptionDescribe("ENGINE", "engine", std::make_shared<ParserIdentifier>()),
            OptionDescribe("INSERT_METHOD", "insert_method", std::make_shared<ParserIdentifier>()),
            OptionDescribe("KEY_BLOCK_SIZE", "key_block_size", std::make_shared<ParserLiteral>()),
            OptionDescribe("MAX_ROWS", "max_rows", std::make_shared<ParserLiteral>()),
            OptionDescribe("MIN_ROWS", "min_rows", std::make_shared<ParserLiteral>()),
            OptionDescribe("PACK_KEYS", "pack_keys", std::make_shared<ParserBoolOption<true>>()),
            OptionDescribe("PASSWORD", "password", std::make_shared<ParserStringLiteral>()),
            OptionDescribe("ROW_FORMAT", "row_format", std::make_shared<ParserIdentifier>()),
            OptionDescribe("STATS_AUTO_RECALC", "stats_auto_recalc", std::make_shared<ParserBoolOption<true>>()),
            OptionDescribe("STATS_PERSISTENT", "stats_persistent", std::make_shared<ParserBoolOption<true>>()),
            OptionDescribe("STATS_SAMPLE_PAGES", "stats_sample_pages", std::make_shared<ParserLiteral>()),
            OptionDescribe("TABLESPACE", "tablespace", std::make_shared<ParserTablespaceName>()),
            OptionDescribe("UNION", "union", std::make_shared<ParserExpression>()),
        }
    }.parse(pos, node, expected);
}

}

}
