#include <Parsers/ParserUseQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ASTUseQuery.h>


namespace DB
{

bool ParserUseQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_use(Keyword::USE);
    ParserKeyword s_database(Keyword::DATABASE);
    /// Use ParserCompoundIdentifier to support compound database names like "db.prefix"
    ParserCompoundIdentifier name_p{/*table_name_with_optional_uuid*/ false, /*allow_query_parameter*/ true};

    if (!s_use.ignore(pos, expected))
        return false;

    ASTPtr database;
    Expected test_expected;

    /// test if we have DATABASE <identifier> pattern without moving pos
    Pos test_pos = pos;
    ASTPtr test_node;

    bool has_database_keyword_pattern =
        s_database.parse(test_pos, test_node, test_expected) &&
        name_p.parse(test_pos, test_node, test_expected);

    // now the actual parsing
    if (has_database_keyword_pattern)
    {
        // Parse DATABASE <identifier>
        s_database.ignore(pos, expected);
        if (!name_p.parse(pos, database, expected))
            return false;
    }
    else
    {
        // Parse identifier directly (handles "USE <database>" or "USE <database>.<prefix>")
        if (!name_p.parse(pos, database, expected))
            return false;
    }

    /// Convert compound identifier to a single dotted string for ASTUseQuery
    /// Both ASTIdentifier and ASTTableIdentifier inherit from ASTIdentifier,
    /// so we need to use tryGetIdentifier to handle both cases
    String database_name;
    auto * identifier = typeid_cast<ASTIdentifier *>(database.get());
    if (identifier)
    {
        /// If it's a compound identifier with multiple parts, join them with dots
        if (identifier->name_parts.size() > 1)
        {
            for (size_t i = 0; i < identifier->name_parts.size(); ++i)
            {
                if (i > 0)
                    database_name += ".";
                database_name += identifier->name_parts[i];
            }
        }
        else
        {
            database_name = identifier->name();
        }
    }
    else
    {
        /// This should not happen, but as a fallback use the whole AST representation
        database_name = database->getAliasOrColumnName();
    }

    auto query = std::make_shared<ASTUseQuery>();
    /// Create a simple identifier with the full database name
    auto simple_database = std::make_shared<ASTIdentifier>(database_name);
    query->set(query->database, simple_database);
    node = query;

    return true;
}

}
