#include <Parsers/Access/ASTCreateUserQuery.h>
#include <Parsers/Access/ParserCreateUserQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/wipePasswordFromQuery.h>
#include <Common/typeid_cast.h>


namespace DB
{

String wipePasswordFromQuery(const String & query)
{
    String error_message;
    const char * begin = query.data();
    const char * end = begin + query.size();

    {
        ParserCreateUserQuery parser;
        const char * pos = begin;
        if (auto ast = tryParseQuery(parser, pos, end, error_message, false, "", false, 0, 0))
        {
            auto create_query = typeid_cast<std::shared_ptr<ASTCreateUserQuery>>(ast);
            create_query->show_password = false;
            return serializeAST(*create_query);
        }
    }

    return query;
}

}
