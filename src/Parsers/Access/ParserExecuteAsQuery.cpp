#include <Parsers/Access/ParserExecuteAsQuery.h>

#include <Parsers/Access/ASTExecuteAsQuery.h>
#include <Parsers/Access/ASTUserNameWithHost.h>
#include <Parsers/Access/ParserUserNameWithHost.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/CommonParsers.h>


namespace DB
{

ParserExecuteAsQuery::ParserExecuteAsQuery(IParser & subquery_parser_)
    : subquery_parser(subquery_parser_)
{
}

bool ParserExecuteAsQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (!ParserKeyword{Keyword::EXECUTE_AS}.ignore(pos, expected))
        return false;

    ASTPtr target_user;
    if (!ParserUserNameWithHost(/*allow_query_parameter=*/ false).parse(pos, target_user, expected))
        return false;

    auto query = make_intrusive<ASTExecuteAsQuery>();
    node = query;

    query->set(query->target_user, target_user);

    /// support 1) EXECUTE AS <user1>  2) EXECUTE AS <user1> SELECT ...

    ASTPtr subquery;
    if (subquery_parser.parse(pos, subquery, expected))
    {
        if (auto * sub_output = dynamic_cast<ASTQueryWithOutput *>(subquery.get()))
        {
            /// Hoist output options (FORMAT, INTO OUTFILE, COMPRESSION, SETTINGS) from the subquery
            /// to the outer EXECUTE AS query. This is needed because the inner `executeQuery` is called
            /// with `QueryFlags{ .internal = true }` and returns raw blocks — the outer `executeQuery`
            /// pipeline reads these options from the top-level AST to apply formatting.
            if (sub_output->out_file)
            {
                query->out_file = sub_output->out_file;
                query->children.push_back(query->out_file);
                query->setIsOutfileAppend(sub_output->isOutfileAppend());
                query->setIsOutfileTruncate(sub_output->isOutfileTruncate());
                query->setIsIntoOutfileWithStdout(sub_output->isIntoOutfileWithStdout());
            }
            if (sub_output->format_ast)
            {
                query->format_ast = sub_output->format_ast;
                query->children.push_back(query->format_ast);
            }
            if (sub_output->compression)
            {
                query->compression = sub_output->compression;
                query->children.push_back(query->compression);
            }
            if (sub_output->compression_level)
            {
                query->compression_level = sub_output->compression_level;
                query->children.push_back(query->compression_level);
            }
            if (sub_output->settings_ast)
            {
                query->settings_ast = sub_output->settings_ast;
                query->children.push_back(query->settings_ast);
            }
            ASTQueryWithOutput::resetOutputASTIfExist(*sub_output);
        }

        query->set(query->subquery, subquery);
    }

    return true;
}

}
