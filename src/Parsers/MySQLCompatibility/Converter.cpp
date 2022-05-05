#include <Parsers/MySQLCompatibility/types.h>

#include <Parsers/MySQLCompatibility/Converter.h>

#include <Parsers/MySQLCompatibility/Recognizer.h>
#include <Parsers/MySQLCompatibility/TreePath.h>

#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSetQuery.h>

#include <Common/SettingsChanges.h>

namespace MySQLCompatibility
{

String Converter::dumpAST(const String & query) const
{
    MySQLPtr root = std::make_shared<MySQLTree>();
    std::string error;
    MySQLTree::FromQuery(query, root, error);

    if (root != nullptr)
        return root->PrintTree();
    else
        return "MySQL query is invalid: " + error;
}

String Converter::dumpTerminals(const String & query) const
{
    MySQLPtr root = std::make_shared<MySQLTree>();
    std::string error;
    MySQLTree::FromQuery(query, root, error);

    if (root != nullptr)
        return root->PrintTerminalPaths();
    else
        return "MySQL query is invalid, TODO: listen antlr errors";
}

static void queryMovePosition(const char *& pos, const char * end)
{
    while (pos < end)
    {
        if (pos + 1 < end && (*pos) == '/' && *(pos + 1) == '*')
        {
            while (pos < end)
            {
                if (pos + 1 < end && (*pos) == '*' && *(pos + 1) == '/')
                {
                    pos += 2;
                    break;
                }
                ++pos;
            }
            continue;
        }

        if ((*pos) == '\'' || (*pos) == '"' || (*pos) == '`')
        {
            char quote = *pos;
            int bs_count = 0;
            ++pos;
            while (pos < end)
            {
                if ((*pos) == '\\')
                    bs_count += 1;

                if ((*pos) == quote && bs_count % 2 == 0)
                {
                    ++pos;
                    break;
                }

                if ((*pos) != '\\')
                    bs_count = 0;

                ++pos;
            }
            continue;
        }

        bool line_comment = false;
        line_comment = (line_comment || (*pos == '#'));
        line_comment = (line_comment || (pos + 1 < end && (*pos) == '-' && *(pos + 1) == '-'));
        line_comment = (line_comment || (pos + 1 < end && (*pos) == '/' && *(pos + 1) == '/'));

        if (line_comment)
        {
            while (pos < end)
            {
                // FIXME: add other linebreaks
                if (*pos == '\n')
                {
                    ++pos;
                    break;
                }
                ++pos;
            }
            continue;
        }

        if ((*pos) == ';')
        {
            ++pos;
            break;
        }

        ++pos;
    }
}

String Converter::extractQuery(const char *& pos, const char * end)
{
    String query = "";
    const char * begin = pos;
    queryMovePosition(pos, end);
    for (int i = 0; (begin + i) < pos && (begin + i) < end; ++i)
        query += begin[i];

    return query;
}

bool Converter::toClickHouseAST(const String & query, CHPtr & ch_tree, String & error) const
{
    String internal_error;
    ch_tree = nullptr;

    MySQLPtr root = nullptr;
    if (!MySQLTree::FromQuery(query, root, internal_error))
    {
        error = "invalid MySQL query; " + internal_error;
        return false;
    }
    GenericRecognizer recognizer;
    auto result = recognizer.Recognize(root);

    if (result == nullptr)
    {
        error = "ClickHouse does not support this type of MySQL queries";
        return false;
    }

    if (!result->setup(internal_error))
    {
        error = "failed to convert correct MySQL query; " + internal_error;
        return false;
    }

    result->convert(ch_tree);
    return true;
}

}
