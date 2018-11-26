#include <DataTypes/DataTypesNumber.h>

#include <Storages/System/StorageSystemOne.h>
#include <Storages/System/StorageSystemNumbers.h>
#include <Databases/DatabaseMemory.h>

#include <Parsers/ParserSelectQuery.h>
#include <Parsers/parseQuery.h>

#include <Interpreters/Context.h>
#include <Interpreters/SyntaxAnalyzer.h>

#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/ReadBufferFromFileDescriptor.h>

#include <vector>
#include <unordered_map>
#include <iostream>


using namespace DB;

namespace DB
{
    namespace ErrorCodes
    {
        extern const int SYNTAX_ERROR;
    }
}

struct TestEntry
{
    String query;
    std::unordered_map<String, String> expected_aliases; /// alias -> AST.getID()
    NamesAndTypesList source_columns = {};
    Names required_result_columns = {};

    bool check(const Context & context)
    {
        ASTPtr ast = parse(query);

        auto res = SyntaxAnalyzer(context, {}).analyze(ast, source_columns, required_result_columns);
        return checkAliases(*res);
    }

private:
    bool checkAliases(const SyntaxAnalyzerResult & res)
    {
        for (const auto & alias : res.aliases)
        {
            const String & alias_name = alias.first;
            if (expected_aliases.count(alias_name) == 0 ||
                expected_aliases[alias_name] != alias.second->getID())
            {
                std::cout << "unexpected alias: " << alias_name << ' ' << alias.second->getID() << std::endl;
                return false;
            }
            else
                expected_aliases.erase(alias_name);
        }

        if (!expected_aliases.empty())
        {
            std::cout << "missing aliases: " << expected_aliases.size() << std::endl;
            return false;
        }

        return true;
    }

    static ASTPtr parse(const std::string & query)
    {
        ParserSelectQuery parser;
        std::string message;
        auto text = query.data();
        if (ASTPtr ast = tryParseQuery(parser, text, text + query.size(), message, false, "", false, 0))
            return ast;
        throw Exception(message, ErrorCodes::SYNTAX_ERROR);
    }
};


int main()
{
    std::vector<TestEntry> queries =
    {
        {
            "SELECT number AS n FROM system.numbers LIMIT 0",
            {{"n", "Identifier_number"}},
            { NameAndTypePair("number", std::make_shared<DataTypeUInt64>()) }
        },

        {
            "SELECT number AS n FROM system.numbers LIMIT 0",
            {{"n", "Identifier_number"}}
        }
    };

    Context context = Context::createGlobal();

    auto system_database = std::make_shared<DatabaseMemory>("system");
    context.addDatabase("system", system_database);
    //context.setCurrentDatabase("system");
    system_database->attachTable("one", StorageSystemOne::create("one"));
    system_database->attachTable("numbers", StorageSystemNumbers::create("numbers", false));

    size_t success = 0;
    for (auto & entry : queries)
    {
        try
        {
            if (entry.check(context))
            {
                ++success;
                std::cout << "[OK] " << entry.query << std::endl;
            }
            else
                std::cout << "[Failed] " << entry.query << std::endl;
        }
        catch (Exception & e)
        {
            std::cout << "[Error] " << entry.query << std::endl << e.displayText() << std::endl;
        }
    }

    return success != queries.size();
}
