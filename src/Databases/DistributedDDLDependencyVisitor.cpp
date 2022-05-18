#include <Databases/DistributedDDLDependencyVisitor.h>
#include <Dictionaries/getDictionaryConfigurationFromAST.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTRenameQuery.h>
#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Parsers/ASTSystemQuery.h>
#include <Poco/String.h>

namespace DB
{

    TableNamesSet getDependenciesSetFromQuery(ContextMutablePtr global_context, const ASTPtr & ast)
    {
        assert(global_context == global_context->getGlobalContext());
        DistributedDDLDependenciesVisitor::Data data;
        data.default_database = global_context->getCurrentDatabase();
        data.query = ast;
        data.global_context = global_context;
        DistributedDDLDependenciesVisitor visitor{data};
        visitor.visit(ast);
        return data.dependencies;
    }

    void DistributedDDLDependencyVisitor::visit(const ASTPtr & ast, Data & data)
    {
        /// Looking for functions in column default expressions and dictionary source definition
        if (const auto * function = ast->as<ASTFunction>())
            visit(*function, data);
        else if (const auto * dict_source = ast->as<ASTFunctionWithKeyValueArguments>())
            visit(*dict_source, data);
        else if (const auto * storage = ast->as<ASTStorage>())
            visit(*storage, data);
        else if (const auto * rename_query = ast->as<ASTRenameQuery>())
            visit(*rename_query, data);
        else if (const auto * table_and_output_query = ast->as<ASTQueryWithTableAndOutput>())
            visit(*table_and_output_query, data);
        else if (const auto * table_identifier = ast->as<ASTTableIdentifier>())
            visit(*table_identifier, data);
        else if (const auto * system_query = ast->as<ASTSystemQuery>())
            visit(*system_query, data);
    }

    bool DistributedDDLDependencyVisitor::needChildVisit(const ASTPtr & node, const ASTPtr & child)
    {
        if (node->as<ASTStorage>())
            return false;

        if (node->as<ASTTableIdentifier>())
            return false;

        if (auto * create = node->as<ASTCreateQuery>())
        {
            if (child.get() == create->select)
                return false;
        }

        return true;
    }

    void DistributedDDLDependencyVisitor::visit(const ASTFunction & function, Data & data)
    {
        if (function.name == "joinGet" ||
            function.name == "dictHas" ||
            function.name == "dictIsIn" ||
            function.name.starts_with("dictGet"))
        {
            extractTableNameFromArgument(function, data, 0);
        }
        else if (Poco::toLower(function.name) == "in")
        {
            extractTableNameFromArgument(function, data, 1);
        }

    }

    void DistributedDDLDependencyVisitor::visit(const ASTFunctionWithKeyValueArguments & dict_source, Data & data)
    {
        if (dict_source.name != "clickhouse")
            return;
        if (!dict_source.elements)
            return;

        auto config = getDictionaryConfigurationFromAST(data.query->as<ASTCreateQuery &>(), data.global_context);
        auto info = getInfoIfClickHouseDictionarySource(config, data.global_context);

        if (!info || !info->is_local)
            return;

        if (info->table_name.database.empty())
            info->table_name.database = data.default_database;
        data.dependencies.emplace(std::move(info->table_name));
    }

    void DistributedDDLDependencyVisitor::visit(const ASTStorage & storage, Data & data)
    {
        if (!storage.engine)
            return;
        if (storage.engine->name != "Dictionary")
            return;

        extractTableNameFromArgument(*storage.engine, data, 0);
    }

    void DistributedDDLDependencyVisitor::visit(const ASTRenameQuery & rename_query, Data & data)
    {
        QualifiedTableName qualified_name;

        for (const auto & element : rename_query.elements)
        {
            String from = element.from.database.empty() ? element.from.table : element.from.database + '.' + element.from.table;
            String to = element.to.database.empty() ? element.to.table : element.to.database + '.' + element.to.table;

            if (auto maybe_qualified_name_from = QualifiedTableName::tryParseFromString(from))
            {
                qualified_name = std::move(*maybe_qualified_name_from);

                if (qualified_name.database.empty())
                {
                    qualified_name.database = data.default_database;
                }
                data.dependencies.emplace(std::move(qualified_name));
            }

            if (auto maybe_qualified_name_to = QualifiedTableName::tryParseFromString(to))
            {
                qualified_name = std::move(*maybe_qualified_name_to);

                if (qualified_name.database.empty())
                {
                    qualified_name.database = data.default_database;
                }
                data.dependencies.emplace(std::move(qualified_name));
            }
        }
    }

    void DistributedDDLDependencyVisitor::visit(const ASTQueryWithTableAndOutput & table_and_output_query, Data & data)
    {
        QualifiedTableName qualified_name;

        String database = table_and_output_query.getDatabase();
        String table = table_and_output_query.getTable();

        String name = database.empty() ? table : database + '.' + table;

        if (auto maybe_qualified_name_from = QualifiedTableName::tryParseFromString(name))
        {
            qualified_name = std::move(*maybe_qualified_name_from);

            if (qualified_name.database.empty())
            {
                qualified_name.database = data.default_database;
            }
            data.dependencies.emplace(std::move(qualified_name));
        }
    }

    void DistributedDDLDependencyVisitor::visit(const ASTTableIdentifier & table_identifier, Data & data)
    {
        QualifiedTableName qualified_name;

        qualified_name.database = table_identifier.getDatabaseName();
        qualified_name.table = table_identifier.shortName();

        if (qualified_name.database.empty())
        {
            qualified_name.database = data.default_database;
        }
        data.dependencies.emplace(std::move(qualified_name));

    }

    void DistributedDDLDependencyVisitor::visit(const ASTSystemQuery & system_query, Data & data)
    {
        QualifiedTableName qualified_name;

        String database = system_query.getDatabase();
        String table = system_query.getTable();

        String name = database.empty() ? table : database + '.' + table;

        if (auto maybe_qualified_name_from = QualifiedTableName::tryParseFromString(name))
        {
            qualified_name = std::move(*maybe_qualified_name_from);

            if (qualified_name.database.empty())
            {
                qualified_name.database = data.default_database;
            }
            data.dependencies.emplace(std::move(qualified_name));
        }
    }

    void DistributedDDLDependencyVisitor::extractTableNameFromArgument(const ASTFunction & function, Data & data, size_t arg_idx)
    {
        /// Just ignore incorrect arguments, proper exception will be thrown later
        if (!function.arguments || function.arguments->children.size() <= arg_idx)
            return;

        QualifiedTableName qualified_name;

        const auto * arg = function.arguments->as<ASTExpressionList>()->children[arg_idx].get();
        if (const auto * literal = arg->as<ASTLiteral>())
        {
            if (literal->value.getType() != Field::Types::String)
                return;

            auto maybe_qualified_name = QualifiedTableName::tryParseFromString(literal->value.get<String>());
            /// Just return if name if invalid
            if (!maybe_qualified_name)
                return;

            qualified_name = std::move(*maybe_qualified_name);
        }
        else if (const auto * identifier = dynamic_cast<const ASTIdentifier *>(arg))
        {
            /// ASTIdentifier or ASTTableIdentifier
            auto table_identifier = identifier->createTable();
            /// Just return if table identified is invalid
            if (!table_identifier)
                return;

            qualified_name.database = table_identifier->getDatabaseName();
            qualified_name.table = table_identifier->shortName();
        }
        else
        {
            assert(false);
            return;
        }

        if (qualified_name.database.empty())
        {
            /// It can be table/dictionary from default database or XML dictionary, but we cannot distinguish it here.
            qualified_name.database = data.default_database;
        }
        data.dependencies.emplace(std::move(qualified_name));
    }
}
