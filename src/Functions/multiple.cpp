#include <Functions/IFunction.h>
#include "Columns/ColumnArray.h"
#include "Columns/ColumnConst.h"
#include "Columns/ColumnString.h"
#include "DataTypes/DataTypeArray.h"
#include "DataTypes/DataTypeLowCardinality.h"
#include "DataTypes/DataTypeString.h"
#include "DataTypes/DataTypeTuple.h"
#include "DataTypes/DataTypesNumber.h"
#include "Databases/DatabaseMemory.h"
#include "Interpreters/Context.h"
#include "Interpreters/Context_fwd.h"
#include "Interpreters/InDepthNodeVisitor.h"
#include "Interpreters/InterpreterCreateQuery.h"
#include "Interpreters/InterpreterInsertQuery.h"
#include "Interpreters/InterpreterSelectWithUnionQuery.h"
#include "Interpreters/executeQuery.h"
#include "Parsers/ASTColumnDeclaration.h"
#include "Parsers/ASTCreateQuery.h"
#include "Parsers/ASTFunction.h"
#include "Parsers/ASTInsertQuery.h"
#include "Parsers/ASTSelectWithUnionQuery.h"
#include "Parsers/ASTSelectQuery.h"
#include "Parsers/ASTTablesInSelectQuery.h"
#include "Parsers/IAST.h"
#include "Parsers/ParserDataType.h"
#include "Processors/Chunk.h"
#include "Processors/Executors/CompletedPipelineExecutor.h"
#include "Processors/Executors/PullingPipelineExecutor.h"
#include <Parsers/ASTIdentifier.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/queryToString.h>
#include "Common/logger_useful.h"
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeFactory.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int NOT_FOUND_COLUMN_IN_BLOCK;
}

class FunctionCreateMultipleTables : public IFunction
{
public:
    static constexpr auto name = "createMultipleTables";

    static FunctionPtr create(ContextPtr context)
    {
        auto fn = std::make_shared<FunctionCreateMultipleTables>();
        fn->cur_context = Context::createCopy(context);
        fn->cur_context->setSetting("allow_experimental_object_type", 1);
        fn->cur_context->setSetting("allow_suspicious_low_cardinality_types", 1);
        fn->cur_context->setSetting("allow_experimental_annoy_index", 1);
        fn->cur_context->setSetting("allow_deprecated_database_ordinary", 1);
        return fn;
    }


    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    /// Get the name of the function.
    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 2;
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        WhichDataType lhs_uuid(arguments[0].type.get());
        if (!lhs_uuid.isUUID())
            throw DB::Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First agrument (uuid) of function {} is expected to be UUID but got {}",
                getName(), arguments[0].type->getName());

        WhichDataType rhs_str(arguments[1].type.get());
        if (!rhs_str.isString())
            throw DB::Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Second agrument (table schema) of function {} is expected to be String but got {}",
                getName(), arguments[1].type->getName());

        return std::make_shared<DataTypeTuple>(DataTypes{std::make_shared<DataTypeString>(), std::make_shared<DataTypeUInt32>()});
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        if (input_rows_count == 0)
            return result_type->createColumn();

        const auto * col_uuid =  typeid_cast<const ColumnUUID *>(arguments[0].column.get());
        const auto * col_string = typeid_cast<const ColumnString *>(arguments[1].column.get());
        if (!col_uuid)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected column string, got {}", arguments[0].column->getName());
        if (!col_string)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected column string, got {}", arguments[1].column->getName());

        std::unordered_set<std::string> databases;

        auto * log = &Poco::Logger::get("createMultipleTables");

        auto res_err = ColumnUInt32::create();
        auto res_message = ColumnString::create();

        auto context = Context::createCopy(cur_context);

        size_t size = input_rows_count;
        for (size_t ps = 0; ps < size; ++ps)
        {
            UUID uuid = col_uuid->getData()[ps];
            String database_name = "db_" + toString(uuid);


            if (!databases.contains(database_name))
            {

                String create_db_query = "create database if not exists `" + database_name + "` engine = Ordinary";
                auto io = executeQuery(create_db_query, context, true);

                // CompletedPipelineExecutor executor(io.pipeline);
                // executor.execute();

                databases.emplace(database_name);
            }

            context->setCurrentDatabase(database_name);
            auto query = col_string->getDataAt(ps);
            if (query.size == 0)
            {
                res_err->getData().emplace_back(0);
                res_message->insertDefault();
                continue;
            }

            try
            {
                ParserQuery parser(query.data + query.size, false);

                /// TODO: parser should fail early when max_query_size limit is reached.
                ASTPtr ast = parseQuery(parser, query.data, query.data + query.size, "", 1000000, 1000);

                ASTCreateQuery * ast_create = ast->as<ASTCreateQuery>();
                if (!ast_create)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Query {} is not create query", query);

                String cur_db = ast_create->getDatabase();
                if (cur_db.empty())
                    cur_db = "default";

                ast_create->setDatabase(database_name);
                String table_name = cur_db + "_" + ast_create->getTable();
                ast_create->setTable(table_name);

                ASTFunction * engine = ast_create->storage->engine;
                if (startsWith(engine->name, "Replicated"))
                {
                    engine->name = engine->name.substr(toString("Replicated").size());
                    if (engine->arguments->size() > 1)
                    {
                        engine->arguments->children = ASTs(engine->arguments->children.begin() + 2, engine->arguments->children.end());
                    }
                }

                if (ast_create->storage->settings)
                {
                    SettingsChanges changes;
                    changes.reserve(ast_create->storage->settings->changes.size());
                    for (auto change : ast_create->storage->settings->changes)
                    {
                        if (change.name != "storage_policy")
                            changes.push_back(std::move(change));
                    }
                    changes.emplace_back("allow_nullable_key", 1);
                    ast_create->storage->settings->changes.swap(changes);
                }

                LOG_TRACE(log, "Creating ({} / {}): {}", ps, size, queryToString(*ast_create));
                InterpreterCreateQuery create(ast_create->clone(), context);
                create.execute();


                ASTExpressionList expr;
                Strings strs;
                for (const auto & col : ast_create->columns_list->columns->children)
                {
                    if (const auto * col_ast = col->as<ASTColumnDeclaration>())
                    {
                        if (!col_ast->default_specifier.empty())
                        {
                            auto kind = columnDefaultKindFromString(col_ast->default_specifier);
                            if (kind != ColumnDefaultKind::Default)
                                continue;
                        }

                        const auto & col_name = col_ast->name;
                        auto col_type = col_ast->type;
                        std::string no_lc_type = recursiveRemoveLowCardinality(DataTypeFactory::instance().get(col_type))->getName();

                        ParserDataType parser_dt;
                        ASTPtr no_lc_type_ast = parseQuery(parser_dt, no_lc_type.data(), no_lc_type.data() + no_lc_type.size(), "data type", 0, 100000);

                        auto decl = std::make_shared<ASTColumnDeclaration>();
                        decl->name = col_name;
                        decl->type = no_lc_type_ast;
                        expr.children.emplace_back(decl);
                        strs.emplace_back(col_name);
                    }
                }

                if (!strs.empty())
                {
                    std::string schema = queryToString(expr);
                    std::string ins_query = "insert into table t select * from generateRandom('a', 42, 1, 1) limit 1 settings allow_experimental_object_type = 1, allow_suspicious_low_cardinality_types = 1, allow_experimental_annoy_index = 1, max_block_size = 1";
                    ASTPtr ins_ast = parseQuery(parser, ins_query.data(), ins_query.data() + ins_query.size(), "", 1000000, 1000);

                    //std::cerr << "0" << std::endl;
                    ASTInsertQuery * insert = ins_ast->as<ASTInsertQuery>();
                    insert->database = std::make_shared<ASTIdentifier>(database_name);
                    insert->table = std::make_shared<ASTIdentifier>(table_name);

                    //std::cerr << "1" << std::endl;
                    ASTSelectWithUnionQuery * ast_select_with_union = insert->select->as<ASTSelectWithUnionQuery>();
                    //std::cerr << "2" << std::endl;
                    ASTSelectQuery * ast_select = ast_select_with_union->list_of_selects->children.front()->as<ASTSelectQuery>();
                    //std::cerr << "3" << std::endl;
                    ASTTablesInSelectQuery * tables_in_select_query = ast_select->tables()->as<ASTTablesInSelectQuery>();
                    //std::cerr << "4" << std::endl;
                    ASTTablesInSelectQueryElement * tables_element = tables_in_select_query->children.front()->as<ASTTablesInSelectQueryElement>();
                    //std::cerr << "5" << std::endl;
                    ASTTableExpression * table_expr = tables_element->table_expression->as<ASTTableExpression>();
                    //std::cerr << "6" << std::endl;
                    //const auto & vvv = *table_expr;
                    //std::cerr << typeid(vvv).name() << std::endl;
                    ASTFunction * table_function = table_expr->table_function->as<ASTFunction>();
                    //std::cerr << "7" << std::endl;
                    //std::cerr << table_function->arguments->children.size() << ' ' << table_function->parameters->children.size() << std::endl;
                    table_function->arguments->children.front()->as<ASTLiteral>()->value = schema;

                    auto columns_lst = std::make_shared<ASTExpressionList>();
                    for (const auto & col_name : strs)
                        columns_lst->children.push_back(std::make_shared<ASTIdentifier>(col_name));
                    insert->columns = std::move(columns_lst);

                    LOG_TRACE(log, "Inserting ({} / {}): {}", ps, size, queryToString(*insert));

                    InterpreterInsertQuery interpreter_insert(ins_ast, context);
                    auto io = interpreter_insert.execute();
                    CompletedPipelineExecutor executor(io.pipeline);
                    executor.execute();
                }

                res_err->getData().emplace_back(0);
                res_message->insertDefault();
            }
            catch (DB::Exception & e)
            {
                res_err->getData().emplace_back(e.code());
                const auto & msg = e.message();
                res_message->insertData(msg.data(), msg.size());
            }
        }

        return ColumnTuple::create(Columns{std::move(res_message), std::move(res_err)});
    }

    ContextMutablePtr cur_context;
};


struct ReplaceDatabaseAndTableMatcher
{
    struct Data
    {
        std::string current_database_name;
        std::string replace_database_name;
    };

    static bool needChildVisit(ASTPtr &, const ASTPtr &) { return true; }
    static void visit(ASTPtr & ast, Data & data)
    {
        if (auto * t = ast->as<ASTTablesInSelectQueryElement>())
            visit(*t, data);
    }

    static void visit(ASTTablesInSelectQueryElement & elem, Data & data)
    {
        if (!elem.table_expression)
            return;

        auto * ast_table_expression = elem.table_expression->as<ASTTableExpression>();
        if (!ast_table_expression)
            return;

        if (!ast_table_expression->database_and_table_name)
            return;

        auto * identifier = ast_table_expression->database_and_table_name->as<ASTTableIdentifier>();
        if (!identifier)
            return;

        std::string cur_db;
        std::string cur_table;

        if (identifier->compound())
        {
            cur_db = identifier->name_parts[0];
            cur_table = identifier->name_parts[1];
        }
        else
        {
            cur_db = data.current_database_name;
            cur_table = identifier->name_parts[0];
        }

        auto qualified_identifier = std::make_shared<ASTTableIdentifier>(data.replace_database_name, cur_db + "_" + cur_table);
        if (!identifier->alias.empty())
            qualified_identifier->setAlias(identifier->alias);
        ast_table_expression->database_and_table_name = qualified_identifier;
    }
};

using ReplaceDatabaseAndTablVisitor = InDepthNodeVisitor<ReplaceDatabaseAndTableMatcher, true>;

class FunctionRunMultipleQueries : public IFunction
{
public:
    static constexpr auto name = "runMultipleQueries";

    static FunctionPtr create(ContextPtr context)
    {
        auto fn = std::make_shared<FunctionRunMultipleQueries>();
        fn->cur_context = Context::createCopy(context);
        // fn->cur_context->setSetting("allow_experimental_object_type", 1);
        // fn->cur_context->setSetting("allow_suspicious_low_cardinality_types", 1);
        // fn->cur_context->setSetting("allow_experimental_annoy_index", 1);
        // fn->cur_context->setSetting("allow_deprecated_database_ordinary", 1);
        return fn;
    }


    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    /// Get the name of the function.
    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 3;
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        WhichDataType lhs_uuid(arguments[0].type.get());
        if (!lhs_uuid.isUUID())
            throw DB::Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First agrument (uuid) of function {} is expected to be UUID but got {}",
                getName(), arguments[0].type->getName());

        WhichDataType rhs_str(arguments[1].type.get());
        if (!rhs_str.isString())
            throw DB::Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Second agrument (table schema) of function {} is expected to be String but got {}",
                getName(), arguments[1].type->getName());

        WhichDataType rhs_str2(arguments[2].type.get());
        if (!rhs_str2.isString())
            throw DB::Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Third agrument (current database) of function {} is expected to be String but got {}",
                getName(), arguments[2].type->getName());

        return std::make_shared<DataTypeTuple>(DataTypes{std::make_shared<DataTypeString>(), std::make_shared<DataTypeUInt32>()});
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        if (input_rows_count == 0)
            return result_type->createColumn();

        const auto * col_uuid =  typeid_cast<const ColumnUUID *>(arguments[0].column.get());
        const auto * col_string = typeid_cast<const ColumnString *>(arguments[1].column.get());
        if (!col_uuid)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected column string, got {}", arguments[0].column->getName());
        if (!col_string)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected column string, got {}", arguments[1].column->getName());

        std::unordered_set<std::string> databases;

        auto * log = &Poco::Logger::get("runMultipleQueries");

        auto res_err = ColumnUInt32::create();
        auto res_message = ColumnString::create();

        auto context = Context::createCopy(cur_context);

        size_t size = input_rows_count;
        for (size_t ps = 0; ps < size; ++ps)
        {
            UUID uuid = col_uuid->getData()[ps];
            String database_name = "db_" + toString(uuid);

            auto query = col_string->getDataAt(ps);
            if (query.size == 0)
            {
                res_err->getData().emplace_back(0);
                res_message->insertDefault();
                continue;
            }

            ParserQuery parser(query.data + query.size, false);
            ASTPtr ast = parseQuery(parser, query.data, query.data + query.size, "", 1000000, 1000);

            ASTSelectWithUnionQuery * ast_select = ast->as<ASTSelectWithUnionQuery>();
            if (!ast_select)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Query {} is not select query", query);

            try
            {
                context->setCurrentDatabase(database_name);
                ReplaceDatabaseAndTablVisitor::Data data;
                data.current_database_name = "default";
                data.replace_database_name = database_name;
                ReplaceDatabaseAndTablVisitor(data).visit(ast);

                InterpreterSelectWithUnionQuery interpreter(ast, context, SelectQueryOptions{});
                BlockIO io = interpreter.execute();

                PullingPipelineExecutor executor(io.pipeline);
                Chunk chunk;
                size_t num_rows = 0;
                while (executor.pull(chunk))
                {
                    num_rows += chunk.getNumRows();
                }

                LOG_TRACE(log, "Read {} rows. Query : {}", num_rows, queryToString(ast));
                res_err->getData().emplace_back(0);
                res_message->insertDefault();
            }
            catch (DB::Exception & e)
            {
                LOG_TRACE(log, "Excpetion while executing query : {}", queryToString(ast));
                res_err->getData().emplace_back(e.code());
                const auto & msg = e.message();
                res_message->insertData(msg.data(), msg.size());
            }
        }

        return ColumnTuple::create(Columns{std::move(res_message), std::move(res_err)});
    }

    ContextMutablePtr cur_context;
};

REGISTER_FUNCTION(CreateMultipleTables)
{
    factory.registerFunction<FunctionCreateMultipleTables>();
    factory.registerFunction<FunctionRunMultipleQueries>();
}

}
