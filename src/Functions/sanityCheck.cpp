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
#include "Interpreters/InterpreterCreateQuery.h"
#include "Interpreters/InterpreterInsertQuery.h"
#include "Parsers/ASTColumnDeclaration.h"
#include "Parsers/ASTCreateQuery.h"
#include "Parsers/ASTFunction.h"
#include "Parsers/ASTInsertQuery.h"
#include "Parsers/ASTSelectWithUnionQuery.h"
#include "Parsers/ASTSelectQuery.h"
#include "Parsers/ASTTablesInSelectQuery.h"
#include "Parsers/IAST.h"
#include "Parsers/ParserDataType.h"
#include "Processors/Executors/CompletedPipelineExecutor.h"
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

class FunctionSanityCheck : public IFunction
{
public:
    static constexpr auto name = "sanityCheck";

    static FunctionPtr create(ContextPtr context)
    {
        auto fn = std::make_shared<FunctionSanityCheck>();
        fn->context = Context::createCopy(context);
        fn->context->setSetting("allow_experimental_object_type", 1);
        fn->context->setSetting("allow_suspicious_low_cardinality_types", 1);
        fn->context->setSetting("allow_experimental_annoy_index", 1);
        //fn->context->setSetting("allow_nullable_key", 1);
        return fn;
    }


    bool useDefaultImplementationForConstants() const override { return false; }
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
        WhichDataType lhs(arguments[0].type);

        if (!lhs.isString())
            throw DB::Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First agrument (query) of function {} is expected to be string but got {}",
                getName(), arguments[0].type->getName());

        if (!arguments[1].column)
            throw DB::Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Second agrument (table schema) of function {} is expected to be constant",
                getName());


        const auto * rhs_arr = typeid_cast<const DataTypeArray *>(arguments[1].type.get());
        if (!rhs_arr)
            throw DB::Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Second agrument (table schema) of function {} is expected to be array(tuple(UUID, string)) but got {}",
                getName(), arguments[1].type->getName());

        const auto * rhs_tuple = typeid_cast<const DataTypeTuple *>(rhs_arr->getNestedType().get());
        if (rhs_tuple == nullptr || rhs_tuple->getElements().size() != 2)
            throw DB::Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Second agrument (table schema) of function {} is expected to be array(tuple(UUID, string)) but got {}",
                getName(), arguments[1].type->getName());

        WhichDataType rhs_str(rhs_tuple->getElement(1).get());
        if (!rhs_str.isString())
            throw DB::Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Second agrument (table schema) of function {} is expected to be array(tuple(UUID, string)) but got {}",
                getName(), arguments[1].type->getName());

        WhichDataType rhs_uuid(rhs_tuple->getElement(0).get());
        if (!rhs_uuid.isUUID())
            throw DB::Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Second agrument (table schema) of function {} is expected to be array(tuple(UUID, string)) but got {}",
                getName(), arguments[1].type->getName());

        return std::make_shared<DataTypeTuple>(DataTypes{std::make_shared<DataTypeString>(), std::make_shared<DataTypeUInt32>()});
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        if (input_rows_count == 0)
            return result_type->createColumn();

        const auto * col_const = typeid_cast<const ColumnConst *>(arguments[1].column.get());
        if (!col_const)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected column const, got {}", arguments[1].column->getName());

        auto internal = col_const->getDataColumnPtr();
        const auto * col_arr = typeid_cast<const ColumnArray *>(internal.get());
        if (!col_arr)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected column arr, got {}", internal->getName());

        const auto & offsets = col_arr->getOffsets();

        const auto * col_tuple = typeid_cast<const ColumnTuple *>(col_arr->getDataPtr().get());
        if (!col_tuple)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected column tuple, got {}", col_arr->getDataPtr()->getName());

        const auto * col_uuid =  typeid_cast<const ColumnUUID *>(col_tuple->getColumnPtr(0).get());
        const auto * col_string = typeid_cast<const ColumnString *>(col_tuple->getColumnPtr(1).get());
        if (!col_uuid)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected column string, got {}", col_tuple->getColumnPtr(0)->getName());
        if (!col_string)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected column string, got {}", col_tuple->getColumnPtr(1)->getName());

        std::unordered_map<std::string, std::shared_ptr<DatabaseMemory>> databases;



        auto * log = &Poco::Logger::get("FunctionSanityCheck");

        size_t size = offsets[0];
        for (size_t ps = 0; ps < size; ++ps)
        {
            try
            {
                UUID uuid = col_uuid->getData()[ps];
                String database_name = "db_" + toString(uuid);
                if (!databases.contains(database_name))
                {
                    auto db = std::make_shared<DatabaseMemory>(database_name, context);
                    DatabaseCatalog::instance().attachDatabase(database_name, db);
                    databases.emplace(database_name, db);
                }

                context->setCurrentDatabase(database_name);

                auto query = col_string->getDataAt(ps);
                if (query.size == 0)
                    continue;
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
                            if (!(kind == ColumnDefaultKind::Default || kind == ColumnDefaultKind::Materialized))
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

                    std::cerr << "0" << std::endl;
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
                    std::cerr << "7" << std::endl;
                    //std::cerr << table_function->arguments->children.size() << ' ' << table_function->parameters->children.size() << std::endl;
                    table_function->arguments->children.front()->as<ASTLiteral>()->value = schema;

                    auto columns_lst = std::make_shared<ASTExpressionList>();
                    for (const auto & col_name : strs)
                        columns_lst->children.push_back(std::make_shared<ASTIdentifier>(col_name));
                    insert->columns = std::move(columns_lst);

                    LOG_TRACE(log, "Inserting ({} / {}): {}", ps, size, queryToString(*insert));

                    try
                    {
                        InterpreterInsertQuery interpreter_insert(ins_ast, context);
                        auto io = interpreter_insert.execute();
                        CompletedPipelineExecutor executor(io.pipeline);
                        executor.execute();
                    }
                    catch (DB::Exception &)
                    {
                        //if (e.code() != ErrorCodes::NOT_IMPLEMENTED && e.code() != ErrorCodes::ILLEGAL_COLUMN && e.code() != ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK && INCORRECT_DATA)
                        //    throw;
                    }
                }

            }
            catch (DB::Exception &)
            {
                tryLogCurrentException(log);
                throw;
            }
        }

        return result_type->createColumnConst(input_rows_count, Tuple{Field{""}, Field{0}});
    }

    ContextMutablePtr context;
};

REGISTER_FUNCTION(SanityCheck)
{
    factory.registerFunction<FunctionSanityCheck>();
}

}
