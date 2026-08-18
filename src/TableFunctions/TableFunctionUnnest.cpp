#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTFunction.h>
#include <Storages/StorageValues.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/registerTableFunctions.h>
#include <Common/typeid_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

/** unnest(array) explodes a constant array into one row per element.
  *
  * Empty arrays and NULL produce zero rows. Nested arrays are exploded one level:
  * the result column type is the array element type.
  *
  * Correlated uses such as `FROM t CROSS JOIN unnest(t.arr)` are rewritten to
  * ARRAY JOIN in the analyzer; this table function handles standalone
  * `FROM unnest([1, 2, 3])`.
  */
class TableFunctionUnnest : public ITableFunction
{
public:
    static constexpr auto name = "unnest";
    std::string getName() const override { return name; }
    bool hasStaticStructure() const override { return true; }

private:
    StoragePtr executeImpl(
        const ASTPtr & ast_function,
        ContextPtr context,
        const std::string & table_name,
        ColumnsDescription cached_columns,
        bool is_insert_query) const override;

    const char * getStorageEngineName() const override
    {
        /// It is StorageValues, which is not registered as a table engine.
        return "";
    }

    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;
    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;

    DataTypePtr unnest_type;
    ColumnPtr unnest_column;
};

void TableFunctionUnnest::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    const auto * function = ast_function->as<ASTFunction>();
    if (!function || !function->arguments || function->arguments->children.size() != 1)
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Table function '{}' requires exactly one Array or Nullable(Array) argument",
            getName());

    const auto [column, type] = evaluateConstantExpressionAsColumn(function->arguments->children[0], context);

    DataTypePtr argument_type = type;
    ColumnPtr argument_column = column->convertToFullColumnIfConst();

    bool is_null = false;
    if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(argument_type.get()))
    {
        argument_type = nullable_type->getNestedType();
        const auto * nullable_column = typeid_cast<const ColumnNullable *>(argument_column.get());
        if (!nullable_column)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Nullable type of table function '{}' argument does not match the column", getName());

        is_null = nullable_column->isNullAt(0);
        argument_column = nullable_column->getNestedColumnPtr()->convertToFullColumnIfConst();
    }

    const auto * array_type = typeid_cast<const DataTypeArray *>(argument_type.get());
    if (!array_type)
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Table function '{}' requires an Array or Nullable(Array) argument, got {}",
            getName(),
            type->getName());

    unnest_type = array_type->getNestedType();

    if (is_null)
    {
        unnest_column = unnest_type->createColumn();
        return;
    }

    const auto * array_column = typeid_cast<const ColumnArray *>(argument_column.get());
    if (!array_column)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Array type of table function '{}' argument does not match the column", getName());

    unnest_column = array_column->getDataPtr();
}

ColumnsDescription TableFunctionUnnest::getActualTableStructure(ContextPtr /*context*/, bool /*is_insert_query*/) const
{
    if (!unnest_type)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Table function '{}' arguments are not parsed", getName());

    return ColumnsDescription{{{name, unnest_type}}};
}

StoragePtr TableFunctionUnnest::executeImpl(
    const ASTPtr & /*ast_function*/,
    ContextPtr context,
    const std::string & table_name,
    ColumnsDescription /*cached_columns*/,
    bool is_insert_query) const
{
    auto columns = getActualTableStructure(context, is_insert_query);

    Block res_block;
    res_block.insert({unnest_column, unnest_type, name});

    auto res = std::make_shared<StorageValues>(StorageID(getDatabaseName(), table_name), columns, res_block);
    res->startup();
    return res;
}

}

void registerTableFunctionUnnest(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionUnnest>(
        {
            .description = R"(Explodes an array into a table with one row per element. Empty arrays and NULL produce zero rows. Nested arrays are exploded one level. For `FROM t CROSS JOIN unnest(t.arr)`, ClickHouse rewrites the query to `ARRAY JOIN`.)",
            .syntax = "unnest(array)",
            .arguments = {{"array", "The array to explode. Empty arrays and NULL produce zero rows.", {"Array"}}},
            .returned_value = {"A table with a single `unnest` column whose type is the array element type."},
            .examples = {{"Explode a constant array", "SELECT * FROM unnest([1, 2, 3]);", ""}},
            .introduced_in = {26, 8},
            .category = FunctionDocumentation::Category::TableFunction,
        },
        {.allow_readonly = true},
        TableFunctionFactory::Case::Insensitive);
}

}
