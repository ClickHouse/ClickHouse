#include <TableFunctions/TableFunctionFactory.h>
#include <Formats/FormatFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/parseColumnsListForTableFunction.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Processors/Formats/ISchemaReader.h>
#include <Common/Exception.h>
#include <Storages/StorageInput.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <DataTypes/DataTypeFactory.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <TableFunctions/registerTableFunctions.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int CANNOT_EXTRACT_TABLE_STRUCTURE;
}

namespace
{

/* input(structure) - allows to make INSERT SELECT from incoming stream of data
 */
class TableFunctionInput : public ITableFunction
{
public:
    static constexpr auto name = "input";
    std::string getName() const override { return name; }
    bool hasStaticStructure() const override { return true; }
    bool needStructureHint() const override { return true; }
    void setStructureHint(const ColumnsDescription & structure_hint_) override { structure_hint = structure_hint_; }

private:
    StoragePtr executeImpl(const ASTPtr & ast_function, ContextPtr context, const std::string & table_name, ColumnsDescription cached_columns, bool is_insert_query) const override;
    const char * getStorageEngineName() const override
    {
        /// Technically it's Input but it doesn't register itself
        return "";
    }

    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;
    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    String structure;
    ColumnsDescription structure_hint;
};

void TableFunctionInput::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    const auto * function = ast_function->as<ASTFunction>();

    if (!function->arguments)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Table function '{}' must have arguments", getName());

    auto args = function->arguments->children;

    if (args.empty())
    {
        structure = "auto";
        return;
    }

    if (args.size() != 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Table function '{}' requires exactly 1 argument: structure", getName());

    structure = checkAndGetLiteralArgument<String>(evaluateConstantExpressionOrIdentifierAsLiteral(args[0], context), "structure");
}

ColumnsDescription TableFunctionInput::getActualTableStructure(ContextPtr context, bool /*is_insert_query*/) const
{
    if (structure == "auto")
    {
        /// The analyzer may have already set a structure hint by mapping
        /// destination-table columns to identifier expressions in the surrounding
        /// `INSERT ... SELECT` (see `Context::executeTableFunction` and
        /// `QueryAnalyzer::resolveTableFunction`). Honour that hint first — the
        /// user's destination column names take precedence over a format's
        /// fixed schema, so queries like
        ///     INSERT INTO t (data) SELECT data FROM input() FORMAT LineAsString
        /// keep working even though `LineAsString` declares its column as `line`.
        if (!structure_hint.empty())
            return structure_hint;

        /// No analyzer hint. If the surrounding `INSERT` specifies a fixed-schema
        /// format (`LineAsString`, `RawBLOB`, `JSONAsString`, ...), derive the
        /// structure from the format. This handles the case from #104532 where
        /// the SELECT list cannot be matched to the destination columns by name
        /// (e.g. literals projected before the input column), so `structure_hint`
        /// is left empty.
        const auto & insert_format = context->getInsertFormat();
        if (!insert_format.empty())
        {
            const auto & factory = FormatFactory::instance();
            if (factory.checkIfFormatHasExternalSchemaReader(insert_format))
            {
                auto external_schema_reader = factory.getExternalSchemaReader(insert_format, context);
                return ColumnsDescription(external_schema_reader->readSchema());
            }
        }

        throw Exception(
            ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE,
            "Table function '{}' was used without structure argument but structure could not be determined automatically. Please, "
            "provide structure manually",
            getName());
    }
    return parseColumnsListFromString(structure, context);
}

StoragePtr TableFunctionInput::executeImpl(const ASTPtr & /*ast_function*/, ContextPtr context, const std::string & table_name, ColumnsDescription /*cached_columns*/, bool is_insert_query) const
{
    auto storage = std::make_shared<StorageInput>(StorageID(getDatabaseName(), table_name), getActualTableStructure(context, is_insert_query));
    storage->startup();
    return storage;
}

}

void registerTableFunctionInput(TableFunctionFactory & factory)
{
    /// `input` only reads the data stream that the client is sending alongside the INSERT query;
    /// it does not access arbitrary hosts or storages, so it does not need the `CREATE TEMPORARY TABLE` grant
    /// and is allowed in readonly mode.
    factory.registerFunction<TableFunctionInput>({}, {.allow_readonly = true});
}

}
