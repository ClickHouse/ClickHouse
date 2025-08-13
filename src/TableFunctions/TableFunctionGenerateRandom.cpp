#include <Common/Exception.h>

#include <Storages/StorageGenerateRandom.h>
#include <Storages/checkAndGetLiteralArgument.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>

#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Functions/FunctionGenerateRandomStructure.h>
#include <Interpreters/parseColumnsListForTableFunction.h>
#include <Interpreters/evaluateConstantExpression.h>

#include <Common/randomSeed.h>

#include "registerTableFunctions.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int LOGICAL_ERROR;
}

namespace
{

/* generateRandom([structure, max_array_length, max_string_length, random_seed])
 * - creates a temporary storage that generates columns with random data
 */
class TableFunctionGenerateRandom : public ITableFunction
{
public:
    static constexpr auto name = "generateRandom";
    std::string getName() const override { return name; }
    bool hasStaticStructure() const override { return structure != "auto"; }

    bool needStructureHint() const override { return structure == "auto"; }
    void setStructureHint(const ColumnsDescription & structure_hint_) override { structure_hint = structure_hint_; }

private:
    StoragePtr executeImpl(const ASTPtr & ast_function, ContextPtr context, const std::string & table_name, ColumnsDescription cached_columns, bool is_insert_query) const override;
    const char * getStorageTypeName() const override { return "GenerateRandom"; }

    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;
    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    String structure = "auto";
    UInt64 max_string_length = 10;
    UInt64 max_array_length = 10;
    std::optional<UInt64> random_seed;
    ColumnsDescription structure_hint;
};

void TableFunctionGenerateRandom::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    ASTs & args_func = ast_function->children;

    if (args_func.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Table function '{}' must have arguments.", getName());

    ASTs & args = args_func.at(0)->children;

    if (args.empty())
        return;

    /// First, check if first argument is structure or seed.
    const auto * first_arg_literal = args[0]->as<const ASTLiteral>();
    bool first_argument_is_structure = !first_arg_literal || first_arg_literal->value.getType() == Field::Types::String;
    size_t max_args = first_argument_is_structure ? 4 : 3;

    if (args.size() > max_args)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                        "Table function '{}' requires at most four (or three if structure is missing) arguments: "
                        " [structure, random_seed, max_string_length, max_array_length].", getName());

    if (first_argument_is_structure)
    {
        /// Allow constant expression for structure argument, it can be generated using generateRandomStructure function.
        args[0] = evaluateConstantExpressionAsLiteral(args[0], context);
    }

    // All the arguments must be literals.
    for (const auto & arg : args)
    {
        const IAST * arg_raw = arg.get();
        if (const auto * func = arg_raw->as<const ASTFunction>(); func && func->name == "_CAST")
            arg_raw = func->arguments->children.at(0).get();

        if (!arg_raw->as<const ASTLiteral>())
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "All arguments of table function '{}' except structure argument must be literals. "
                "Got '{}' instead", getName(), arg->formatForErrorMessage());
        }
    }

    size_t arg_index = 0;

    if (first_argument_is_structure)
    {
        /// Parsing first argument as table structure and creating a sample block
        structure = checkAndGetLiteralArgument<String>(args[arg_index], "structure");
        ++arg_index;
    }

    if (args.size() >= arg_index + 1)
    {
        const IAST * arg_raw = args[arg_index].get();
        if (const auto * func = arg_raw->as<const ASTFunction>(); func && func->name == "_CAST")
            arg_raw = func->arguments->children.at(0).get();

        const auto & literal = arg_raw->as<const ASTLiteral &>();
        ++arg_index;
        if (!literal.value.isNull())
            random_seed = checkAndGetLiteralArgument<UInt64>(literal, "random_seed");
    }

    if (args.size() >= arg_index + 1)
    {
        max_string_length = checkAndGetLiteralArgument<UInt64>(args[arg_index], "max_string_length");
        ++arg_index;
    }

    if (args.size() == arg_index + 1)
    {
        max_array_length = checkAndGetLiteralArgument<UInt64>(args[arg_index], "max_string_length");
        ++arg_index;
    }
}

ColumnsDescription TableFunctionGenerateRandom::getActualTableStructure(ContextPtr context, bool /*is_insert_query*/) const
{
    if (structure == "auto")
    {
        if (structure_hint.empty())
        {
            auto random_structure = FunctionGenerateRandomStructure::generateRandomStructure(random_seed.value_or(randomSeed()), context);
            return parseColumnsListFromString(random_structure, context);
        }

        return structure_hint;
    }

    return parseColumnsListFromString(structure, context);
}

StoragePtr TableFunctionGenerateRandom::executeImpl(const ASTPtr & /*ast_function*/, ContextPtr context, const std::string & table_name, ColumnsDescription /*cached_columns*/, bool is_insert_query) const
{
    ColumnsDescription columns = getActualTableStructure(context, is_insert_query);
    auto res = std::make_shared<StorageGenerateRandom>(
        StorageID(getDatabaseName(), table_name), columns, String{}, max_array_length, max_string_length, random_seed);
    res->startup();
    return res;
}

}

void registerTableFunctionGenerate(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionGenerateRandom>({.documentation = {}, .allow_readonly = true});
}

}
