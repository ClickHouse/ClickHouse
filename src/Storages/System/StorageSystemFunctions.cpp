#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeEnum.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Interpreters/formatWithPossiblyHidingSecrets.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Functions/UserDefined/UserDefinedExecutableFunctionFactory.h>
#include <Functions/UserDefined/UserDefinedWebAssembly.h>
#include <Parsers/ASTCreateSQLFunctionQuery.h>
#include <Parsers/ASTCreateWasmFunctionQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Storages/System/StorageSystemFunctions.h>
#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_FUNCTION;
}

enum class FunctionOrigin : int8_t
{
    SYSTEM = 0,
    SQL_USER_DEFINED = 1,
    EXECUTABLE_USER_DEFINED = 2,
    WASM_USER_DEFINED = 3,
};

namespace
{
    /// Per-function metadata that cannot be obtained without resolving / parsing the function.
    /// `nullopt` means "unknown" and is reported as NULL in the result column.
    struct ExtraInfo
    {
        std::optional<UInt64> min_arguments;
        std::optional<UInt64> max_arguments;
        std::optional<UInt8> is_deterministic;
        std::optional<UInt8> higher_order_function;
    };

    template <typename Factory>
    void fillRow(
        MutableColumns & res_columns,
        const String & name,
        UInt64 is_aggregate,
        const String & create_query,
        FunctionOrigin function_origin,
        const Factory & factory,
        const ExtraInfo & extra)
    {
        res_columns[0]->insert(name);
        res_columns[1]->insert(is_aggregate);

        if constexpr (std::is_same_v<Factory, UserDefinedSQLFunctionFactory> || std::is_same_v<Factory, UserDefinedExecutableFunctionFactory>)
        {
            res_columns[2]->insert(false);
            res_columns[3]->insertDefault();
        }
        else
        {
            res_columns[2]->insert(factory.isCaseInsensitive(name));
            if (factory.isAlias(name))
                res_columns[3]->insert(factory.aliasTo(name));
            else
                res_columns[3]->insertDefault();
        }

        res_columns[4]->insert(create_query);
        res_columns[5]->insert(static_cast<Int8>(function_origin));

        if constexpr (std::is_same_v<Factory, FunctionFactory> || std::is_same_v<Factory, AggregateFunctionFactory>)
        {
            if (factory.isAlias(name))
            {
                res_columns[6]->insertDefault();
                res_columns[7]->insertDefault();
                res_columns[8]->insertDefault();
                res_columns[9]->insertDefault();
                res_columns[10]->insertDefault();
                res_columns[11]->insertDefault();
                res_columns[12]->insertDefault();
                res_columns[13]->insertDefault();
            }
            else
            {
                auto documentation = factory.getDocumentation(name);
                res_columns[6]->insert(documentation.description);
                res_columns[7]->insert(documentation.syntaxAsString());
                res_columns[8]->insert(documentation.argumentsAsString());
                res_columns[9]->insert(documentation.parametersAsString());
                res_columns[10]->insert(documentation.returnedValueAsString());
                res_columns[11]->insert(documentation.examplesAsString());
                res_columns[12]->insert(documentation.introducedInAsString());
                res_columns[13]->insert(documentation.categoryAsString());
            }
        }
        else
        {
            res_columns[6]->insertDefault();
            res_columns[7]->insertDefault();
            res_columns[8]->insertDefault();
            res_columns[9]->insertDefault();
            res_columns[10]->insertDefault();
            res_columns[11]->insertDefault();
            res_columns[12]->insertDefault();
            res_columns[13]->insertDefault();
        }

        if (extra.min_arguments)
            res_columns[14]->insert(*extra.min_arguments);
        else
            res_columns[14]->insertDefault();

        if (extra.max_arguments)
            res_columns[15]->insert(*extra.max_arguments);
        else
            res_columns[15]->insertDefault();

        if (extra.is_deterministic)
            res_columns[16]->insert(*extra.is_deterministic);
        else
            res_columns[16]->insertDefault();

        if (extra.higher_order_function)
            res_columns[17]->insert(*extra.higher_order_function);
        else
            res_columns[17]->insertDefault();
    }

    /// Resolve an ordinary function and read static metadata from its overload resolver.
    /// Anything that throws or returns no resolver is reported as NULL — the function is
    /// genuinely unavailable to introspection without a richer query context.
    ExtraInfo getOrdinaryFunctionExtraInfo(const FunctionFactory & factory, const String & name, ContextPtr context)
    {
        ExtraInfo info;
        try
        {
            auto resolver = factory.tryGet(name, context);
            if (!resolver)
                return info;

            info.is_deterministic = resolver->isDeterministic() ? UInt8{1} : UInt8{0};
            info.higher_order_function = resolver->isHigherOrder() ? UInt8{1} : UInt8{0};

            if (!resolver->isVariadic())
            {
                const auto n = static_cast<UInt64>(resolver->getNumberOfArguments());
                info.min_arguments = n;
                info.max_arguments = n;
            }
            /// Variadic functions: the resolver does not expose a static [min, max] range,
            /// so leave both as NULL rather than guessing.
        }
        catch (...)
        {
            /// Some functions need a fully-formed query context to construct (e.g. those that
            /// inspect settings at build time). Reporting NULL is the honest answer; log the
            /// exception message at debug level so the failure is visible to anyone
            /// investigating empty cells.
            LOG_DEBUG(
                getLogger("system.functions"),
                "Cannot resolve function {} for introspection: {}",
                name,
                getCurrentExceptionMessage(/* with_stacktrace */ false));
        }
        return info;
    }

    /// SQL UDFs: arity is the size of the lambda's parameter list. Determinism cannot be
    /// inferred from the body in general, so leave it NULL.
    ///
    /// AST shape (mirrors UserDefinedSQLFunctionVisitor): function_core is the lambda
    /// ASTFunction; its first child is an ASTExpressionList with two entries — a `tuple`
    /// ASTFunction holding the parameter identifiers, and the body. The parameter count
    /// lives one more level down inside the tuple's arguments.
    ExtraInfo getSQLUserDefinedFunctionExtraInfo(const ASTPtr & ast)
    {
        ExtraInfo info;
        if (!ast)
            return info;

        const auto * create = ast->as<ASTCreateSQLFunctionQuery>();
        if (!create || !create->function_core || create->function_core->children.empty())
            return info;

        const auto & lambda_args = create->function_core->children.at(0);
        if (!lambda_args || lambda_args->children.empty())
            return info;

        const auto & param_tuple = lambda_args->children.at(0);
        if (!param_tuple || param_tuple->children.empty())
            return info;

        const auto * params = param_tuple->children.at(0)->as<ASTExpressionList>();
        if (!params)
            return info;

        const auto n = static_cast<UInt64>(params->children.size());
        info.min_arguments = n;
        info.max_arguments = n;
        return info;
    }
}


std::vector<std::pair<String, Int8>> getOriginEnumsValues()
{
    return std::vector<std::pair<String, Int8>>{
        {"System", static_cast<Int8>(FunctionOrigin::SYSTEM)},
        {"SQLUserDefined", static_cast<Int8>(FunctionOrigin::SQL_USER_DEFINED)},
        {"ExecutableUserDefined", static_cast<Int8>(FunctionOrigin::EXECUTABLE_USER_DEFINED)},
        {"WasmUserDefined", static_cast<Int8>(FunctionOrigin::WASM_USER_DEFINED)},
    };
}

ColumnsDescription StorageSystemFunctions::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"name", std::make_shared<DataTypeString>(), "The name of the function."},
        {"is_aggregate", std::make_shared<DataTypeUInt8>(), "Whether the function is an aggregate function."},
        {"case_insensitive", std::make_shared<DataTypeUInt8>(), "Whether the function name can be used case-insensitively."},
        {"alias_to", std::make_shared<DataTypeString>(), "The original function name, if the function name is an alias."},
        {"create_query", std::make_shared<DataTypeString>(), "Obsolete."},
        {"origin", std::make_shared<DataTypeEnum8>(getOriginEnumsValues()), "Obsolete."},
        {"description", std::make_shared<DataTypeString>(), "A high-level description what the function does."},
        {"syntax", std::make_shared<DataTypeString>(), "Signature of the function."},
        {"arguments", std::make_shared<DataTypeString>(), "The function arguments."},
        {"parameters", std::make_shared<DataTypeString>(), "The function parameters (only for aggregate function)."},
        {"returned_value", std::make_shared<DataTypeString>(), "What does the function return."},
        {"examples", std::make_shared<DataTypeString>(), "Usage example."},
        {"introduced_in", std::make_shared<DataTypeString>(), "ClickHouse version in which the function was first introduced."},
        {"categories", std::make_shared<DataTypeString>(), "The category of the function."},
        {"min_arguments", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()),
            "The minimum number of arguments the function accepts. NULL when unknown (e.g. variadic functions, aggregate functions, executable user-defined functions)."},
        {"max_arguments", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()),
            "The maximum number of arguments the function accepts. NULL when unbounded (variadic) or unknown."},
        {"is_deterministic", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>()),
            "Whether the function returns the same result for the same arguments. NULL when unknown (e.g. aggregate or user-defined functions)."},
        {"higher_order_function", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>()),
            "Whether the function is higher-order — i.e. accepts at least one lambda expression as an argument (e.g. arrayMap, arrayFilter, mapApply). NULL when unknown."}
    };
}

void StorageSystemFunctions::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    /// Resolving every function (and the helpers their constructors create internally —
    /// e.g. coalesce constructs isNotNull/assumeNotNull/if; map constructs array/mapFromArrays;
    /// monthName constructs dateName) would otherwise pollute query_log.used_functions for
    /// the user's query. Suppress factory accounting for the duration of the read.
    Context::SuppressQueryFactoriesInfoScope suppress_factory_info;

    const auto & functions_factory = FunctionFactory::instance();
    const auto & function_names = functions_factory.getAllRegisteredNames();
    for (const auto & function_name : function_names)
    {
        const ExtraInfo extra = getOrdinaryFunctionExtraInfo(functions_factory, function_name, context);
        fillRow(res_columns, function_name, 0, "", FunctionOrigin::SYSTEM, functions_factory, extra);
    }

    const auto & aggregate_functions_factory = AggregateFunctionFactory::instance();
    const auto & aggregate_function_names = aggregate_functions_factory.getAllRegisteredNames();
    for (const auto & function_name : aggregate_function_names)
    {
        /// Aggregate functions need argument types and parameters to instantiate, so static
        /// arity / determinism are not available. Report NULL rather than guess.
        fillRow(res_columns, function_name, 1, "", FunctionOrigin::SYSTEM, aggregate_functions_factory, ExtraInfo{});
    }

    const auto & user_defined_sql_functions_factory = UserDefinedSQLFunctionFactory::instance();
    const auto & user_defined_sql_functions_names = user_defined_sql_functions_factory.getAllRegisteredNames();
    for (const auto & function_name : user_defined_sql_functions_names)
    {
        ASTPtr ast;
        try
        {
            ast = user_defined_sql_functions_factory.get(function_name);
        }
        catch (const Exception & e)
        {
            if (e.code() == ErrorCodes::UNKNOWN_FUNCTION)
                tryLogCurrentException(getLogger("system.functions"), fmt::format("Function {} does not exist", function_name), LogsLevel::debug);
            else
                throw;
        }
        /// WASM functions are stored in the same SQL objects storage but have their own origin.
        /// They are emitted separately below; skip them here to avoid duplicates.
        if (ast && ast->as<ASTCreateWasmFunctionQuery>())
            continue;

        String create_query;
        if (ast)
            create_query = format({context, *ast});
        const ExtraInfo extra = getSQLUserDefinedFunctionExtraInfo(ast);
        fillRow(res_columns, function_name, 0, create_query, FunctionOrigin::SQL_USER_DEFINED, user_defined_sql_functions_factory, extra);
    }

    const auto & user_defined_executable_functions_factory = UserDefinedExecutableFunctionFactory::instance();
    const auto & user_defined_executable_functions_names = user_defined_executable_functions_factory.getRegisteredNames(context); /// NOLINT(readability-static-accessed-through-instance)
    for (const auto & function_name : user_defined_executable_functions_names)
    {
        fillRow(res_columns, function_name, 0, "", FunctionOrigin::EXECUTABLE_USER_DEFINED, user_defined_executable_functions_factory, ExtraInfo{});
    }

    const auto & wasm_functions_factory = UserDefinedWebAssemblyFunctionFactory::instance();
    for (const auto & registered : wasm_functions_factory.getAllFunctions())
    {
        const auto & func = *registered.function;
        const auto & arg_names = func.getArgumentNames();
        const auto & arg_types = func.getArguments();
        const auto & result_type = func.getResultType();

        String create_query;
        if (registered.create_query)
            create_query = format({context, *registered.create_query});

        String syntax = registered.sql_name + "(";
        String arguments_str;
        for (size_t i = 0; i < arg_types.size(); ++i)
        {
            if (i > 0)
                syntax += ", ";
            const String & arg_name = i < arg_names.size() ? arg_names[i] : ("arg" + std::to_string(i + 1));
            const String type_name = arg_types[i]->getName();
            syntax += arg_name + " " + type_name;
            arguments_str += "- `" + arg_name + "` — " + type_name + "\n";
        }
        syntax += ")";

        String returned_value_str;
        if (result_type)
        {
            syntax += " -> " + result_type->getName();
            returned_value_str = result_type->getName();
        }

        res_columns[0]->insert(registered.sql_name);
        res_columns[1]->insert(UInt64(0)); // is_aggregate
        res_columns[2]->insert(false); // case_insensitive
        res_columns[3]->insertDefault(); // alias_to
        res_columns[4]->insert(create_query);
        res_columns[5]->insert(static_cast<Int8>(FunctionOrigin::WASM_USER_DEFINED));
        res_columns[6]->insertDefault(); // description
        res_columns[7]->insert(syntax);
        res_columns[8]->insert(arguments_str);
        res_columns[9]->insertDefault(); // parameters
        res_columns[10]->insert(returned_value_str);
        res_columns[11]->insertDefault(); // examples
        res_columns[12]->insertDefault(); // introduced_in
        res_columns[13]->insertDefault(); // categories

        /// WASM functions have a fixed, declared arity from their CREATE FUNCTION signature.
        /// Determinism is unknown — the WASM module is opaque to ClickHouse — so report NULL.
        /// They cannot accept lambda parameters.
        const auto arity = static_cast<UInt64>(arg_types.size());
        res_columns[14]->insert(arity); // min_arguments
        res_columns[15]->insert(arity); // max_arguments
        res_columns[16]->insertDefault(); // is_deterministic
        res_columns[17]->insert(UInt8{0}); // higher_order_function
    }
}

void StorageSystemFunctions::backupData(BackupEntriesCollector & backup_entries_collector, const String & data_path_in_backup, const std::optional<ASTs> & /* partitions */)
{
    UserDefinedSQLFunctionFactory::instance().backup(backup_entries_collector, data_path_in_backup);
}

void StorageSystemFunctions::restoreDataFromBackup(RestorerFromBackup & restorer, const String & data_path_in_backup, const std::optional<ASTs> & /* partitions */)
{
    UserDefinedSQLFunctionFactory::instance().restore(restorer, data_path_in_backup);
}

}
