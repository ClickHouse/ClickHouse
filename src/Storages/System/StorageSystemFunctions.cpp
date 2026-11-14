#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <WindowFunctions/WindowFunctionFactory.h>
#include <Analyzer/FunctionNode.h>
#include <Storages/System/SystemTableSourceRegistry.h>
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
#include <Parsers/ASTCreateFunctionWithDriverQuery.h>
#include <Parsers/ASTCreateWasmFunctionQuery.h>
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
    template <typename Factory>
    void fillRow(
        MutableColumns & res_columns,
        const String & name,
        FunctionKind function_kind,
        const String & create_query,
        FunctionOrigin function_origin,
        const Factory & factory,
        ContextPtr context)
    {
        size_t i = 0;

        res_columns[i++]->insert(name);
        res_columns[i++]->insert(function_kind == FunctionKind::AGGREGATE);
        res_columns[i++]->insert(function_kind == FunctionKind::WINDOW);

        if constexpr (std::is_same_v<Factory, UserDefinedSQLFunctionFactory> || std::is_same_v<Factory, UserDefinedExecutableFunctionFactory>)
        {
            res_columns[i++]->insert(false);
            res_columns[i++]->insertDefault();
        }
        else
        {
            res_columns[i++]->insert(factory.isCaseInsensitive(name));
            if (factory.isAlias(name))
                res_columns[i++]->insert(factory.aliasTo(name));
            else
                res_columns[i++]->insertDefault();
        }

        res_columns[i++]->insert(create_query);
        res_columns[i++]->insert(static_cast<Int8>(function_origin));

        if constexpr (std::is_same_v<Factory, FunctionFactory> || std::is_same_v<Factory, AggregateFunctionFactory> || std::is_same_v<Factory, WindowFunctionFactory>)
        {
            if (factory.isAlias(name))
            {
                res_columns[i++]->insertDefault();
                res_columns[i++]->insertDefault();
                res_columns[i++]->insertDefault();
                res_columns[i++]->insertDefault();
                res_columns[i++]->insertDefault();
                res_columns[i++]->insertDefault();
                res_columns[i++]->insertDefault();
                res_columns[i++]->insertDefault();
            }
            else
            {
                auto documentation = factory.getDocumentation(name);
                res_columns[i++]->insert(documentation.description);
                res_columns[i++]->insert(documentation.syntaxAsString());
                res_columns[i++]->insert(documentation.argumentsAsString());
                res_columns[i++]->insert(documentation.parametersAsString());
                res_columns[i++]->insert(documentation.returnedValueAsString());
                res_columns[i++]->insert(documentation.examplesAsString());
                res_columns[i++]->insert(documentation.introducedInAsString());
                res_columns[i++]->insert(documentation.categoryAsString());
            }
        }
        else
        {
            res_columns[i++]->insertDefault();
            res_columns[i++]->insertDefault();
            res_columns[i++]->insertDefault();
            res_columns[i++]->insertDefault();
            res_columns[i++]->insertDefault();
            res_columns[i++]->insertDefault();
            res_columns[i++]->insertDefault();
            res_columns[i++]->insertDefault();
        }

        /// Declarative signature (see IFunction::getSignatureString) and the
        /// deterministic / higher-order flags. Only regular functions in the
        /// system FunctionFactory carry these; for everything else (aggregates,
        /// user-defined, WASM) leave the cells empty. Aliases resolve to their
        /// canonical function below and inherit its metadata.
        String signature;
        bool inserted_function_flags = false;
        if constexpr (std::is_same_v<Factory, FunctionFactory>)
        {
            try
            {
                /// `fillData` runs under `SuppressQueryFactoriesInfoScope`, so the normal
                /// resolution path does not register names in `query_log.used_functions`.
                /// `tryGet` resolves aliases to the canonical function (so aliases inherit
                /// its metadata) and honors `use_legacy_to_time`, unlike a duplicated lookup.
                if (auto resolver = factory.tryGet(name, context))
                {
                    signature = resolver->getSignatureString();
                    const UInt8 is_deterministic = resolver->isDeterministic();
                    const UInt8 is_higher_order = resolver->isHigherOrderFunction();
                    res_columns[i++]->insert(is_deterministic);
                    res_columns[i++]->insert(is_higher_order);
                    inserted_function_flags = true;
                }
            }
            catch (...)
            {
                /// Some functions throw on construction unless certain settings are set
                /// (e.g. deprecated/error-prone functions). Treat them as having no
                /// signature and unknown determinism / higher-order flags.
                LOG_DEBUG(
                    getLogger("system.functions"),
                    "Cannot resolve function {} for introspection: {}",
                    name,
                    getCurrentExceptionMessage(/* with_stacktrace */ false));
            }
        }
        if (!inserted_function_flags)
        {
            res_columns[i++]->insertDefault();
            res_columns[i++]->insertDefault();
        }
        res_columns[i++]->insert(signature);
    }

}


static std::vector<std::pair<String, Int8>> getOriginEnumsValues()
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
        {"is_window", std::make_shared<DataTypeUInt8>(), "Whether the function is a window function that can only be used with an `OVER` clause."},
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
        {"deterministic", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>()),
            "Whether the function returns the same result for the same arguments. NULL when unknown (e.g. aggregate or user-defined functions)."},
        {"higher_order", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>()),
            "Whether the function is higher-order — i.e. accepts at least one lambda expression as an argument (e.g. arrayMap, arrayFilter, mapApply). NULL when unknown."},
        {"signature", std::make_shared<DataTypeString>(), "Declarative signature of the function, when available."}
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
        fillRow(res_columns, function_name, FunctionKind::ORDINARY, "", FunctionOrigin::SYSTEM, functions_factory, context);
    }

    const auto & aggregate_functions_factory = AggregateFunctionFactory::instance();
    const auto & aggregate_function_names = aggregate_functions_factory.getAllRegisteredNames();
    for (const auto & function_name : aggregate_function_names)
    {
        fillRow(res_columns, function_name, FunctionKind::AGGREGATE, "", FunctionOrigin::SYSTEM, aggregate_functions_factory, context);
    }

    const auto & window_functions_factory = WindowFunctionFactory::instance();
    const auto & window_function_names = window_functions_factory.getAllRegisteredNames();
    for (const auto & function_name : window_function_names)
    {
        fillRow(res_columns, function_name, FunctionKind::WINDOW, "", FunctionOrigin::SYSTEM, window_functions_factory, context);
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
        /// When the runtime registry has the function, it is emitted separately below with full
        /// signature information; skip it here to avoid duplicates. A stored definition without a
        /// runtime registration (e.g. after a restart on a build without a WebAssembly engine or
        /// with the feature turned off) must stay visible, so that the leftover can be discovered
        /// and removed with `DROP FUNCTION`.
        if (ast && ast->as<ASTCreateWasmFunctionQuery>())
        {
            if (UserDefinedWebAssemblyFunctionFactory::instance().has(function_name))
                continue;

            size_t i = 0;
            res_columns[i++]->insert(function_name);
            res_columns[i++]->insert(UInt64(0)); // is_aggregate
            res_columns[i++]->insert(UInt8{0}); // is_window
            res_columns[i++]->insert(false); // case_insensitive
            res_columns[i++]->insertDefault(); // alias_to
            res_columns[i++]->insert(format({context, *ast}));
            res_columns[i++]->insert(static_cast<Int8>(FunctionOrigin::WASM_USER_DEFINED));
            res_columns[i++]->insertDefault(); // description
            res_columns[i++]->insertDefault(); // syntax
            res_columns[i++]->insertDefault(); // arguments
            res_columns[i++]->insertDefault(); // parameters
            res_columns[i++]->insertDefault(); // returned_value
            res_columns[i++]->insertDefault(); // examples
            res_columns[i++]->insertDefault(); // introduced_in
            res_columns[i++]->insertDefault(); // categories
            res_columns[i++]->insertDefault(); // is_deterministic
            res_columns[i++]->insert(UInt8{0}); // higher_order
            res_columns[i++]->insertDefault(); // signature
            continue;
        }

        /// The same applies to driver-created executable functions: they are materialized
        /// in the executable UDF loader and emitted with the `ExecutableUserDefined` origin below.
        if (ast && ast->as<ASTCreateFunctionWithDriverQuery>())
            continue;

        String create_query;
        if (ast)
            create_query = format({context, *ast});
        fillRow(res_columns, function_name, FunctionKind::ORDINARY, create_query, FunctionOrigin::SQL_USER_DEFINED, user_defined_sql_functions_factory, context);
    }

    const auto & user_defined_executable_functions_factory = UserDefinedExecutableFunctionFactory::instance();
    const auto & user_defined_executable_functions_names = user_defined_executable_functions_factory.getRegisteredNames(context); /// NOLINT(readability-static-accessed-through-instance)
    for (const auto & function_name : user_defined_executable_functions_names)
    {
        fillRow(res_columns, function_name, FunctionKind::ORDINARY, "", FunctionOrigin::EXECUTABLE_USER_DEFINED, user_defined_executable_functions_factory, context);
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

        size_t i = 0;
        res_columns[i++]->insert(registered.sql_name);
        res_columns[i++]->insert(UInt64(0)); // is_aggregate
        res_columns[i++]->insert(UInt8{0}); // is_window
        res_columns[i++]->insert(false); // case_insensitive
        res_columns[i++]->insertDefault(); // alias_to
        res_columns[i++]->insert(create_query);
        res_columns[i++]->insert(static_cast<Int8>(FunctionOrigin::WASM_USER_DEFINED));
        res_columns[i++]->insertDefault(); // description
        res_columns[i++]->insert(syntax);
        res_columns[i++]->insert(arguments_str);
        res_columns[i++]->insertDefault(); // parameters
        res_columns[i++]->insert(returned_value_str);
        res_columns[i++]->insertDefault(); // examples
        res_columns[i++]->insertDefault(); // introduced_in
        res_columns[i++]->insertDefault(); // categories
        res_columns[i++]->insertDefault(); // is_deterministic
        res_columns[i++]->insert(UInt8{0}); // higher_order
        res_columns[i++]->insertDefault(); // signature
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

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemFunctions) }
