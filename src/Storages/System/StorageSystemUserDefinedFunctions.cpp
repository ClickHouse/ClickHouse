#include <Storages/System/StorageSystemUserDefinedFunctions.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnArray.h>
#include <Interpreters/Context.h>
#include <Common/ExternalLoaderStatus.h>
#include <Functions/UserDefined/ExternalUserDefinedExecutableFunctionsLoader.h>
#include <Functions/UserDefined/UserDefinedExecutableFunction.h>
#include <Processors/Sources/ShellCommandSource.h>
#include <Common/Exception.h>


namespace DB
{

StorageSystemUserDefinedFunctions::StorageSystemUserDefinedFunctions(
    const StorageID & storage_id_, ColumnsDescription columns_description_)
    : IStorageSystemOneBlock(storage_id_, std::move(columns_description_))
{
}

ColumnsDescription StorageSystemUserDefinedFunctions::getColumnsDescription()
{
    return ColumnsDescription
    {
        // ===== System/Non-Config Fields (External Loader metadata) =====
        {"name", std::make_shared<DataTypeString>(),
            "UDF name."},
        {"load_status", std::make_shared<DataTypeEnum8>(
            DataTypeEnum8::Values{
                {"Success", 0},
                {"Failed", 1}
            }),
            "Loading status. Possible values: "
            "Success — UDF loaded and ready to use, "
            "Failed — UDF failed to load (see field 'loading_error_message' for details)."},
        {"loading_error_message", std::make_shared<DataTypeString>(),
            "Detailed error message when loading failed. Empty if loaded successfully."},
        {"last_successful_update_time", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime>()),
            "Timestamp of the last successful update. NULL if never succeeded."},
        {"loading_duration_ms", std::make_shared<DataTypeUInt64>(),
            "Time spent loading the UDF, in milliseconds."},

        // ===== UDF Configuration Fields (from XML config) =====
        {"type", std::make_shared<DataTypeEnum8>(
            DataTypeEnum8::Values{
                {"executable", 0},
                {"executable_pool", 1}
            }),
            "UDF type: 'executable' (single process) or 'executable_pool' (process pool)."},
        {"command", std::make_shared<DataTypeString>(),
            "Script or command to execute for this UDF."},
        {"format", std::make_shared<DataTypeString>(),
            "Data format for I/O (e.g., 'TabSeparated', 'JSONEachRow')."},
        {"return_type", std::make_shared<DataTypeString>(),
            "Function return type (e.g., 'String', 'UInt64')."},
        {"return_name", std::make_shared<DataTypeString>(),
            "Optional return value identifier. Empty if not configured."},
        {"argument_types", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()),
            "Array of argument types (e.g., ['String', 'UInt64'])."},
        {"argument_names", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()),
            "Array of argument names. Empty strings for unnamed arguments."},
        {"max_command_execution_time", std::make_shared<DataTypeUInt64>(),
            "Maximum seconds to process a data block. Only for 'executable_pool' type."},
        {"command_termination_timeout", std::make_shared<DataTypeUInt64>(),
            "Seconds before sending SIGTERM to command process."},
        {"command_read_timeout", std::make_shared<DataTypeUInt64>(),
            "Milliseconds for reading from command stdout."},
        {"command_write_timeout", std::make_shared<DataTypeUInt64>(),
            "Milliseconds for writing to command stdin."},
        {"pool_size", std::make_shared<DataTypeUInt64>(),
            "Number of command process instances. Only for 'executable_pool' type."},
        {"send_chunk_header", std::make_shared<DataTypeUInt8>(),
            "Whether to send row count before each data chunk (boolean)."},
        {"execute_direct", std::make_shared<DataTypeUInt8>(),
            "Whether to execute command directly (1) or via /bin/bash (0)."},
        {"lifetime", std::make_shared<DataTypeUInt64>(),
            "Reload interval in seconds. 0 means reload is disabled."},
        {"deterministic", std::make_shared<DataTypeUInt8>(),
            "Whether function returns the same result for the same arguments (boolean)."}
    };
}

void StorageSystemUserDefinedFunctions::fillData(
    MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    const auto & external_executable_functions_loader = context->getExternalUserDefinedExecutableFunctionsLoader();

    for (const auto & load_result : external_executable_functions_loader.getLoadResults())
    {
        size_t i = 0;

        // ===== System/Non-Config Fields =====
        res_columns[i++]->insert(load_result.name);

        // Map ExternalLoaderStatus to simplified Success/Failed enum
        // Success (0): LOADED, LOADED_AND_RELOADING
        // Failed (1): All other states (NOT_LOADED, FAILED, LOADING, FAILED_AND_RELOADING, NOT_EXIST)
        Int8 simplified_status = (load_result.status == ExternalLoaderStatus::LOADED ||
                                  load_result.status == ExternalLoaderStatus::LOADED_AND_RELOADING) ? 0 : 1;
        res_columns[i++]->insert(simplified_status);

        if (load_result.exception)
            res_columns[i++]->insert(getExceptionMessage(load_result.exception, false));
        else
            res_columns[i++]->insertDefault();

        // last_successful_update_time - NULL if never succeeded
        if (simplified_status == 0)  // Success
        {
            res_columns[i++]->insert(static_cast<UInt64>(
                std::chrono::system_clock::to_time_t(load_result.last_successful_update_time)));
        }
        else
        {
            res_columns[i++]->insertDefault();  // NULL
        }

        // loading_duration_ms - convert to milliseconds
        res_columns[i++]->insert(
            std::chrono::duration_cast<std::chrono::milliseconds>(load_result.loading_duration).count());

        // ===== UDF Configuration Fields =====
        const auto udf_ptr = std::dynamic_pointer_cast<const UserDefinedExecutableFunction>(load_result.object);

        if (udf_ptr)
        {
            const auto & config = udf_ptr->getConfiguration();
            const auto coordinator = udf_ptr->getCoordinator();
            const auto & exec_config = coordinator->getConfiguration();

            // Type enum: 0 = executable, 1 = executable_pool
            res_columns[i++]->insert(exec_config.is_executable_pool ? Int8(1) : Int8(0));

            // Reconstruct full command with arguments for user visibility
            String full_command = config.command;
            if (!config.command_arguments.empty())
            {
                for (const auto & arg : config.command_arguments)
                    full_command += " " + arg;
            }
            res_columns[i++]->insert(full_command);

            res_columns[i++]->insert(exec_config.format);
            res_columns[i++]->insert(config.result_type->getName());
            res_columns[i++]->insert(config.result_name);

            Array argument_types_array;
            for (const auto & arg : config.arguments)
                argument_types_array.push_back(arg.type->getName());
            res_columns[i++]->insert(argument_types_array);

            Array argument_names_array;
            for (const auto & arg : config.arguments)
                argument_names_array.push_back(arg.name);
            res_columns[i++]->insert(argument_names_array);

            res_columns[i++]->insert(exec_config.max_command_execution_time_seconds);
            res_columns[i++]->insert(exec_config.command_termination_timeout_seconds);
            res_columns[i++]->insert(exec_config.command_read_timeout_milliseconds);
            res_columns[i++]->insert(exec_config.command_write_timeout_milliseconds);
            res_columns[i++]->insert(exec_config.pool_size);
            res_columns[i++]->insert(exec_config.send_chunk_header ? 1 : 0);
            res_columns[i++]->insert(exec_config.execute_direct ? 1 : 0);

            // Lifetime has min/max range for jitter, we use max_sec as representative value
            const auto & lifetime = udf_ptr->getLifetime();
            res_columns[i++]->insert(lifetime.max_sec);

            res_columns[i++]->insert(config.is_deterministic ? 1 : 0);
        }
        else
        {
            // Failed to load - configuration unavailable, insert defaults for all config fields
            // Config fields: type, command, format, return_type, return_name, argument_types, argument_names,
            // max_command_execution_time, command_termination_timeout, command_read_timeout, command_write_timeout,
            // pool_size, send_chunk_header, execute_direct, lifetime, deterministic
            constexpr size_t config_fields_count = 16;
            for (size_t j = 0; j < config_fields_count; ++j)
                res_columns[i++]->insertDefault();
        }
    }
}

}
