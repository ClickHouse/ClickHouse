
// #pragma once

// #include "config.h"

// #if USE_AWS_S3
// #include <IO/S3Settings.h>
// #include <Storages/ObjectStorage/StorageObjectStorage.h>
// #include <Disks/ObjectStorages/S3/S3ObjectStorage.h>
// #include <Parsers/IAST_fwd.h>
// #include <Disks/ObjectStorages/IObjectStorage.h>
// #include <Storages/ObjectStorage/S3/StorageParsableArguments.h>

// namespace DB
// {


// void S3StorageParsableArguments::fromAST(ASTs & args, ContextPtr context, bool with_structure)
// {
//     auto extra_credentials = extractExtraCredentials(args);

//     size_t count = StorageURL::evalArgsAndCollectHeaders(args, headers_from_ast, context);

//     ASTs key_value_asts;
//     if (auto * first_key_value_arg_it = getFirstKeyValueArgument(args);
//         first_key_value_arg_it != args.end())
//     {
//         key_value_asts = ASTs(first_key_value_arg_it, args.end());
//         count -= key_value_asts.size();
//     }

//     if (count == 0 || count > getMaxNumberOfArguments(with_structure))
//         throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
//             "Storage S3 requires 1 to {} arguments. All supported signatures:\n{}",
//             getMaxNumberOfArguments(with_structure),
//             getSignatures(with_structure));

//     auto key_value_args = parseKeyValueArguments(key_value_asts, context);
//     if (key_value_args.contains("structure"))
//         with_structure = false;

//     const auto & config = context->getConfigRef();
//     s3_capabilities = std::make_unique<S3Capabilities>(getCapabilitiesFromConfig(config, "s3"));

//     std::unordered_map<std::string_view, size_t> engine_args_to_idx;
//     bool no_sign_request = false;

//     /// When adding new arguments in the signature don't forget to update addStructureAndFormatToArgsIfNeeded as well.

//     /// For 2 arguments we support:
//     /// - s3(source, format)
//     /// - s3(source, NOSIGN)
//     /// We can distinguish them by looking at the 2-nd argument: check if it's NOSIGN or not.
//     if (count == 2)
//     {
//         auto second_arg = checkAndGetLiteralArgument<String>(args[1], "format/NOSIGN");
//         if (boost::iequals(second_arg, "NOSIGN"))
//             no_sign_request = true;
//         else
//             engine_args_to_idx = {{"format", 1}};
//     }
//     /// For 3 arguments we support:
//     /// if with_structure == 0:
//     /// - s3(source, NOSIGN, format)
//     /// - s3(source, format, compression_method)
//     /// - s3(source, access_key_id, secret_access_key)
//     /// if with_structure == 1:
//     /// - s3(source, NOSIGN, format)
//     /// - s3(source, format, structure)
//     /// - s3(source, access_key_id, secret_access_key)
//     /// We can distinguish them by looking at the 2-nd argument: check if it's NOSIGN or format name.
//     else if (count == 3)
//     {
//         auto second_arg = checkAndGetLiteralArgument<String>(args[1], "format/access_key_id/NOSIGN");
//         if (boost::iequals(second_arg, "NOSIGN"))
//         {
//             no_sign_request = true;
//             engine_args_to_idx = {{"format", 2}};
//         }
//         else if (second_arg == "auto" || FormatFactory::instance().exists(second_arg))
//         {
//             if (with_structure)
//                 engine_args_to_idx = {{"format", 1}, {"structure", 2}};
//             else
//                 engine_args_to_idx = {{"format", 1}, {"compression_method", 2}};
//         }
//         else
//             engine_args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}};
//     }
//     /// For 4 arguments we support:
//     /// if with_structure == 0:
//     /// - s3(source, access_key_id, secret_access_key, session_token)
//     /// - s3(source, access_key_id, secret_access_key, format)
//     /// - s3(source, NOSIGN, format, compression_method)
//     /// if with_structure == 1:
//     /// - s3(source, format, structure, compression_method),
//     /// - s3(source, access_key_id, secret_access_key, format),
//     /// - s3(source, access_key_id, secret_access_key, session_token)
//     /// - s3(source, NOSIGN, format, structure)
//     /// We can distinguish them by looking at the 2-nd argument: check if it's a NOSIGN, format name of something else.
//     else if (count == 4)
//     {
//         auto second_arg = checkAndGetLiteralArgument<String>(args[1], "access_key_id/NOSIGN");
//         if (boost::iequals(second_arg, "NOSIGN"))
//         {
//             no_sign_request = true;
//             if (with_structure)
//                 engine_args_to_idx = {{"format", 2}, {"structure", 3}};
//             else
//                 engine_args_to_idx = {{"format", 2}, {"compression_method", 3}};
//         }
//         else if (with_structure && (second_arg == "auto" || FormatFactory::instance().exists(second_arg)))
//         {
//             engine_args_to_idx = {{"format", 1}, {"structure", 2}, {"compression_method", 3}};
//         }
//         else
//         {
//             auto fourth_arg = checkAndGetLiteralArgument<String>(args[3], "session_token/format");
//             if (fourth_arg == "auto" || FormatFactory::instance().exists(fourth_arg))
//             {
//                 engine_args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}, {"format", 3}};
//             }
//             else
//             {
//                 engine_args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}, {"session_token", 3}};
//             }
//         }
//     }
//     /// For 5 arguments we support:
//     /// if with_structure == 0:
//     /// - s3(source, access_key_id, secret_access_key, session_token, format)
//     /// - s3(source, access_key_id, secret_access_key, format, compression)
//     /// if with_structure == 1:
//     /// - s3(source, access_key_id, secret_access_key, format, structure)
//     /// - s3(source, access_key_id, secret_access_key, session_token, format)
//     /// - s3(source, NOSIGN, format, structure, compression_method)
//     else if (count == 5)
//     {
//         if (with_structure)
//         {
//             auto second_arg = checkAndGetLiteralArgument<String>(args[1], "NOSIGN/access_key_id");
//             if (boost::iequals(second_arg, "NOSIGN"))
//             {
//                 no_sign_request = true;
//                 engine_args_to_idx = {{"format", 2}, {"structure", 3}, {"compression_method", 4}};
//             }
//             else
//             {
//                 auto fourth_arg = checkAndGetLiteralArgument<String>(args[3], "format/session_token");
//                 if (fourth_arg == "auto" || FormatFactory::instance().exists(fourth_arg))
//                 {
//                     engine_args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}, {"format", 3}, {"structure", 4}};
//                 }
//                 else
//                 {
//                     engine_args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}, {"session_token", 3}, {"format", 4}};
//                 }
//             }
//         }
//         else
//         {
//             auto fourth_arg = checkAndGetLiteralArgument<String>(args[3], "session_token/format");
//             if (fourth_arg == "auto" || FormatFactory::instance().exists(fourth_arg))
//             {
//                 engine_args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}, {"format", 3}, {"compression_method", 4}};
//             }
//             else
//             {
//                 engine_args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}, {"session_token", 3}, {"format", 4}};
//             }
//         }
//     }
//     /// For 6 arguments we support:
//     /// if with_structure == 0:
//     /// - s3(source, access_key_id, secret_access_key, session_token, format, compression_method)
//     /// if with_structure == 1:
//     /// - s3(source, access_key_id, secret_access_key, format, structure, compression_method)
//     /// - s3(source, access_key_id, secret_access_key, session_token, format, structure)
//     else if (count == 6)
//     {
//         if (with_structure)
//         {
//             auto fourth_arg = checkAndGetLiteralArgument<String>(args[3], "format/session_token");
//             if (fourth_arg == "auto" || FormatFactory::instance().exists(fourth_arg))
//             {
//                 engine_args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}, {"format", 3}, {"structure", 4}, {"compression_method", 5}};
//             }
//             else
//             {
//                 engine_args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}, {"session_token", 3}, {"format", 4}, {"structure", 5}};
//             }
//         }
//         else
//         {
//             engine_args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}, {"session_token", 3}, {"format", 4}, {"compression_method", 5}};
//         }
//     }
//     /// For 7 arguments we support:
//     /// if with_structure == 0:
//     /// - s3(source, access_key_id, secret_access_key, session_token, format, compression_method, partition_strategy)
//     /// if with_structure == 1:
//     /// - s3(source, access_key_id, secret_access_key, session_token, format, structure, partition_strategy)
//     /// - s3(source, access_key_id, secret_access_key, session_token, format, structure, compression_method)
//     else if (count == 7)
//     {
//         if (with_structure)
//         {
//             auto sixth_arg = checkAndGetLiteralArgument<String>(args[6], "compression_method/partition_strategy");
//             if (magic_enum::enum_contains<PartitionStrategyFactory::StrategyType>(sixth_arg))
//             {
//                 engine_args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}, {"session_token", 3}, {"format", 4}, {"structure", 5}, {"partition_strategy", 6}};
//             }
//             else
//             {
//                 engine_args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}, {"session_token", 3}, {"format", 4}, {"structure", 5}, {"compression_method", 6}};
//             }
//         }
//         else
//         {
//             engine_args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}, {"session_token", 3}, {"format", 4}, {"compression_method", 5}, {"partition_strategy", 6}};
//         }
//     }
//     /// For 8 arguments we support:
//     /// if with_structure == 0:
//     /// - s3(source, access_key_id, secret_access_key, session_token, format, compression_method, partition_strategy, partition_columns_in_data_file)
//     /// if with_structure == 1:
//     /// - s3(source, access_key_id, secret_access_key, session_token, format, structure, partition_strategy, partition_columns_in_data_file)
//     /// - s3(source, access_key_id, secret_access_key, session_token, format, structure, compression_method, partition_strategy)
//     else if (count == 8)
//     {
//         if (with_structure)
//         {
//             auto sixth_arg = checkAndGetLiteralArgument<String>(args[6], "compression_method/partition_strategy");
//             if (magic_enum::enum_contains<PartitionStrategyFactory::StrategyType>(sixth_arg))
//             {
//                 engine_args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}, {"session_token", 3}, {"format", 4}, {"structure", 5}, {"partition_strategy", 6}, {"partition_columns_in_data_file", 7}};
//             }
//             else
//             {
//                 engine_args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}, {"session_token", 3}, {"format", 4}, {"structure", 5}, {"compression_method", 6}, {"partition_strategy", 7}};
//             }
//         }
//         else
//         {
//             engine_args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}, {"session_token", 3}, {"format", 4}, {"compression_method", 5}, {"partition_strategy", 6}, {"partition_columns_in_data_file", 7}};
//         }
//     }
//     /// with_structure == 1:
//     ///     s3(source, access_key_id, secret_access_key, session_token, format, structure, compression_method, partition_strategy, partition_columns_in_data_file)
//     /// with_structure == 0:
//     ///     s3(source, access_key_id, secret_access_key, session_token, format, compression_method, partition_strategy, partition_columns_in_data_file, storage_class_name)
//     else if (count == 9)
//     {
//         if (with_structure)
//             engine_args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}, {"session_token", 3}, {"format", 4}, {"structure", 5}, {"compression_method", 6}, {"partition_strategy", 7}, {"partition_columns_in_data_file", 8}};
//         else
//             engine_args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}, {"session_token", 3}, {"format", 4}, {"compression_method", 5}, {"partition_strategy", 6}, {"partition_columns_in_data_file", 7}, {"storage_class_name", 8}};
//     }
//     /// with_structure == 1:
//     ///     s3(source, access_key_id, secret_access_key, session_token, format, structure, compression_method, partition_strategy, partition_columns_in_data_file, storage_class_name)
//     else if (count == 10 && with_structure)
//     {
//         engine_args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}, {"session_token", 3}, {"format", 4}, {"structure", 5}, {"compression_method", 6}, {"partition_strategy", 7}, {"partition_columns_in_data_file", 8}, {"storage_class_name", 9}};
//     }

//     /// This argument is always the first
//     url = S3::URI(checkAndGetLiteralArgument<String>(args[0], "url"), context->getSettingsRef()[Setting::allow_archive_path_syntax]);

//     s3_settings = std::make_unique<S3Settings>();
//     s3_settings->loadFromConfigForObjectStorage(config, "s3", context->getSettingsRef(), url.uri.getScheme(), context->getSettingsRef()[Setting::s3_validate_request_settings]);

//     collectCredentials(extra_credentials, s3_settings->auth_settings, context);

//     if (auto endpoint_settings = context->getStorageS3Settings().getSettings(url.uri.toString(), context->getUserName()))
//     {
//         s3_settings->auth_settings.updateIfChanged(endpoint_settings->auth_settings);
//         s3_settings->request_settings.updateIfChanged(endpoint_settings->request_settings);
//     }

//     if (auto format_value = getFromPositionOrKeyValue<String>("format", args, engine_args_to_idx, key_value_args);
//         format_value.has_value())
//     {
//         format = format_value.value();
//         /// Set format to configuration only of it's not 'auto',
//         /// because we can have default format set in configuration.
//         if (format != "auto")
//             format = format;
//     }

//     if (auto structure_value = getFromPositionOrKeyValue<String>("structure", args, engine_args_to_idx, key_value_args);
//         structure_value.has_value())
//     {
//         structure = structure_value.value();
//     }

//     if (auto compression_method_value = getFromPositionOrKeyValue<String>("compression_method", args, engine_args_to_idx, key_value_args);
//         compression_method_value.has_value())
//     {
//         compression_method = compression_method_value.value();
//     }

//     if (auto partition_strategy_value = getFromPositionOrKeyValue<String>("partition_strategy", args, engine_args_to_idx, key_value_args);
//         partition_strategy_value.has_value())
//     {
//         const auto & partition_strategy_name = partition_strategy_value.value();
//         const auto partition_strategy_type_opt = magic_enum::enum_cast<PartitionStrategyFactory::StrategyType>(partition_strategy_name, magic_enum::case_insensitive);

//         if (!partition_strategy_type_opt.has_value())
//         {
//             throw Exception(ErrorCodes::BAD_ARGUMENTS, "Partition strategy {} is not supported", partition_strategy_name);
//         }

//         partition_strategy_type = partition_strategy_type_opt.value();
//     }

//     if (auto partition_columns_in_data_file_value = getFromPositionOrKeyValue<bool>("partition_columns_in_data_file", args, engine_args_to_idx, key_value_args);
//         partition_columns_in_data_file_value.has_value())
//     {
//         partition_columns_in_data_file = partition_columns_in_data_file_value.value();
//     }
//     else
//         partition_columns_in_data_file = partition_strategy_type != PartitionStrategyFactory::StrategyType::HIVE;

//     if (auto access_key_id_value = getFromPositionOrKeyValue<String>("access_key_id", args, engine_args_to_idx, key_value_args);
//         access_key_id_value.has_value())
//     {
//         s3_settings->auth_settings[S3AuthSetting::access_key_id] = access_key_id_value.value();
//     }

//     if (auto secret_access_key_value = getFromPositionOrKeyValue<String>("secret_access_key", args, engine_args_to_idx, key_value_args);
//         secret_access_key_value.has_value())
//     {
//         s3_settings->auth_settings[S3AuthSetting::secret_access_key] = secret_access_key_value.value();
//     }

//     if (auto session_token_value = getFromPositionOrKeyValue<String>("session_token", args, engine_args_to_idx, key_value_args);
//         session_token_value.has_value())
//     {
//         s3_settings->auth_settings[S3AuthSetting::session_token] = session_token_value.value();
//     }

//     if (no_sign_request)
//     {
//         s3_settings->auth_settings[S3AuthSetting::no_sign_request] = no_sign_request;
//     }
//     else if (auto no_sign_value = getFromPositionOrKeyValue<bool>("no_sign", args, {}, key_value_args);
//         no_sign_value.has_value())
//     {
//         s3_settings->auth_settings[S3AuthSetting::no_sign_request] = no_sign_value.value();
//     }

//     if (auto storage_class_name = getFromPositionOrKeyValue<String>("storage_class_name", args, engine_args_to_idx, key_value_args);
//         storage_class_name.has_value())
//     {
//         s3_settings->request_settings[S3RequestSetting::storage_class_name] = storage_class_name.value();
//     }

//     static_configuration = !s3_settings->auth_settings[S3AuthSetting::access_key_id].value.empty() || s3_settings->auth_settings[S3AuthSetting::no_sign_request].changed;

//     if (extra_credentials)
//         args.push_back(extra_credentials);

//      if (context->getSettingsRef()[Setting::s3_validate_request_settings])
//         s3_settings->request_settings.validateUploadSettings();

//     keys = {url.key};
// }


// }
// #endif

