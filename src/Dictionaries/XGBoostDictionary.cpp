#include <Dictionaries/XGBoostDictionary.h>

#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <DataTypes/IDataType.h>
#include <Dictionaries/DictionaryFactory.h>
#include <Dictionaries/DictionaryPipelineExecutor.h>
#include <Dictionaries/XGBoostModel.h>
#include <Interpreters/Context.h>
#include <QueryPipeline/BlockIO.h>
#include <QueryPipeline/Pipe.h>
#include <Common/logger_useful.h>

#include <Poco/Util/AbstractConfiguration.h>


namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int SUPPORT_IS_DISABLED;
extern const int UNSUPPORTED_METHOD;
}

#if USE_XGBOOST

XGBoostDictionary::XGBoostDictionary(
    const StorageID & dict_id_, const DictionaryStructure & dict_struct_, DictionarySourcePtr source_ptr_, Configuration configuration_)
    : IDictionary(dict_id_)
    , dict_struct(dict_struct_)
    , source_ptr(std::move(source_ptr_))
    , configuration(std::move(configuration_))
    , log(getLogger("XGBoostDictionary"))
{
    trainModel();
}


void XGBoostDictionary::trainModel()
{
    /// Build the training header: features columns in declaration order, followed by the single
    /// target attribute. The XGBoost backend treats every column other than the target as a feature, so the
    /// feature order is the key-column declaration order.
    ColumnsWithTypeAndName header_columns;
    for (const auto & key_attribute : *dict_struct.key)
        header_columns.emplace_back(key_attribute.type->createColumn(), key_attribute.type, key_attribute.name);
    const auto & target_attribute = dict_struct.getAttribute(configuration.target_name);
    header_columns.emplace_back(target_attribute.type->createColumn(), target_attribute.type, target_attribute.name);
    Block header(header_columns);

    model = std::make_unique<XGBoostModel>(configuration.hyper_parameters);

    model->startTraining(header, configuration.target_name);

    BlockIO io = source_ptr->loadAll();

    io.executeWithCallbacks(
        [&]()
        {
            DictionaryPipelineExecutor executor(io.pipeline, false);
            io.pipeline.setConcurrencyControl(false);

            Block block;
            while (executor.pull(block))
                model->addTrainingData(block);
        });

    model->finalizeTraining();

    LOG_INFO(log, "Loaded XGBoost dictionary trained on {} feature(s)", model->getFeatureNames().size());
}


const VectorWithMemoryTracking<String> & XGBoostDictionary::getFeatureNames() const
{
    return model->getFeatureNames();
}


ColumnPtr XGBoostDictionary::predict(const Block & features, const PredictParameters & params) const
{
    query_count.fetch_add(features.rows(), std::memory_order_relaxed);

    return model->predict(features, params);
}


ColumnPtr XGBoostDictionary::getColumn(
    const std::string &,
    const DataTypePtr &,
    const Columns &,
    const DataTypes &,
    DefaultOrFilter) const
{
    /// Disabled because there is no Context here, which means it is not possible to block access
    /// in case `enable_xgboost` is disabled.
    throw Exception(
        ErrorCodes::UNSUPPORTED_METHOD,
        "An XGBoost dictionary does not support `dictGet`. Use function `predictXGBoost('{}', feature_1, ...)` to predict",
        getFullName());
}


ColumnUInt8::Ptr XGBoostDictionary::hasKeys(const Columns &, const DataTypes &) const
{
    throw Exception(
        ErrorCodes::UNSUPPORTED_METHOD,
        "An XGBoost dictionary does not support `dictHas`: it stores no keys, it predicts from a feature vector");
}


Pipe XGBoostDictionary::read(const Names &, size_t, size_t) const
{
    throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "An XGBoost dictionary trains a model and cannot be read back as a table of rows");
}


#endif


void registerDictionaryXGBoost(DictionaryFactory & factory);
void registerDictionaryXGBoost(DictionaryFactory & factory)
{
    auto create_layout = [](const std::string & /* full_name */,
                            [[maybe_unused]] const DictionaryStructure & dict_struct,
                            [[maybe_unused]] const Poco::Util::AbstractConfiguration & config,
                            [[maybe_unused]] const std::string & config_prefix,
                            [[maybe_unused]] DictionarySourcePtr source_ptr,
                            [[maybe_unused]] ContextPtr global_context,
                            [[maybe_unused]] bool created_from_ddl) -> DictionaryPtr
    {
#if !USE_XGBOOST
        throw Exception(
            ErrorCodes::SUPPORT_IS_DISABLED,
            "Dictionary layout `xgboost` is disabled because ClickHouse was built without XGBoost support");
#else

        /// Only CREATE DICTIONARY is supported.
        if (!created_from_ddl)
            throw Exception(
                ErrorCodes::SUPPORT_IS_DISABLED,
                "An XGBoost dictionary defined in a configuration file is not supported. Use `CREATE DICTIONARY`");

        /// The structure must be a complex key of one or more numeric feature columns, followed by exactly one
        /// floating-point attribute: the training target.
        if (!dict_struct.key || dict_struct.key->empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "XGBoost dictionary must have at least one key column (the numeric features)");

        for (const auto & key_attribute : *dict_struct.key)
        {
            const WhichDataType which(key_attribute.type);
            if (!which.isNativeNumber() || which.isEnum())
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "XGBoost dictionary feature key '{}' must be a native numeric type, got {}",
                    key_attribute.name,
                    key_attribute.type->getName());
        }

        if (dict_struct.attributes.size() != 1)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "XGBoost dictionary must have exactly one attribute (the training target), got {}",
                dict_struct.attributes.size());

        /// Check the target data type
        const auto & target_attribute = dict_struct.attributes[0];
        if (!WhichDataType(target_attribute.type).isNativeFloat())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "XGBoost dictionary target attribute '{}' must be Float32 or Float64, got {}. The model predicts a floating-point "
                "value, so an integer target would truncate the prediction",
                target_attribute.name,
                target_attribute.type->getName());

        const String layout_prefix = config_prefix + ".layout.xgboost";

        /// Collect training parameters
        Poco::Util::AbstractConfiguration::Keys layout_keys;
        config.keys(layout_prefix, layout_keys);

        HyperParameters hyper_parameters;
        for (const auto & key : layout_keys)
        {
            /// Poco returns a repeated XML element as `name`, `name[1]`, `name[2]`, ... A parameter written
            /// twice in `LAYOUT(XGBOOST(...))` would otherwise reach the allowlist as `max_depth[1]` and be
            /// reported as an unknown parameter, which hides the real mistake behind Poco's array syntax.
            const auto bracket = key.find('[');
            if (bracket != String::npos)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Training parameter '{}' is specified more than once",
                    key.substr(0, bracket));

            hyper_parameters.emplace(key, config.getString(layout_prefix + "." + key));
        }

        const DictionaryLifetime dict_lifetime{config, config_prefix + ".lifetime"};

        const auto dict_id = StorageID::fromDictionaryConfig(config, config_prefix);

        XGBoostDictionary::Configuration cfg{
            .target_name = target_attribute.name,
            .hyper_parameters = std::move(hyper_parameters),
            .dict_lifetime = dict_lifetime,
        };

        return std::make_unique<XGBoostDictionary>(dict_id, dict_struct, std::move(source_ptr), std::move(cfg));
#endif
    };

    factory.registerLayout(
        "xgboost",
        create_layout,
        /* is_layout_complex= */ true,
        /* has_layout_complex= */ false,
        Documentation{
            .description = "A computational dictionary that trains an immutable XGBoost model at load time from a source table of "
                           "`(features..., target)` rows, then predicts the target for a feature vector through the "
                           "`predictXGBoost` function. The feature columns are the key (any native numeric type) and the single "
                           "attribute is the target (`Float32` or `Float64`). The dictionary holds a model instead of rows, so "
                           "`dictGet`, `dictHas` and reading it as a table are not supported."
#if !USE_XGBOOST
                           " Currently unavailable, because this ClickHouse build does not include XGBoost support."
#endif
            ,
            .syntax = "LAYOUT(XGBOOST([objective '...'] [num_iterations N] [max_depth N] [eta 0.3] [...]))",
            .introduced_in = {26, 9}});
}

}
