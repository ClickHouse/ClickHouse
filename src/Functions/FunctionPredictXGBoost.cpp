#include <atomic>

#include <Access/Common/AccessFlags.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <Core/Field.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Dictionaries/XGBoostDictionary.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Common/Exception.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <Common/FieldVisitors.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace Setting
{
extern const SettingsBool allow_experimental_xgboost;
}

namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int ILLEGAL_COLUMN;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int BAD_ARGUMENTS;
extern const int SUPPORT_IS_DISABLED;
}

namespace
{

/// Row-wise inference against an XGBoost dictionary:
///
///     predictXGBoost(dictionary_name, feature1, feature2, ...[, params])
///
/// Features are passed as individual columns, positionally: argument i (after the dictionary name) is bound
/// to the model's i-th feature (the i-th key column, in declaration order). The optional trailing `params`
/// is a constant `Map(String, <numeric>)` of XGBoost prediction parameters (for example
/// `map('type', 0, 'iteration_end', 0)`), forwarded to the XGBoost prediction call. This is the same model
/// that `dictGet(dictionary_name, target, (feature1, ...))` reaches.
class FunctionPredictXGBoost final : public IFunction
{
public:
    static constexpr auto name = "predictXGBoost";

    explicit FunctionPredictXGBoost(ContextPtr context_)
        : context(std::move(context_))
    {
    }
    static FunctionPtr create(ContextPtr context_)
    {
        if (!context_->getSettingsRef()[Setting::allow_experimental_xgboost])
            throw Exception(
                ErrorCodes::SUPPORT_IS_DISABLED,
                "Function '{}' is experimental. Set `allow_experimental_xgboost` setting to enable it",
                name);

        return std::make_shared<FunctionPredictXGBoost>(context_);
    }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0}; }

    /// A dictionary can be reloaded (retrained) under the same name.
    bool isDeterministic() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() < 2)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function '{}' expects 'dictionary_name, feature1[, feature2, ...][, params]'",
                getName());

        if (!isString(arguments[0].type))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument of function '{}' (dictionary name) must be a constant String",
                getName());

        const size_t feature_end = featureEnd(arguments);

        if (feature_end < 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function '{}' expects at least one feature argument", getName());

        for (size_t i = 1; i < feature_end; ++i)
            if (!isNumber(arguments[i].type))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Feature argument {} of function '{}' must be numeric, got {}",
                    i,
                    getName(),
                    arguments[i].type->getName());

        /// The optional trailing prediction parameters must be a Map from parameter name (String) to a
        /// numeric value, e.g. map('type', 0) or {'type': 0}.
        if (feature_end < arguments.size())
        {
            const auto * map_type = checkAndGetDataType<DataTypeMap>(arguments.back().type.get());
            if (!isString(map_type->getKeyType()) || !isNumber(map_type->getValueType()))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Prediction parameters of function '{}' must be a Map(String, <numeric>), got {}",
                    getName(),
                    arguments.back().type->getName());
        }

        /// Fail at analysis time if the named dictionary is missing or does not have the XGBOOST layout.
        validateDictionaryIsXGBoost(arguments);

        return std::make_shared<DataTypeFloat64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const String dictionary_name = getConstString(arguments[0], "dictionary name");

        auto dictionary = context->getExternalDictionariesLoader().getDictionary(dictionary_name, context);

        if (!access_checked.load(std::memory_order_relaxed))
        {
            context->checkAccess(
                AccessType::dictGet, dictionary->getDatabaseOrNoDatabaseTag(), dictionary->getDictionaryID().getTableName());
            access_checked.store(true, std::memory_order_relaxed);
        }

        const auto * xgb_dict = typeid_cast<const XGBoostDictionary *>(dictionary.get());
        if (!xgb_dict)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Dictionary '{}' is not an XGBoost dictionary", dictionary_name);

        const size_t feature_end = featureEnd(arguments);
        const PredictParameters params = feature_end < arguments.size() ? buildPredictParams(arguments.back()) : PredictParameters{};

        /// Feature names/order the model expects. Each argument column is inserted under the corresponding
        /// name so the dictionary can resolve it by name.
        const auto & feature_names = xgb_dict->getFeatureNames();
        const size_t n_features = feature_names.size();
        const size_t num_feature_args = feature_end - 1;

        if (num_feature_args != n_features)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Function '{}': dictionary '{}' expects {} features but {} were supplied",
                getName(),
                dictionary_name,
                n_features,
                num_feature_args);

        Block block;
        for (size_t c = 0; c < n_features; ++c)
        {
            const auto & arg = arguments[c + 1];
            block.insert({arg.column->convertToFullColumnIfConst(), arg.type, feature_names[c]});
        }

        auto result = xgb_dict->predict(block, params);

        /// This bypasses `getColumn`, so record the predicted rows to keep the dictionary query statistics
        /// consistent with the equivalent `dictGet` path.
        xgb_dict->incrementQueryCount(input_rows_count);

        return result;
    }

private:
    ContextPtr context;
    mutable std::atomic<bool> access_checked{false};

    /// Index one past the last feature argument. The trailing `params` (a Map) is excluded; everything
    /// from index 1 up to this bound is a feature.
    ///     predictXGBoost(dict, f1, f2, f3)                       -> returns 4
    ///     predictXGBoost(dict, f1, f2, f3, map('type', 1))       -> returns 4
    static size_t featureEnd(const ColumnsWithTypeAndName & arguments)
    {
        const bool has_params = arguments.size() >= 3 && WhichDataType(arguments.back().type).isMap();
        return has_params ? arguments.size() - 1 : arguments.size();
    }

    /// Reads the trailing `params` Map into the structured prediction parameters (name -> integer) the model
    /// consumes directly.
    static PredictParameters buildPredictParams(const ColumnWithTypeAndName & arg)
    {
        const auto * col = checkAndGetColumnConst<ColumnMap>(arg.column.get());
        if (!col)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Argument 'params' of function '{}' must be a constant Map", name);

        const Field field = (*col)[0];
        const Map & entries = field.safeGet<Map>();

        PredictParameters params;
        for (const auto & entry : entries)
        {
            const Tuple & key_value = entry.safeGet<Tuple>();
            params.emplace(key_value[0].safeGet<String>(), applyVisitor(FieldVisitorConvertToNumber<Int64>(), key_value[1]));
        }
        return params;
    }

    void validateDictionaryIsXGBoost(const ColumnsWithTypeAndName & arguments) const
    {
        const auto * name_col = checkAndGetColumnConst<ColumnString>(arguments[0].column.get());
        if (!name_col)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Argument 'dictionary name' of function '{}' must be a constant String", getName());

        const String dictionary_name = name_col->getValue<String>();
        const auto layout_type = context->getExternalDictionariesLoader().getDictionaryLayoutType(dictionary_name, context);
        if (layout_type != "xgboost")
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Dictionary '{}' has layout '{}', but function {} requires a dictionary with the XGBOOST layout",
                dictionary_name,
                layout_type,
                getName());
    }

    String getConstString(const ColumnWithTypeAndName & arg, const char * what) const
    {
        const auto * col = checkAndGetColumnConst<ColumnString>(arg.column.get());
        if (!col)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Argument '{}' of function '{}' must be a constant String", what, getName());
        return col->getValue<String>();
    }
};

}

REGISTER_FUNCTION(PredictXGBoost)
{
    factory.registerFunction<FunctionPredictXGBoost>(FunctionDocumentation{
        .description = "Predicts a numeric target for a feature vector using an "
                       "[`XGBOOST`](/sql-reference/statements/create/dictionary/layouts/xgboost) dictionary: "
                       "predictXGBoost(dictionary_name, feature1, feature2, ...[, params]). Features are passed positionally "
                       "in the dictionary's key order. Returns the same value as "
                       "`dictGet(dictionary_name, target, (feature1, feature2, ...))`.",
        .syntax = "predictXGBoost(dictionary_name, feature1[, feature2, ...][, params])",
        .arguments
        = {{"dictionary_name",
            "Name of a dictionary with the XGBOOST layout. See the "
            "[XGBOOST dictionary layout](/sql-reference/statements/create/dictionary/layouts/xgboost) for how to create it "
            "and the training parameters it accepts.",
            {"String"}},
           {"featureN", "Numeric feature values, positionally in the dictionary's key order.", {"(U)Int*", "Float*"}},
           {"params",
            "Optional constant Map of XGBoost prediction parameters, from parameter name to a numeric value, e.g. "
            "`map('type', 0, 'iteration_end', 0)`. See the "
            "[prediction parameters](/sql-reference/statements/create/dictionary/layouts/xgboost#prediction-parameters) for "
            "the accepted keys.",
            {"Map(String, (U)Int*)", "Map(String, Float*)"}}},
        .returned_value = {"The model prediction as Float64, one per row.", {"Float64"}},
        .examples
        = {{"Predict", "SELECT predictXGBoost('model', 1.0, 2.0);", "7.0"},
           {"Predict with parameters", "SELECT predictXGBoost('model', 1.0, 2.0, map('type', 0, 'iteration_end', 0));", "7.0"}},
        .introduced_in = {26, 7},
        .category = FunctionDocumentation::Category::MachineLearning});
}

}
