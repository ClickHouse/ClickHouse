#include "config.h"

#if USE_XGBOOST

#include <atomic>
#include <limits>

#include <Access/Common/AccessFlags.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <Core/Field.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Dictionaries/XGBoostDictionary.h>
#include <Dictionaries/XGBoostModel.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace Setting
{
extern const SettingsBool enable_xgboost;
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
/// is a constant `Map(String, <Int64>)` of XGBoost prediction parameters (for example
/// `map('type', 0, 'iteration_end', 0)`), forwarded to the XGBoost prediction call.
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
        if (!context_->getSettingsRef()[Setting::enable_xgboost])
            throw Exception(
                ErrorCodes::SUPPORT_IS_DISABLED,
                "Function '{}' is experimental. Set `enable_xgboost = 1` to enable it",
                name);

        return std::make_shared<FunctionPredictXGBoost>(context_);
    }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0}; }

    /// With all features constant, the model is evaluated once per block instead of once per row.
    bool useDefaultImplementationForConstants() const override { return true; }

    /// A NULL feature is passed to XGBoost as a missing value (NaN), which the model handles the way it was
    /// trained to, instead of turning the prediction into NULL. So the result is always `Float64`.
    bool useDefaultImplementationForNulls() const override { return false; }

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

        /// A feature may be `Nullable`, and a bare `NULL` (`Nullable(Nothing)`) is a missing value as well.
        for (size_t i = 1; i < feature_end; ++i)
            if (const auto feature_type = removeNullable(arguments[i].type); !isNumber(feature_type) && !isNothing(feature_type))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Feature argument {} of function '{}' must be numeric or Nullable numeric, got {}",
                    i,
                    getName(),
                    arguments[i].type->getName());

        /// The optional trailing prediction parameters must be a Map from parameter name (String) to an
        /// integer value, throw otherwise.
        if (feature_end < arguments.size())
        {
            const auto * map_type = checkAndGetDataType<DataTypeMap>(arguments.back().type.get());
            if (!isString(map_type->getKeyType()) || !isNativeInteger(map_type->getValueType()))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Prediction parameters of function '{}' must be a Map(String, <integer>), got {}",
                    getName(),
                    arguments.back().type->getName());

            /// Checked here rather than in `executeImpl`: when every argument is constant, the default implementation
            /// for constants unwraps them, so `executeImpl` cannot tell a constant Map from a one-row column.
            if (!arguments.back().column || !isColumnConst(*arguments.back().column))
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Argument 'params' of function '{}' must be a constant Map", getName());
        }

        const String dictionary_name = getConstString(arguments[0], "dictionary name");

        checkAccess(dictionary_name);

        validateDictionaryIsXGBoost(dictionary_name);

        /// Validated here, from the dictionary structure and without loading the dictionary, rather than only in
        /// `executeImpl`: a call over no rows never reaches the model, and must fail the same way as a call with rows.
        const auto structure = context->getExternalDictionariesLoader().getDictionaryStructure(dictionary_name, context);
        const size_t n_features = structure.key ? structure.key->size() : 0;
        const size_t num_feature_args = feature_end - 1;
        if (num_feature_args != n_features)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Function '{}': dictionary '{}' expects {} features but {} were supplied",
                getName(),
                dictionary_name,
                n_features,
                num_feature_args);

        if (feature_end < arguments.size())
            XGBoostModel::validatePredictParams(buildPredictParams(arguments.back()));

        return std::make_shared<DataTypeFloat64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const String dictionary_name = getConstString(arguments[0], "dictionary name");

        const auto & loader = context->getExternalDictionariesLoader();

        checkAccess(dictionary_name);

        if (input_rows_count == 0)
            return result_type->createColumn();

        auto dictionary = loader.getDictionary(dictionary_name, context);

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

        return xgb_dict->predict(block, params);
    }

private:
    ContextPtr context;
    mutable std::atomic<bool> access_checked{false};

    void checkAccess(const String & dictionary_name) const
    {
        if (access_checked.load(std::memory_order_relaxed))
            return;

        auto qualified = context->getExternalDictionariesLoader().qualifyDictionaryNameWithDatabase(dictionary_name, context);
        context->checkAccess(
            AccessType::dictGet,
            qualified.database.empty() ? IDictionary::NO_DATABASE_TAG : qualified.database,
            qualified.table);
        access_checked.store(true, std::memory_order_relaxed);
    }

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
    /// consumes directly. `getReturnTypeImpl` has checked that the Map is constant, so its first row is its value.
    static PredictParameters buildPredictParams(const ColumnWithTypeAndName & arg)
    {
        const Field field = (*arg.column)[0];
        const Map & entries = field.safeGet<Map>();

        PredictParameters params;
        for (const auto & entry : entries)
        {
            const Tuple & key_value = entry.safeGet<Tuple>();
            const String & key = key_value[0].safeGet<String>();
            /// A `Map` may repeat a key, and silently keeping one of the values would hide a contradictory call.
            if (!params.emplace(key, getIntegerParamValue(key, key_value[1])).second)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS, "Prediction parameter '{}' of function '{}' is given more than once", key, name);
        }
        return params;
    }

    /// `getReturnTypeImpl` restricts the Map values to native integers, so the field holds either an `Int64` or
    /// a `UInt64`. Unsigned values that do not fit in an `Int64` are rejected rather than wrapped around.
    static Int64 getIntegerParamValue(const String & key, const Field & value)
    {
        if (value.getType() == Field::Types::Int64)
            return value.safeGet<Int64>();

        const UInt64 unsigned_value = value.safeGet<UInt64>();
        if (unsigned_value > static_cast<UInt64>(std::numeric_limits<Int64>::max()))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Value {} of prediction parameter '{}' of function '{}' does not fit in Int64",
                unsigned_value,
                key,
                name);
        return static_cast<Int64>(unsigned_value);
    }

    void validateDictionaryIsXGBoost(const String & dictionary_name) const
    {
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
                       "[`XGBOOST`](/sql-reference/statements/create/dictionary/layouts/xgboost) dictionary. Features are "
                       "passed positionally, in the order the dictionary's key columns were declared. This is the only way to "
                       "query an `XGBOOST` dictionary: it holds a trained model rather than rows, so the generic dictionary "
                       "interface (`dictGet`, `dictHas`, `SELECT * FROM dict`) is not supported. The XGBoost integration is "
                       "experimental, so the `enable_xgboost` setting must be enabled.",
        .syntax = "predictXGBoost(dictionary_name, feature1[, feature2, ...][, params])",
        .arguments
        = {{"dictionary_name",
            "Name of a dictionary with the XGBOOST layout. See the "
            "[XGBOOST dictionary layout](/sql-reference/statements/create/dictionary/layouts/xgboost) for how to create it "
            "and the training parameters it accepts.",
            {"String"}},
           {"featureN",
            "Numeric feature values, positionally in the dictionary's key order. A NULL feature is passed to the model as a "
            "missing value, which XGBoost handles the way it was trained to, so the prediction is never NULL.",
            {"(U)Int*", "Float*", "Nullable((U)Int*)", "Nullable(Float*)"}},
           {"params",
            "Optional constant Map of XGBoost prediction parameters, from parameter name to an integer value, e.g. "
            "`map('type', 0, 'iteration_end', 0)`. Every accepted parameter is an integer or a boolean, so fractional "
            "values are rejected. See "
            "[prediction parameters](/sql-reference/statements/create/dictionary/layouts/xgboost#prediction-parameters) for "
            "the accepted keys.",
            {"Map(String, (U)Int8/16/32/64)"}}},
        .returned_value = {"The model prediction as Float64, one per row.", {"Float64"}},
        .examples = {{"Predict", R"(
-- A model trained on the linear target y = 2 * x1 + 3 * x2. The XGBoost integration is experimental.
SET enable_xgboost = 1;

CREATE TABLE training_data (x1 Float64, x2 Float64, y Float64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO training_data SELECT number AS x1, number * 2 AS x2, 2 * x1 + 3 * x2 AS y FROM numbers(100);

CREATE DICTIONARY model (x1 Float64, x2 Float64, y Float64)
PRIMARY KEY (x1, x2)
SOURCE(CLICKHOUSE(TABLE 'training_data'))
LAYOUT(XGBOOST(objective 'reg:squarederror' num_iterations 100 max_depth 6))
LIFETIME(0);

-- The ground truth is 2 * 1 + 3 * 2 = 8.
SELECT round(predictXGBoost('model', 1.0, 2.0), 1) AS prediction;
        )",
        R"(
┌─prediction─┐
│          8 │
└────────────┘
        )"},
           {"Predict with parameters", R"(
-- The raw margin of the model above, before the objective's transformation. For `reg:squarederror` it is the
-- prediction itself, while for `binary:logistic` it is the log-odds rather than the probability.
SELECT round(predictXGBoost('model', 1.0, 2.0, map('type', 1)), 1) AS margin;
        )",
        R"(
┌─margin─┐
│      8 │
└────────┘
        )"}},
        .introduced_in = {26, 10},
        .category = FunctionDocumentation::Category::MachineLearning});
}

}

#endif
