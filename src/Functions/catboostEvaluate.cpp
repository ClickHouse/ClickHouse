#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>

#include <BridgeHelper/CatBoostLibraryBridgeHelper.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int FILE_DOESNT_EXIST;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
    extern const int ILLEGAL_COLUMN;
}

/// Evaluate CatBoost model.
/// - Arguments: float features first, then categorical features.
/// - Result: Float64.
class FunctionCatBoostEvaluate final : public IFunction, WithContext
{
private:
    mutable std::unique_ptr<CatBoostLibraryBridgeHelper> bridge_helper;

public:
    static constexpr auto name = "catboostEvaluate";

    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionCatBoostEvaluate>(context_); }

    explicit FunctionCatBoostEvaluate(ContextPtr context_) : WithContext(context_) {}
    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    bool isDeterministic() const override { return false; }
    bool useDefaultImplementationForNulls() const override { return false; }
    size_t getNumberOfArguments() const override { return 0; }

    void initBridge(const ColumnConst * name_col) const
    {
        String library_path = getContext()->getConfigRef().getString("catboost_lib_path");
        if (!std::filesystem::exists(library_path))
            throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "Can't load library {}: file doesn't exist", library_path);

        String model_path = name_col->getValue<String>();
        if (!std::filesystem::exists(model_path))
            throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "Can't load model {}: file doesn't exist", model_path);

        bridge_helper = std::make_unique<CatBoostLibraryBridgeHelper>(getContext(), model_path, library_path);
    }

    DataTypePtr getReturnTypeFromLibraryBridge() const
    {
        size_t tree_count = bridge_helper->getTreeCount();
        auto type = std::make_shared<DataTypeFloat64>();
        if (tree_count == 1)
            return type;

        DataTypes types(tree_count, type);

        return std::make_shared<DataTypeTuple>(types);
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() < 2)
            throw Exception(ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION, "Function {} expects at least 2 arguments", getName());

        if (!isString(arguments[0].type))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of first argument of function {}, expected a string.", arguments[0].type->getName(), getName());

        const auto * name_col = checkAndGetColumnConst<ColumnString>(arguments[0].column.get());
        if (!name_col)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "First argument of function {} must be a constant string", getName());

        initBridge(name_col);

        auto type = getReturnTypeFromLibraryBridge();

        bool has_nullable = false;
        for (size_t i = 1; i < arguments.size(); ++i)
            has_nullable = has_nullable || arguments[i].type->isNullable();

        if (has_nullable)
        {
            if (const auto * tuple = typeid_cast<const DataTypeTuple *>(type.get()))
            {
                auto elements = tuple->getElements();
                for (auto & element : elements)
                    element = makeNullable(element);

                type = std::make_shared<DataTypeTuple>(elements);
            }
            else
                type = makeNullable(type);
        }

        return type;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const override
    {
        const auto * name_col = checkAndGetColumnConst<ColumnString>(arguments[0].column.get());
        if (!name_col)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "First argument of function {} must be a constant string", getName());

        ColumnRawPtrs column_ptrs;
        Columns materialized_columns;
        ColumnPtr null_map;

        ColumnsWithTypeAndName feature_arguments(arguments.begin() + 1, arguments.end());
        for (auto & arg : feature_arguments)
        {
            if (auto full_column = arg.column->convertToFullColumnIfConst())
            {
                materialized_columns.push_back(full_column);
                arg.column = full_column;
            }
            if (const auto * col_nullable = checkAndGetColumn<ColumnNullable>(&*arg.column))
            {
                if (!null_map)
                    null_map = col_nullable->getNullMapColumnPtr();
                else
                {
                    auto mut_null_map = IColumn::mutate(std::move(null_map));

                    NullMap & result_null_map = assert_cast<ColumnUInt8 &>(*mut_null_map).getData();
                    const NullMap & src_null_map = col_nullable->getNullMapColumn().getData();

                    for (size_t i = 0, size = result_null_map.size(); i < size; ++i)
                        if (src_null_map[i])
                            result_null_map[i] = 1;

                    null_map = std::move(mut_null_map);
                }

                arg.column = col_nullable->getNestedColumn().getPtr();
                arg.type = static_cast<const DataTypeNullable &>(*arg.type).getNestedType();
            }
        }

        auto res = bridge_helper->evaluate(feature_arguments);

        if (null_map)
        {
            if (const auto * tuple = typeid_cast<const ColumnTuple *>(res.get()))
            {
                auto nested = tuple->getColumns();
                for (auto & col : nested)
                    col = ColumnNullable::create(col, null_map);

                res = ColumnTuple::create(nested);
            }
            else
                res = ColumnNullable::create(res, null_map);
        }

        return res;
    }
};


REGISTER_FUNCTION(CatBoostEvaluate)
{
    FunctionDocumentation::Description description = R"(
Evaluate an external catboost model. [CatBoost](https://catboost.ai) is an open-source gradient boosting library developed by Yandex for machine learning.
Accepts a path to a catboost model and model arguments (features).

**Prerequisites**

1. Build the catboost evaluation library

Before evaluating catboost models, the `libcatboostmodel.<so|dylib>` library must be made available. See [CatBoost documentation](https://catboost.ai/docs/concepts/c-plus-plus-api_dynamic-c-pluplus-wrapper.html) how to compile it.

Next, specify the path to `libcatboostmodel.<so|dylib>` in the clickhouse configuration:

```xml
<clickhouse>
...
    <catboost_lib_path>/path/to/libcatboostmodel.so</catboost_lib_path>
...
</clickhouse>
```

For security and isolation reasons, the model evaluation does not run in the server process but in the clickhouse-library-bridge process.
At the first execution of `catboostEvaluate()`, the server starts the library bridge process if it is not running already. Both processes
communicate using a HTTP interface. By default, port `9012` is used. A different port can be specified as follows - this is useful if port
`9012` is already assigned to a different service.

```xml
<library_bridge>
    <port>9019</port>
</library_bridge>
```

2. Train a catboost model using libcatboost

See [Training and applying models](https://catboost.ai/docs/features/training.html#training) for how to train catboost models from a training data set.
)";
    FunctionDocumentation::Syntax syntax = "catboostEvaluate(path_to_model, feature_1[, feature_2, ..., feature_n])";
    FunctionDocumentation::Arguments arguments = {
        {"path_to_model", "Path to catboost model.", {"const String"}},
        {"feature", "One or more model features/arguments.", {"Float*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the model evaluation result.", {"Float64"}};
    FunctionDocumentation::Examples examples = {
    {
        "catboostEvaluate",
        "SELECT catboostEvaluate('/root/occupy.bin', Temperature, Humidity, Light, CO2, HumidityRatio) AS prediction FROM occupancy LIMIT 1",
        "4.695691092573497"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {22, 9};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionCatBoostEvaluate>(documentation);
}

}
