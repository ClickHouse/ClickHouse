#pragma once

#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <IO/ReadBufferFromFile.h>

namespace DB
{
/** Calculate similarity metrics:
  *
  * ngramDistance(haystack, needle) - calculate n-gram distance between haystack and needle.
  * Returns float number from 0 to 1 - the closer to zero, the more strings are similar to each other.
  * Also support CaseInsensitive and UTF8 formats.
  * ngramDistanceCaseInsensitive(haystack, needle)
  * ngramDistanceUTF8(haystack, needle)
  * ngramDistanceCaseInsensitiveUTF8(haystack, needle)
  */

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
    extern const int TOO_LARGE_STRING_SIZE;
}

template <typename Impl, typename Name>
class FunctionsStringSimilarity : public IFunction
{
public:
    static constexpr auto name = Name::name;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionsStringSimilarity>(); }

    String getName() const override { return name; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isString(arguments[0]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}",
                arguments[0]->getName(), getName());

        if (!isString(arguments[1]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}",
                arguments[1]->getName(), getName());

        return std::make_shared<DataTypeNumber<typename Impl::ResultType>>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t /*input_rows_count*/) const override
    {
        using ResultType = typename Impl::ResultType;

        const ColumnPtr & column_haystack = arguments[0].column;
        const ColumnPtr & column_needle = arguments[1].column;

        const ColumnConst * col_haystack_const = typeid_cast<const ColumnConst *>(&*column_haystack);
        const ColumnConst * col_needle_const = typeid_cast<const ColumnConst *>(&*column_needle);

        if (col_haystack_const && col_needle_const)
        {
            ResultType res{};
            const String & needle = col_needle_const->getValue<String>();
            if (needle.size() > Impl::max_string_size)
            {
                throw Exception(ErrorCodes::TOO_LARGE_STRING_SIZE, "String size of needle is too big for function {}. "
                    "Should be at most {}", getName(), Impl::max_string_size);
            }
            Impl::constantConstant(col_haystack_const->getValue<String>(), needle, res);
            return result_type->createColumnConst(col_haystack_const->size(), toField(res));
        }

        auto col_res = ColumnVector<ResultType>::create();

        typename ColumnVector<ResultType>::Container & vec_res = col_res->getData();
        vec_res.resize(column_haystack->size());

        const ColumnString * col_haystack_vector = checkAndGetColumn<ColumnString>(&*column_haystack);
        const ColumnString * col_needle_vector = checkAndGetColumn<ColumnString>(&*column_needle);

        if (col_haystack_vector && col_needle_const)
        {
            const String & needle = col_needle_const->getValue<String>();
            if (needle.size() > Impl::max_string_size)
            {
                throw Exception(ErrorCodes::TOO_LARGE_STRING_SIZE, "String size of needle is too big for function {}. "
                    "Should be at most {}", getName(), Impl::max_string_size);
            }
            Impl::vectorConstant(col_haystack_vector->getChars(), col_haystack_vector->getOffsets(), needle, vec_res);
        }
        else if (col_haystack_vector && col_needle_vector)
        {
            Impl::vectorVector(
                col_haystack_vector->getChars(),
                col_haystack_vector->getOffsets(),
                col_needle_vector->getChars(),
                col_needle_vector->getOffsets(),
                vec_res);
        }
        else if (col_haystack_const && col_needle_vector)
        {
            const String & haystack = col_haystack_const->getValue<String>();
            if (haystack.size() > Impl::max_string_size)
            {
                throw Exception(ErrorCodes::TOO_LARGE_STRING_SIZE, "String size of haystack is too big for function {}. "
                    "Should be at most {}", getName(), Impl::max_string_size);
            }
            Impl::constantVector(haystack, col_needle_vector->getChars(), col_needle_vector->getOffsets(), vec_res);
        }
        else
        {
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal columns {} and {} of arguments of function {}",
                arguments[0].column->getName(), arguments[1].column->getName(), getName());
        }

        return col_res;
    }
};

struct StringSimilarityModel {
    const String model_name;
    const String path_to_positive;
    const String path_to_negative;
};

using StringSimilarityModels = std::map<String, StringSimilarityModel>;

template <typename Impl, typename Name>
class FunctionsStringCategorySimilarity : public IFunction
{
public:
    static constexpr auto name = Name::name;

    explicit FunctionsStringCategorySimilarity(StringSimilarityModels models) : string_similarity_models(models) {}

    static FunctionPtr create(ContextPtr context)
    {
        const auto& config = context->getConfigRef();
        Poco::Util::AbstractConfiguration::Keys keys;
        config.keys(string_similarity_models_config_key, keys);
        StringSimilarityModels models;

        for (const auto & key : keys)
        {
            if (key.starts_with(string_similarity_model_config_key)) {
                auto model_name = config.getString(
                    fmt::format("{}.{}.{}", string_similarity_models_config_key, key, model_name_config_key));
                auto path_to_positive = config.getString(
                    fmt::format("{}.{}.{}", string_similarity_models_config_key, key, path_to_positive_config_key));
                auto path_to_negative = config.getString(
                    fmt::format("{}.{}.{}", string_similarity_models_config_key, key, path_to_negative_config_key));
                StringSimilarityModel model {model_name, std::move(path_to_positive), std::move(path_to_negative)};
                models.emplace(std::move(model_name), std::move(model));
            }
        }

        return std::make_shared<FunctionsStringCategorySimilarity>(std::move(models));
    }

    String getName() const override { return name; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isString(arguments[0]))
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!isString(arguments[1]))
            throw Exception(
                "Illegal type " + arguments[1]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeNumber<typename Impl::ResultType>>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t /*input_rows_count*/) const override
    {
        using ResultType = typename Impl::ResultType;

        const ColumnPtr & column_haystack = arguments[0].column;
        const ColumnPtr & column_needle = arguments[1].column;

        const ColumnConst * col_haystack_const = typeid_cast<const ColumnConst *>(&*column_haystack);
        const ColumnConst * col_needle_const = typeid_cast<const ColumnConst *>(&*column_needle);

        if (!col_needle_const) {
            throw Exception("Illegal search model " + arguments[1].column->getName(), ErrorCodes::BAD_ARGUMENTS);
        }

        const String & model_name = col_needle_const->getValue<String>();
        const auto& model_it = string_similarity_models.find(model_name);
        if (model_it == string_similarity_models.end()) {
            throw Exception("There is no " + model_name + " string similarity model", ErrorCodes::BAD_ARGUMENTS);
        }
        const auto& model = model_it->second;

        const String positive_needle = readModel(model.path_to_positive);
        const String negative_needle = readModel(model.path_to_negative);

        if (positive_needle.size() > Impl::max_string_size || negative_needle.size() > Impl::max_string_size)
        {
            throw Exception(
                "Size of model " + model.model_name + " is too big. Should be at most "
                    + std::to_string(Impl::max_string_size),
                ErrorCodes::TOO_LARGE_STRING_SIZE);
        }

        if (col_haystack_const) {
            ResultType res_positive{};
            Impl::constantConstant(col_haystack_const->getValue<String>(), positive_needle, res_positive);

            ResultType res_negative{};
            Impl::constantConstant(col_haystack_const->getValue<String>(), negative_needle, res_negative);

            ResultType res{0};
            if (res_positive + res_negative > 0) {
                res = res_positive / (res_positive + res_negative);
            }

            return result_type->createColumnConst(col_haystack_const->size(), toField(res));
        }

        const ColumnString * col_haystack_vector = checkAndGetColumn<ColumnString>(&*column_haystack);
        if (!col_haystack_vector)
        {
            throw Exception(
                "Illegal columns " + arguments[0].column->getName() + " and "
                    + arguments[1].column->getName() + " of arguments of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
        }

        PaddedPODArray<ResultType> vec_positive_res, vec_negative_res;

        vec_positive_res.resize(col_haystack_vector->size());
        Impl::vectorConstant(col_haystack_vector->getChars(), col_haystack_vector->getOffsets(), positive_needle, vec_positive_res);

        vec_negative_res.resize(col_haystack_vector->size());
        Impl::vectorConstant(col_haystack_vector->getChars(), col_haystack_vector->getOffsets(), negative_needle, vec_negative_res);
        
        auto col_res = ColumnVector<ResultType>::create();
        typename ColumnVector<ResultType>::Container & vec_res = col_res->getData();
        vec_res.resize(column_haystack->size());

        for (size_t i = 0; i < column_haystack->size(); ++i) {
            if (vec_positive_res[i] + vec_negative_res[i] > 0) {
                vec_res[i] = vec_positive_res[i] / (vec_positive_res[i] + vec_negative_res[i]);
            } else {
                vec_res[i] = 0;
            }
        }

        return col_res;
    }

private:
    static constexpr auto string_similarity_models_config_key = "string_similarity_models";
    static constexpr auto string_similarity_model_config_key  = "string_similarity_model";
    static constexpr auto model_name_config_key               = "model_name";
    static constexpr auto path_to_positive_config_key         = "path_to_positive";
    static constexpr auto path_to_negative_config_key         = "path_to_negative";

    const StringSimilarityModels string_similarity_models;

    String readModel(const String& path) const {
        ReadBufferFromFile in(path);

        String model;
        model.reserve((in.available()));
        readStringUntilEOF(model, in);

        in.close();
        return model;
    }
};

}
