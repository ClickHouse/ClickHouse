#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>

#include <BridgeHelper/CatBoostLibraryBridgeHelper.h>
#include <BridgeHelper/IBridgeHelper.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include "Common/Exception.h"
#include <Common/assert_cast.h>
#include "gpt_common.h"
#include "gptj.h"
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>

#include <DataTypes/DataTypeString.h>

#include <fstream>
#include <memory>
#include <string>
#include <Common/re2.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int FILE_DOESNT_EXIST;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
    extern const int TOO_MANY_ARGUMENTS_FOR_FUNCTION;
    extern const int ILLEGAL_COLUMN;
    extern const int SYNTAX_ERROR;
    extern const int LOGICAL_ERROR;
}

/// Evaluate GGML model.
/// - Arguments: TBD
/// - Result: TBD
class FunctionGGMLEvaluate final : public IFunction, WithContext
{
public:
    static constexpr auto name = "ggmlEvaluate";

    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionGGMLEvaluate>(context_); }

    explicit FunctionGGMLEvaluate(ContextPtr context_) : WithContext(context_) {}
    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    bool isDeterministic() const override { return false; }
    bool useDefaultImplementationForNulls() const override { return false; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.empty())
            throw Exception(ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION, "Function {} expects exactly 1 argument", getName());
        if (arguments.size() > 1)
            throw Exception(ErrorCodes::TOO_MANY_ARGUMENTS_FOR_FUNCTION, "Function {} expects exactly 1 argument", getName());
        // const auto * name_col = checkAndGetColumn<ColumnString>(arguments[0].column.get());
        // if (!name_col)
        //     throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Argument of function {} must be a string", getName());
        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        std::cout << "GGML!!!" << std::endl;
        std::cout << "input_rows_count is : " << input_rows_count << std::endl;
        std::cout << "result_type is : " << result_type->getName() << std::endl;
        if (input_rows_count == 0) {
            ColumnPtr res = arguments[0].column;
            return res;
        }

        std::string model_path = "/home/m0r0zk01/ggml-model.bin";
        IGptModel* model = getModel(model_path);

        if (!model->load(model_path)) {
            throw Exception(ErrorCodes::SYNTAX_ERROR, "No");
        }

        std::cout << "loaded\n";

        const auto& vals = *arguments[0].column.get();
        auto col_res = ColumnString::create();
        col_res->reserve(input_rows_count);
        UInt64 totalsize = 0;
        std::vector<String> result_raw(input_rows_count);

        for (size_t j = 0; j < input_rows_count; ++j) {
            Field field = vals[j]; // get(i, field);
            std::string val;
            if (!field.tryGet(val)) {
                throw Exception(ErrorCodes::SYNTAX_ERROR, "Nasrali");
            }
            else {
                std::cout << "Processing " << val << '\n';
                std::vector<GptVocab::id> embd_inp = gpt_tokenize(model->vocab, val);
                std::cout << "Tokenized " << val << '\n';
                std::vector<GptVocab::id> embd = model->predict(embd_inp);
                std::string result;
                for (auto id : embd) {
                    result += model->vocab.id_to_token[id];
                }
                std::cout << "Predicted " << result << '\n';

                result_raw[j] = std::move(result);
                totalsize += result_raw[j].size() + 1;
            }
        }

        col_res->getChars().resize(totalsize);
        col_res->getOffsets().resize(input_rows_count);
        auto* data_ptr = col_res->getChars().data();
        UInt64 offset = 0;
        for (size_t i = 0; i < input_rows_count; ++i) {
            memcpy(data_ptr + offset, result_raw[i].data(), result_raw[i].size());
            data_ptr[offset + result_raw[i].size()] = '\0';
            offset += result_raw[i].size() + 1;
            col_res->getOffsets()[i] = offset;
        }

        // params.n_predict = std::min(params.n_predict, model.hparams.n_ctx - static_cast<int>(embd_inp.size()));
        // std::vector<GptVocab::id> embd;

        // size_t mem_per_token = 0;
        // gptj_eval(model, params.n_threads, 0, { 0, 1, 2, 3 }, logits, mem_per_token);


        std::cout << "Success!!!" << std::endl;
        return col_res;
    }

private:
    IGptModel* getModel(const std::string & path) const
    {
        auto & storage = getContext()->getGptStorage();
        auto * model = storage.get("gptj", []() { return std::make_unique<GptJModel>(); });
        if (!model->load(path)) {
            // TODO: more suitable error codes
            throw Exception(ErrorCodes::SYNTAX_ERROR, "Could not load model from {}", path);
        }
        return model;
    }
};


REGISTER_FUNCTION(GGMLEvaluate)
{
    factory.registerFunction<FunctionGGMLEvaluate>();
}

}
