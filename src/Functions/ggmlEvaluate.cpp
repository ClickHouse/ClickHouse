#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>

#include <BridgeHelper/CatBoostLibraryBridgeHelper.h>
#include <BridgeHelper/IBridgeHelper.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Common/assert_cast.h>
#include "ggml.h"
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>

#include <DataTypes/DataTypeString.h>

#include <fstream>
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
}

/// Evaluate CatBoost model.
/// - Arguments: float features first, then categorical features.
/// - Result: Float64.
class FunctionGGMLEvaluate final : public IFunction, WithContext
{
private:
    mutable std::unique_ptr<CatBoostLibraryBridgeHelper> bridge_helper;

    // default hparams (GPT-J 6B)
    struct gptj_hparams {
        int32_t n_vocab = 50400;
        int32_t n_ctx   = 2048;
        int32_t n_embd  = 4096;
        int32_t n_head  = 16;
        int32_t n_layer = 28;
        int32_t n_rot   = 64;
        int32_t ftype   = 1;
        float   eps     = 1e-5f;
    };

    struct gptj_layer {
        // normalization
        struct ggml_tensor * ln_1_g;
        struct ggml_tensor * ln_1_b;

        // attention
        struct ggml_tensor * c_attn_q_proj_w;
        struct ggml_tensor * c_attn_k_proj_w;
        struct ggml_tensor * c_attn_v_proj_w;

        struct ggml_tensor * c_attn_proj_w;

        // ff
        struct ggml_tensor * c_mlp_fc_w;
        struct ggml_tensor * c_mlp_fc_b;

        struct ggml_tensor * c_mlp_proj_w;
        struct ggml_tensor * c_mlp_proj_b;
    };

    struct gptj_model {
        gptj_hparams hparams;

        // normalization
        struct ggml_tensor * ln_f_g;
        struct ggml_tensor * ln_f_b;

        struct ggml_tensor * wte; // position embedding

        struct ggml_tensor * lmh_g; // language model head
        struct ggml_tensor * lmh_b; // language model bias

        std::vector<gptj_layer> layers;

        // key + value memory
        struct ggml_tensor * memory_k;
        struct ggml_tensor * memory_v;

        //
        struct ggml_context * ctx;
        std::map<std::string, struct ggml_tensor *> tensors;
    };

    struct gpt_vocab {
        using id    = int32_t;
        using token = std::string;

        std::map<token, id> token_to_id;
        std::map<id, token> id_to_token;
        std::vector<std::string> special_tokens;

        void add_special_token(const std::string & token) {
            special_tokens.push_back(token);
        }
    };

    struct gpt_params {
        int32_t seed         = -1;   // RNG seed
        int32_t n_threads    = std::min(4, static_cast<int32_t>(std::thread::hardware_concurrency()));
        int32_t n_predict    = 200;  // new tokens to predict
        int32_t n_parallel   = 1;    // number of parallel streams
        int32_t n_batch      = 8;    // batch size for prompt processing
        int32_t n_ctx        = 2048; // context size (this is the KV cache max size)
        int32_t n_gpu_layers = 0;    // number of layers to offlload to the GPU

        bool ignore_eos = false; // ignore EOS token when generating text

        // sampling parameters
        int32_t top_k          = 40;
        float   top_p          = 0.9f;
        float   temp           = 0.9f;
        int32_t repeat_last_n  = 64;
        float   repeat_penalty = 1.00f;

        std::string model      = "~/ggml-model.bin"; // model path
        std::string prompt;
        std::string token_test;

        bool    interactive      = false;
        int32_t interactive_port = -1;
    };

    // load the model's weights from a file
    #pragma clang diagnostic ignored "-Wkeyword-macro"
    #define return std::cout << __LINE__ << std::endl; return
    bool gptj_model_load(const std::string & fname, gptj_model & model, gpt_vocab & vocab) const {
        auto fin = std::ifstream(fname, std::ios::binary);
        if (!fin) {
            return false;
        }

        // verify magic
        {
            uint32_t magic;
            fin.read(reinterpret_cast<char*>(&magic), sizeof(magic));
            if (magic != GGML_FILE_MAGIC) {
                return false;
            }
        }

        // load hparams
        {
            auto & hparams = model.hparams;

            fin.read(reinterpret_cast<char*>(&hparams.n_vocab), sizeof(hparams.n_vocab));
            fin.read(reinterpret_cast<char*>(&hparams.n_ctx),   sizeof(hparams.n_ctx));
            fin.read(reinterpret_cast<char*>(&hparams.n_embd),  sizeof(hparams.n_embd));
            fin.read(reinterpret_cast<char*>(&hparams.n_head),  sizeof(hparams.n_head));
            fin.read(reinterpret_cast<char*>(&hparams.n_layer), sizeof(hparams.n_layer));
            fin.read(reinterpret_cast<char*>(&hparams.n_rot),   sizeof(hparams.n_rot));
            fin.read(reinterpret_cast<char*>(&hparams.ftype),   sizeof(hparams.ftype));

            hparams.ftype %= GGML_QNT_VERSION_FACTOR;
        }

        // load vocab
        {
            int32_t n_vocab = 0;
            fin.read(reinterpret_cast<char*>(&n_vocab), sizeof(n_vocab));

            if (n_vocab != model.hparams.n_vocab) {
                return false;
            }

            std::string word;
            std::vector<char> buf(128);

            for (int i = 0; i < n_vocab; i++) {
                uint32_t len;
                fin.read(reinterpret_cast<char*>(&len), sizeof(len));

                buf.resize(len);
                fin.read(reinterpret_cast<char*>(buf.data()), len);
                word.assign(buf.data(), len);

                vocab.token_to_id[word] = i;
                vocab.id_to_token[i] = word;
            }
        }

        // for the big tensors, we have the option to store the data in 16-bit floats or quantized
        // in order to save memory and also to speed up the computation
        ggml_type wtype = ggml_ftype_to_ggml_type(static_cast<enum ggml_ftype>(model.hparams.ftype));
        if (wtype == GGML_TYPE_COUNT) {
            return false;
        }

        auto & ctx = model.ctx;

        size_t ctx_size = 0;

        {
            const auto & hparams = model.hparams;

            const int n_embd  = hparams.n_embd;
            const int n_layer = hparams.n_layer;
            const int n_ctx   = hparams.n_ctx;
            const int n_vocab = hparams.n_vocab;

            ctx_size += ggml_row_size(GGML_TYPE_F32, n_embd); // ln_f_g
            ctx_size += ggml_row_size(GGML_TYPE_F32, n_embd); // ln_f_b

            ctx_size += ggml_row_size(wtype, n_embd*n_vocab); // wte

            ctx_size += ggml_row_size(wtype,         n_embd*n_vocab); // lmh_g
            ctx_size += ggml_row_size(GGML_TYPE_F32,        n_vocab); // lmh_b

            ctx_size += n_layer*(ggml_row_size(GGML_TYPE_F32, n_embd)); // ln_1_g
            ctx_size += n_layer*(ggml_row_size(GGML_TYPE_F32, n_embd)); // ln_1_b

            ctx_size += n_layer*(ggml_row_size(wtype, n_embd*n_embd)); // c_attn_q_proj_w
            ctx_size += n_layer*(ggml_row_size(wtype, n_embd*n_embd)); // c_attn_k_proj_w
            ctx_size += n_layer*(ggml_row_size(wtype, n_embd*n_embd)); // c_attn_v_proj_w

            ctx_size += n_layer*(ggml_row_size(wtype, n_embd*n_embd)); // c_attn_proj_w

            ctx_size += n_layer*(ggml_row_size(wtype,         4*n_embd*n_embd)); // c_mlp_fc_w
            ctx_size += n_layer*(ggml_row_size(GGML_TYPE_F32, 4*n_embd));        // c_mlp_fc_b

            ctx_size += n_layer*(ggml_row_size(wtype,         4*n_embd*n_embd)); // c_mlp_proj_w
            ctx_size += n_layer*(ggml_row_size(GGML_TYPE_F32,   n_embd));        // c_mlp_proj_b

            ctx_size += n_ctx*n_layer*ggml_row_size(GGML_TYPE_F16, n_embd); // memory_k
            ctx_size += n_ctx*n_layer*ggml_row_size(GGML_TYPE_F16, n_embd); // memory_v

            ctx_size += (5 + 10*n_layer)*512; // object overhead
        }

        // create the ggml context
        {
            struct ggml_init_params params = {
                /*.mem_size   =*/ ctx_size,
                /*.mem_buffer =*/ nullptr,
                /*.no_alloc   =*/ false,
            };

            model.ctx = ggml_init(params);
            if (!model.ctx) {
                return false;
            }
        }

        // prepare memory for the weights
        {
            const auto & hparams = model.hparams;

            const int n_embd  = hparams.n_embd;
            const int n_layer = hparams.n_layer;
            const int n_vocab = hparams.n_vocab;

            model.layers.resize(n_layer);

            model.wte    = ggml_new_tensor_2d(ctx, wtype,         n_embd, n_vocab);

            model.ln_f_g = ggml_new_tensor_1d(ctx, GGML_TYPE_F32, n_embd);
            model.ln_f_b = ggml_new_tensor_1d(ctx, GGML_TYPE_F32, n_embd);

            model.lmh_g  = ggml_new_tensor_2d(ctx, wtype,         n_embd, n_vocab);
            model.lmh_b  = ggml_new_tensor_1d(ctx, GGML_TYPE_F32, n_vocab);

            // map by name
            model.tensors["transformer.wte.weight"] = model.wte;

            model.tensors["transformer.ln_f.weight"] = model.ln_f_g;
            model.tensors["transformer.ln_f.bias"]   = model.ln_f_b;

            model.tensors["lm_head.weight"] = model.lmh_g;
            model.tensors["lm_head.bias"]   = model.lmh_b;

            for (int i = 0; i < n_layer; ++i) {
                auto & layer = model.layers[i];

                layer.ln_1_g          = ggml_new_tensor_1d(ctx, GGML_TYPE_F32,   n_embd);
                layer.ln_1_b          = ggml_new_tensor_1d(ctx, GGML_TYPE_F32,   n_embd);

                layer.c_attn_q_proj_w = ggml_new_tensor_2d(ctx, wtype,           n_embd,   n_embd);
                layer.c_attn_k_proj_w = ggml_new_tensor_2d(ctx, wtype,           n_embd,   n_embd);
                layer.c_attn_v_proj_w = ggml_new_tensor_2d(ctx, wtype,           n_embd,   n_embd);

                layer.c_attn_proj_w   = ggml_new_tensor_2d(ctx, wtype,           n_embd,   n_embd);

                layer.c_mlp_fc_w      = ggml_new_tensor_2d(ctx, wtype,           n_embd, 4*n_embd);
                layer.c_mlp_fc_b      = ggml_new_tensor_1d(ctx, GGML_TYPE_F32, 4*n_embd);

                layer.c_mlp_proj_w    = ggml_new_tensor_2d(ctx, wtype,         4*n_embd,   n_embd);
                layer.c_mlp_proj_b    = ggml_new_tensor_1d(ctx, GGML_TYPE_F32,   n_embd);

                // map by name
                model.tensors["transformer.h." + std::to_string(i) + ".ln_1.weight"]          = layer.ln_1_g;
                model.tensors["transformer.h." + std::to_string(i) + ".ln_1.bias"]            = layer.ln_1_b;

                model.tensors["transformer.h." + std::to_string(i) + ".attn.q_proj.weight"]   = layer.c_attn_q_proj_w;
                model.tensors["transformer.h." + std::to_string(i) + ".attn.k_proj.weight"]   = layer.c_attn_k_proj_w;
                model.tensors["transformer.h." + std::to_string(i) + ".attn.v_proj.weight"]   = layer.c_attn_v_proj_w;

                model.tensors["transformer.h." + std::to_string(i) + ".attn.out_proj.weight"] = layer.c_attn_proj_w;

                model.tensors["transformer.h." + std::to_string(i) + ".mlp.fc_in.weight"]     = layer.c_mlp_fc_w;
                model.tensors["transformer.h." + std::to_string(i) + ".mlp.fc_in.bias"]       = layer.c_mlp_fc_b;

                model.tensors["transformer.h." + std::to_string(i) + ".mlp.fc_out.weight"]    = layer.c_mlp_proj_w;
                model.tensors["transformer.h." + std::to_string(i) + ".mlp.fc_out.bias"]      = layer.c_mlp_proj_b;
            }
        }

        // key + value memory
        {
            const auto & hparams = model.hparams;

            const int n_embd  = hparams.n_embd;
            const int n_layer = hparams.n_layer;
            const int n_ctx   = hparams.n_ctx;

            const int n_mem      = n_layer*n_ctx;
            const int n_elements = n_embd*n_mem;

            model.memory_k = ggml_new_tensor_1d(ctx, GGML_TYPE_F16, n_elements);
            model.memory_v = ggml_new_tensor_1d(ctx, GGML_TYPE_F16, n_elements);
        }

        // load weights
        {
            int n_tensors = 0;

            while (true) {
                int32_t n_dims;
                int32_t length;
                int32_t ttype;

                fin.read(reinterpret_cast<char *>(&n_dims), sizeof(n_dims));
                fin.read(reinterpret_cast<char *>(&length), sizeof(length));
                fin.read(reinterpret_cast<char *>(&ttype),  sizeof(ttype));

                if (fin.eof()) {
                    break;
                }

                int32_t nelements = 1;
                int32_t ne[2] = { 1, 1 };
                for (int i = 0; i < n_dims; ++i) {
                    fin.read(reinterpret_cast<char *>(&ne[i]), sizeof(ne[i]));
                    nelements *= ne[i];
                }

                std::string nname(length, 0);
                fin.read(nname.data(), length);

                if (model.tensors.find(nname) == model.tensors.end()) {
                    return false;
                }

                auto* tensor = model.tensors[nname];
                if (ggml_nelements(tensor) != nelements) {
                    return false;
                }

                if (tensor->ne[0] != ne[0] || tensor->ne[1] != ne[1]) {
                    return false;
                }

                const size_t bpe = ggml_type_size(ggml_type(ttype));

                if ((nelements*bpe)/ggml_blck_size(tensor->type) != ggml_nbytes(tensor)) {
                    return false;
                }

                fin.read(reinterpret_cast<char *>(tensor->data), ggml_nbytes(tensor));

                if (++n_tensors % 8 == 0) {
                    std::cout << ".";
                    std::cout.flush();
                }
            }

            std::cout << "done" << std::endl;
        }

        fin.close();

        return true;
    }

    void gpt_split_words(std::string str, std::vector<std::string>& words) const {
        const std::string pattern = R"('s|'t|'re|'ve|'m|'ll|'d| ?[[:alpha:]]+| ?[[:digit:]]+| ?[^\s[:alpha:][:digit:]]+|\s+(?!\S)|\s+)";
        const RE2 re(pattern);

        std::string match;

        re2::StringPiece input(str);

        while (RE2::FindAndConsume(&input, pattern, &match)) {
            words.push_back(match);
        }
    }

    std::string regex_replace(const std::string& input, const re2::RE2& pattern, const std::string& replace_with) const {
        std::string output = input;
        while (RE2::Replace(&output, pattern, replace_with)) { }
        return output;
    }

    std::vector<gpt_vocab::id> gpt_tokenize(const gpt_vocab & vocab, const std::string & text) const {
        std::vector<std::string> words;

        // first split the text into words
        {
            std::string str = text;

            // Generate the subpattern from the special_tokens vector if it's not empty
            if (!vocab.special_tokens.empty()) {
                const RE2 escape(R"([\[\\\^\$\.\|\?\*\+\(\)\{\}])");
                std::string special_tokens_subpattern;
                for (const auto & token : vocab.special_tokens) {
                    if (!special_tokens_subpattern.empty()) {
                        special_tokens_subpattern += "|";
                    }
                    special_tokens_subpattern += regex_replace(token, escape, R"(\$&)");
                }

                // std::regex re(special_tokens_subpattern);
                // std::smatch m;
                // // Split the text by special tokens.
                // while (std::regex_search(str, m, re)) {
                //     // Split the substrings in-between special tokens into words.
                //     gpt_split_words(m.prefix(), words);
                //     // Add matched special tokens as words.
                //     for (auto x : m) {
                //         words.push_back(x);
                //     }
                //     str = m.suffix();
                // }
                const RE2 re(special_tokens_subpattern);
                re2::StringPiece input(str);  // Wrap the std::string in a StringPiece
                std::string match;
                while (RE2::FindAndConsume(&input, re, &match)) {
                    // Split the substrings in-between special tokens into words
                    gpt_split_words(std::string(input.substr(0, input.size() - match.size() - str.size())), words);
                    // Add matched special tokens as words
                    words.push_back(match);
                    // Adjust input to remove the processed part including the current match
                    str = std::string(input);  // Update str to remaining input for any necessary reason outside the loop
                }
                // Remaining text without special tokens will be handled below.
            }

            gpt_split_words(str, words);
        }

        // find the longest token that forms each word in words:
        std::vector<gpt_vocab::id> tokens;
        for (const auto & word : words) {
            for (int i = 0; i < static_cast<int>(word.size()); ){
                for (int j = static_cast<int>(word.size()) - 1; j >= i; j--){
                    auto cand = word.substr(i, j-i+1);
                    auto it = vocab.token_to_id.find(cand);
                    if (it != vocab.token_to_id.end()){ // word.substr(i, j-i+1) in vocab
                        tokens.push_back(it->second);
                        i = j + 1;
                        break;
                    }
                    else if (j == i){ // word.substr(i, 1) has no matching
                        i++;
                    }
                }
            }
        }

        return tokens;
    }
    #undef return


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

        gpt_params params;
        gpt_vocab vocab;
        gptj_model model;

        params.model = "/home/ArtNext/ggml-model.bin";

        if (!gptj_model_load(params.model, model, vocab)) {
            throw Exception(ErrorCodes::SYNTAX_ERROR, "No");
        }

        const auto& vals = *arguments[0].column.get();

        for (size_t i = 0; i < input_rows_count; ++i) {
            Field field;
            field = vals[i]; // get(i, field);
            std::string val;
            if (!field.tryGet(val)) {
                std::cout << "Ploho!" << std::endl;
            }
            else {
                std::cout << "Ne Ploho! " << val << std::endl;
            }
        }

        // std::vector<gpt_vocab::id> embd_inp = gpt_tokenize(vocab, );
        // params.n_predict = std::min(params.n_predict, model.hparams.n_ctx - static_cast<int>(embd_inp.size()));
        // std::vector<gpt_vocab::id> embd;

        // size_t mem_per_token = 0;
        // gptj_eval(model, params.n_threads, 0, { 0, 1, 2, 3 }, logits, mem_per_token);


        std::cout << "Success!!!" << std::endl;

        ColumnPtr res = arguments[0].column;
        return res;
    }
};


REGISTER_FUNCTION(GGMLEvaluate)
{
    factory.registerFunction<FunctionGGMLEvaluate>();
}

}
