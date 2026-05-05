#include <Functions/Embedding/OnnxEmbeddingModel.h>

#if defined(USE_ONNXRUNTIME)

#include <Common/logger_useful.h>

#include <onnxruntime_cxx_api.h>

#include <rapidjson/document.h>
#include <rapidjson/filereadstream.h>

#include <Common/Exception.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>

// ONNX protobuf for model introspection (generated during ORT build)
#include <onnx/onnx-ml.pb.h>

#include <chrono>
#include <cmath>
#include <set>
#include <fstream>
#include <cstdio>
#include <unordered_map>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_PARSE_INPUT_ASSERTION_FAILED;
    extern const int INCORRECT_DATA;
}

// ============================================================
// Tokenizer (supports both BPE and WordPiece from tokenizer.json)
// ============================================================

namespace
{

/// UTF-8 character iteration helpers.
inline size_t utf8CharLen(unsigned char c)
{
    if (c < 0x80) return 1;
    if ((c & 0xE0) == 0xC0) return 2;
    if ((c & 0xF0) == 0xE0) return 3;
    return 4;
}

std::vector<std::string> splitUtf8Chars(std::string_view text)
{
    std::vector<std::string> chars;
    chars.reserve(text.size());
    size_t i = 0;
    while (i < text.size())
    {
        size_t len = utf8CharLen(static_cast<unsigned char>(text[i]));
        len = std::min(len, text.size() - i);
        chars.emplace_back(text.data() + i, len);
        i += len;
    }
    return chars;
}

/// SentencePiece-style space replacement for BPE tokenizers.
std::string normalizeBPE(std::string_view text)
{
    static constexpr std::string_view SPIECE_UNDERLINE = "\xe2\x96\x81";
    std::string out;
    out.reserve(text.size());
    for (char c : text)
    {
        if (c == ' ')
            out += SPIECE_UNDERLINE;
        else
            out += c;
    }
    return out;
}

struct MergePairHash
{
    size_t operator()(const std::pair<std::string, std::string> & p) const
    {
        size_t h1 = std::hash<std::string>{}(p.first);
        size_t h2 = std::hash<std::string>{}(p.second);
        return h1 ^ (h2 * 0x9e3779b97f4a7c15ULL);
    }
};

enum class TokenizerType { BPE, WordPiece };

struct Tokenizer
{
    TokenizerType type = TokenizerType::BPE;
    std::unordered_map<std::string, int32_t> vocab;
    std::unordered_map<std::pair<std::string, std::string>, int32_t, MergePairHash> merge_ranks; // BPE only
    int32_t cls_id = 101;   // [CLS] for WordPiece, <bos> for BPE
    int32_t sep_id = 102;   // [SEP] for WordPiece, <eos> for BPE
    int32_t unk_id = 100;   // [UNK] for WordPiece, <unk> for BPE
    int32_t pad_id = 0;
    std::string continuing_subword_prefix = "##";  // WordPiece only

    // ---- BPE encoding ----
    std::vector<int32_t> encodeBPE(const std::string & text) const
    {
        auto symbols = splitUtf8Chars(text);
        if (symbols.empty()) return {};

        while (symbols.size() > 1)
        {
            int best_rank = INT_MAX;
            int best_pos = -1;
            for (int i = 0; i < static_cast<int>(symbols.size()) - 1; ++i)
            {
                auto it = merge_ranks.find({symbols[i], symbols[i + 1]});
                if (it != merge_ranks.end() && it->second < best_rank)
                {
                    best_rank = it->second;
                    best_pos = i;
                }
            }
            if (best_pos < 0) break;
            symbols[best_pos] += symbols[best_pos + 1];
            symbols.erase(symbols.begin() + best_pos + 1);
        }

        std::vector<int32_t> ids;
        ids.reserve(symbols.size());
        for (const auto & sym : symbols)
        {
            auto it = vocab.find(sym);
            ids.push_back(it != vocab.end() ? it->second : unk_id);
        }
        return ids;
    }

    // ---- WordPiece encoding ----
    std::vector<int32_t> encodeWordPiece(std::string_view text) const
    {
        std::vector<int32_t> ids;

        // Simple whitespace + punctuation tokenization (BERT basic tokenizer)
        size_t i = 0;
        while (i < text.size())
        {
            // Skip whitespace
            while (i < text.size() && (text[i] == ' ' || text[i] == '\t' || text[i] == '\n' || text[i] == '\r'))
                ++i;
            if (i >= text.size()) break;

            // Extract a word (split on whitespace and punctuation)
            size_t word_start = i;
            bool is_punct = false;

            unsigned char c = static_cast<unsigned char>(text[i]);
            if (c < 0x80 && std::ispunct(c))
            {
                // Single punctuation character is its own token
                ++i;
                is_punct = true;
            }
            else
            {
                // Consume until whitespace or punctuation
                while (i < text.size())
                {
                    unsigned char ch = static_cast<unsigned char>(text[i]);
                    if (ch < 0x80 && (std::isspace(ch) || std::ispunct(ch)))
                        break;
                    i += utf8CharLen(ch);
                }
            }

            std::string word(text.data() + word_start, i - word_start);

            // Lowercase for uncased models
            for (auto & ch : word)
                if (ch >= 'A' && ch <= 'Z')
                    ch = ch - 'A' + 'a';

            if (is_punct || word.size() <= 1)
            {
                auto it = vocab.find(word);
                ids.push_back(it != vocab.end() ? it->second : unk_id);
                continue;
            }

            // WordPiece greedy longest-match
            size_t start = 0;
            bool bad = false;
            while (start < word.size())
            {
                size_t end = word.size();
                bool found = false;
                while (end > start)
                {
                    std::string substr = word.substr(start, end - start);
                    if (start > 0)
                        substr = continuing_subword_prefix + substr;
                    auto it = vocab.find(substr);
                    if (it != vocab.end())
                    {
                        ids.push_back(it->second);
                        found = true;
                        break;
                    }
                    // Try shorter
                    size_t prev = end - 1;
                    // Respect UTF-8 boundaries
                    while (prev > start && (static_cast<unsigned char>(word[prev]) & 0xC0) == 0x80)
                        --prev;
                    end = prev;
                }
                if (!found)
                {
                    bad = true;
                    break;
                }
                start = end;
            }
            if (bad)
                ids.push_back(unk_id);
        }
        return ids;
    }

    /// Encode text: tokenize → add special tokens → truncate.
    std::vector<int64_t> encode(std::string_view text, size_t max_len) const
    {
        std::vector<int32_t> piece_ids;
        if (type == TokenizerType::BPE)
        {
            std::string normalized = normalizeBPE(text);
            piece_ids = encodeBPE(normalized);
        }
        else
        {
            piece_ids = encodeWordPiece(text);
        }

        // Truncate to max_len - 2 (for cls/sep or bos/eos)
        size_t max_content = max_len >= 2 ? max_len - 2 : 0;
        if (piece_ids.size() > max_content)
            piece_ids.resize(max_content);

        std::vector<int64_t> ids;
        ids.reserve(piece_ids.size() + 2);
        ids.push_back(cls_id);
        for (int32_t id : piece_ids) ids.push_back(id);
        ids.push_back(sep_id);
        return ids;
    }
};

std::string readFile(const std::string & path)
{
    ReadBufferFromFile in(path);
    String contents;
    readStringUntilEOF(contents, in);
    return contents;
}

Tokenizer loadTokenizer(const std::string & tokenizer_path)
{
    LoggerPtr log = getLogger("OnnxEmbeddingModel");
    LOG_INFO(log, "Loading tokenizer from: {}", tokenizer_path);

    std::string json_str = readFile(tokenizer_path);
    rapidjson::Document doc;
    doc.Parse(json_str.c_str());
    if (doc.HasParseError())
        throw Exception(ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED, "Failed to parse tokenizer.json");

    Tokenizer tok;

    const auto & model = doc["model"];
    std::string model_type = model["type"].GetString();

    // Load vocab
    const auto & vocab_obj = model["vocab"];
    if (model_type == "WordPiece")
    {
        tok.type = TokenizerType::WordPiece;
        tok.vocab.reserve(vocab_obj.MemberCount());
        for (auto it = vocab_obj.MemberBegin(); it != vocab_obj.MemberEnd(); ++it)
            tok.vocab[it->name.GetString()] = it->value.GetInt();

        if (model.HasMember("continuing_subword_prefix"))
            tok.continuing_subword_prefix = model["continuing_subword_prefix"].GetString();

        // Default BERT special tokens
        tok.cls_id = 101; tok.sep_id = 102; tok.unk_id = 100; tok.pad_id = 0;
    }
    else
    {
        tok.type = TokenizerType::BPE;
        tok.vocab.reserve(vocab_obj.MemberCount());
        for (auto it = vocab_obj.MemberBegin(); it != vocab_obj.MemberEnd(); ++it)
            tok.vocab[it->name.GetString()] = it->value.GetInt();

        // Load merges
        const auto & merges_arr = model["merges"];
        tok.merge_ranks.reserve(merges_arr.Size());
        for (rapidjson::SizeType i = 0; i < merges_arr.Size(); ++i)
        {
            const auto & pair_arr = merges_arr[i];
            tok.merge_ranks[{pair_arr[0].GetString(), pair_arr[1].GetString()}] = static_cast<int32_t>(i);
        }

        // BPE defaults
        tok.cls_id = 2; tok.sep_id = 1; tok.unk_id = 3; tok.pad_id = 2;
    }

    // Override special token IDs from added_tokens
    if (doc.HasMember("added_tokens"))
    {
        for (const auto & token : doc["added_tokens"].GetArray())
        {
            std::string content = token["content"].GetString();
            int32_t id = token["id"].GetInt();
            if (content == "<bos>" || content == "[CLS]") tok.cls_id = id;
            else if (content == "<eos>" || content == "[SEP]") tok.sep_id = id;
            else if (content == "<unk>" || content == "[UNK]") tok.unk_id = id;
            else if (content == "<pad>" || content == "[PAD]") tok.pad_id = id;
        }
    }

    LOG_INFO(log, "Tokenizer loaded: type={}, vocab_size={}, cls={}, sep={}, unk={}",
        model_type, tok.vocab.size(), tok.cls_id, tok.sep_id, tok.unk_id);
    return tok;
}


} // anonymous namespace

// ============================================================
// OnnxEmbeddingModel::Impl
// ============================================================

struct OnnxEmbeddingModel::Impl
{
    Tokenizer tokenizer;
    Ort::Env env{ORT_LOGGING_LEVEL_WARNING, "OnnxEmbeddingModel"};
    std::unique_ptr<Ort::Session> session;

    // Auto-detected from model at load time
    std::vector<std::string> input_names;
    std::string output_name;
    size_t output_dims = 768;
    size_t output_rank = 2;
    bool has_token_type_ids = false;
    bool inputs_are_int32 = false;

    // Model info (extracted at load time for system table)
    std::string producer;
    std::string domain;
    int64_t ir_version = 0;
    int64_t opset_version = 0;
    int intra_op_threads = 0;
    std::string model_path;

    // From ONNX proto (parsed once at load time)
    size_t num_ops = 0;
    size_t num_layers = 0;       // attention layers
    size_t num_heads = 0;
    size_t num_parameters = 0;
    std::vector<std::string> op_types;
    std::string quantization = "fp32";
    bool has_external_data = false;
};

// ============================================================
// OnnxEmbeddingModel
// ============================================================

OnnxEmbeddingModel::~OnnxEmbeddingModel() = default;

OnnxEmbeddingModel::OnnxEmbeddingModel(
    const std::string & model_path, const std::string & tokenizer_path, int intra_threads)
    : impl(std::make_unique<Impl>())
{
    init(model_path, tokenizer_path, intra_threads);
}

void OnnxEmbeddingModel::init(const std::string & model_path, const std::string & tokenizer_path, int intra_threads)
{
    LoggerPtr log = getLogger("OnnxEmbeddingModel");
    LOG_INFO(log, "Loading OnnxEmbeddingModel...");

    impl->tokenizer = loadTokenizer(tokenizer_path);
    impl->intra_op_threads = intra_threads;
    impl->model_path = model_path;

    Ort::SessionOptions opts;
    opts.SetIntraOpNumThreads(intra_threads);
    opts.SetInterOpNumThreads(1);
    opts.SetGraphOptimizationLevel(ORT_ENABLE_ALL);
    opts.SetExecutionMode(ORT_SEQUENTIAL);

    impl->session = std::make_unique<Ort::Session>(impl->env, model_path.c_str(), opts);

    Ort::AllocatorWithDefaultOptions alloc;

    // Extract model metadata from session
    {
        auto metadata = impl->session->GetModelMetadata();
        impl->producer = metadata.GetProducerNameAllocated(alloc).get();
        impl->domain = metadata.GetDomainAllocated(alloc).get();
        /// Ok: GetOpset is best-effort metadata; if unsupported or throws, fall back to 0
        /// and let the proto-parsing block below recover it from opset_import.
        try { impl->opset_version = impl->session->GetOpset(""); } catch (...) { impl->opset_version = 0; }
    }

    // Parse ONNX proto for graph-level info (ops, layers, quantization, parameters)
    try
    {
        onnx::ModelProto proto;
        std::ifstream onnx_file(model_path, std::ios::binary);
        if (onnx_file && proto.ParseFromIstream(&onnx_file))
        {
            impl->ir_version = proto.ir_version();
            if (impl->opset_version == 0 && proto.opset_import_size() > 0)
                impl->opset_version = proto.opset_import(0).version();

            const auto & graph = proto.graph();
            impl->num_ops = graph.node_size();

            std::set<std::string> op_set;
            for (int i = 0; i < graph.node_size(); ++i)
            {
                const auto & node = graph.node(i);
                op_set.insert(node.op_type());
                if (node.op_type() == "MultiHeadAttention" || node.op_type() == "Attention")
                {
                    ++impl->num_layers;
                    for (int a = 0; a < node.attribute_size(); ++a)
                        if (node.attribute(a).name() == "num_heads")
                            impl->num_heads = node.attribute(a).i();
                }
            }
            impl->op_types.assign(op_set.begin(), op_set.end());

            size_t total_params = 0;
            bool ext = false;
            bool i8 = false;
            bool fp16 = false;
            for (int i = 0; i < graph.initializer_size(); ++i)
            {
                const auto & t = graph.initializer(i);
                size_t elems = 1;
                for (int d = 0; d < t.dims_size(); ++d)
                    elems *= t.dims(d);
                total_params += elems;
                if (t.has_data_location() && t.data_location() == onnx::TensorProto::EXTERNAL)
                    ext = true;
                if (t.data_type() == onnx::TensorProto::INT8 || t.data_type() == onnx::TensorProto::UINT8)
                    i8 = true;
                if (t.data_type() == onnx::TensorProto::FLOAT16)
                    fp16 = true;
            }
            impl->num_parameters = total_params;
            impl->has_external_data = ext;
            if (i8) impl->quantization = "int8";
            else if (fp16) impl->quantization = "fp16";
            else impl->quantization = "fp32";

            LOG_INFO(log, "ONNX proto: ir={} opset={} ops={} layers={} heads={} params={} quant={}",
                impl->ir_version, impl->opset_version, impl->num_ops, impl->num_layers,
                impl->num_heads, impl->num_parameters, impl->quantization);
        }
    }
    catch (const std::exception & e)
    {
        LOG_WARNING(log, "Failed to parse ONNX proto: {}", e.what());
    }

    // Auto-detect input names, token_type_ids presence, and dtype
    for (size_t i = 0; i < impl->session->GetInputCount(); ++i)
    {
        std::string name = impl->session->GetInputNameAllocated(i, alloc).get();
        impl->input_names.push_back(name);
        if (name == "token_type_ids")
            impl->has_token_type_ids = true;
        LOG_DEBUG(log, "ORT input[{}]: {}", i, name);
    }
    // Detect int32 vs int64 from first input
    if (impl->session->GetInputCount() > 0)
    {
        auto type_info = impl->session->GetInputTypeInfo(0);
        auto tensor_info = type_info.GetTensorTypeAndShapeInfo();
        if (tensor_info.GetElementType() == ONNX_TENSOR_ELEMENT_DATA_TYPE_INT32)
            impl->inputs_are_int32 = true;
    }

    // Auto-detect output name, dims, and rank (pooled vs per-token)
    // Use first output by default; prefer "sentence_embedding" if present
    for (size_t i = 0; i < impl->session->GetOutputCount(); ++i)
    {
        std::string name = impl->session->GetOutputNameAllocated(i, alloc).get();
        LOG_DEBUG(log, "ORT output[{}]: {}", i, name);

        auto type_info = impl->session->GetOutputTypeInfo(i);
        auto tensor_info = type_info.GetTensorTypeAndShapeInfo();
        auto shape = tensor_info.GetShape();

        if (i == 0 || name == "sentence_embedding")
        {
            impl->output_name = name;
            impl->output_rank = shape.size();
            // dims is always the last dimension
            if (!shape.empty() && shape.back() > 0)
                impl->output_dims = static_cast<size_t>(shape.back());
        }
    }

    // Validate this is a text embedding model
    bool has_input_ids = false;
    for (const auto & name : impl->input_names)
        if (name == "input_ids") has_input_ids = true;
    if (!has_input_ids)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "OnnxEmbeddingModel: model has no 'input_ids' input — this doesn't appear to be a text model. "
            "Expected inputs: input_ids, attention_mask [, token_type_ids].");

    if (impl->output_name.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "OnnxEmbeddingModel: model has no outputs.");

    {
        auto type_info = impl->session->GetOutputTypeInfo(0);
        auto tensor_info = type_info.GetTensorTypeAndShapeInfo();
        auto elem_type = tensor_info.GetElementType();
        if (elem_type != ONNX_TENSOR_ELEMENT_DATA_TYPE_FLOAT && elem_type != ONNX_TENSOR_ELEMENT_DATA_TYPE_FLOAT16)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "OnnxEmbeddingModel: output tensor is not float — this doesn't appear to be an embedding model. "
                "Expected float32 or float16 output.");
    }

    if (impl->output_rank != 2 && impl->output_rank != 3)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "OnnxEmbeddingModel: output rank is {} — expected 2 [batch, dims] or 3 [batch, seq, dims] for an embedding model.",
            impl->output_rank);

    if (impl->output_dims <= 1)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "OnnxEmbeddingModel: output dimension is {} — expected >1 for an embedding model. "
            "This looks like a classification model.", impl->output_dims);

    LOG_INFO(log, "OnnxEmbeddingModel loaded: path={} inputs={} output={} dims={} rank={} token_type_ids={} int32={}",
        model_path, impl->input_names.size(), impl->output_name, impl->output_dims,
        impl->output_rank, impl->has_token_type_ids, impl->inputs_are_int32);
}

size_t OnnxEmbeddingModel::getMaxDims() const
{
    return impl->output_dims;
}

size_t OnnxEmbeddingModel::getVocabSize() const
{
    return impl->tokenizer.vocab.size();
}

size_t OnnxEmbeddingModel::getMemoryBytes() const
{
    // Parameters from ONNX proto (counted at init) × bytes per param.
    // fp32 = 4 bytes, fp16 = 2, int8 = 1. Default to fp32.
    size_t bytes_per_param = 4;
    if (impl->quantization == "fp16") bytes_per_param = 2;
    else if (impl->quantization == "int8") bytes_per_param = 1;
    return impl->num_parameters * bytes_per_param;
}

OnnxEmbeddingModel::OnnxModelInfo OnnxEmbeddingModel::getModelInfo() const
{
    OnnxModelInfo info;

    info.producer = impl->producer;
    info.domain = impl->domain;
    info.ir_version = impl->ir_version;
    info.opset_version = impl->opset_version;
    info.num_layers = impl->num_layers;
    info.num_heads = impl->num_heads;
    info.hidden_size = impl->output_dims;
    info.max_seq_length = kMaxSeqLen;
    info.num_parameters = impl->num_parameters;
    info.num_ops = impl->num_ops;
    info.op_types = impl->op_types;
    info.quantization = impl->quantization;
    info.has_external_data = impl->has_external_data;
    info.tokenizer_type = impl->tokenizer.type == TokenizerType::BPE ? "BPE" : "WordPiece";
    info.vocab_size = impl->tokenizer.vocab.size();
    info.intra_op_threads = impl->intra_op_threads;
    info.execution_provider = "CPUExecutionProvider";

    return info;
}

size_t OnnxEmbeddingModel::validateDims(size_t dims) const
{
    if (dims == 0) return std::min(kDenseDefaultDims, impl->output_dims);
    return std::min(dims, impl->output_dims);
}

std::vector<int32_t> OnnxEmbeddingModel::tokenize(std::string_view text) const
{
    auto ids64 = impl->tokenizer.encode(text, kMaxSeqLen);
    std::vector<int32_t> ids32(ids64.size());
    for (size_t i = 0; i < ids64.size(); ++i)
        ids32[i] = static_cast<int32_t>(ids64[i]);
    return ids32;
}

std::vector<std::string> OnnxEmbeddingModel::tokenizeToStrings(std::string_view text) const
{
    auto ids = impl->tokenizer.encode(text, kMaxSeqLen);
    std::vector<std::string> result;
    result.reserve(ids.size());
    for (auto id : ids)
    {
        // Reverse lookup: find token string by ID
        for (const auto & [token, token_id] : impl->tokenizer.vocab)
        {
            if (token_id == static_cast<int32_t>(id))
            {
                result.push_back(token);
                break;
            }
        }
    }
    return result;
}


template <typename T>
std::vector<std::vector<float>> OnnxEmbeddingModel::extractEmbeddings(
    const std::vector<Ort::Value> & outputs,
    size_t batch,
    size_t max_len,
    const std::vector<std::vector<int64_t>> & all_ids,
    size_t dims) const
    requires(std::is_same_v<T, Ort::Float16_t> || std::is_same_v<T, float>)
{
    const T * out_data = outputs[0].GetTensorData<T>();
    const size_t model_dims = impl->output_dims;
    std::vector<std::vector<float>> result(batch);

    auto to_float = [](T v) -> float
    {
        if constexpr (std::is_same_v<T, Ort::Float16_t>)
            return v.ToFloat();
        else
            return v;
    };

    if (impl->output_rank == 3)
    {
        // Per-token output [batch, seq, dims] — need mean pooling over non-padded tokens
        for (size_t i = 0; i < batch; ++i)
        {
            const T * token_embs = out_data + i * max_len * model_dims;
            const size_t seq_len = all_ids[i].size(); // actual (non-padded) length

            // Mean pool: sum embeddings of real tokens, divide by count
            result[i].assign(dims, 0.0f);
            for (size_t t = 0; t < seq_len; ++t)
                for (size_t d = 0; d < dims; ++d)
                    result[i][d] += to_float(token_embs[t * model_dims + d]);

            float inv_count = seq_len > 0 ? 1.0f / static_cast<float>(seq_len) : 0.0f;
            for (size_t d = 0; d < dims; ++d)
                result[i][d] *= inv_count;

            // L2 normalize
            float norm_sq = 0.0f;
            for (size_t d = 0; d < dims; ++d)
                norm_sq += result[i][d] * result[i][d];
            float inv = norm_sq > 0.0f ? 1.0f / std::sqrt(norm_sq) : 0.0f;
            for (size_t d = 0; d < dims; ++d)
                result[i][d] *= inv;
        }
    }
    else
    {
        // Already pooled [batch, dims] — just truncate and normalize
        for (size_t i = 0; i < batch; ++i)
        {
            const T * row = out_data + i * model_dims;
            result[i].resize(dims);
            for (size_t d = 0; d < dims; ++d)
                result[i][d] = to_float(row[d]);

            float norm_sq = 0.0f;
            for (size_t d = 0; d < dims; ++d)
                norm_sq += result[i][d] * result[i][d];
            float inv = norm_sq > 0.0f ? 1.0f / std::sqrt(norm_sq) : 0.0f;
            for (size_t d = 0; d < dims; ++d)
                result[i][d] *= inv;
        }
    }

    return result;
}

template std::vector<std::vector<float>> OnnxEmbeddingModel::extractEmbeddings<float>(
    const std::vector<Ort::Value> &, size_t, size_t, const std::vector<std::vector<int64_t>> &, size_t) const;
template std::vector<std::vector<float>> OnnxEmbeddingModel::extractEmbeddings<Ort::Float16_t>(
    const std::vector<Ort::Value> &, size_t, size_t, const std::vector<std::vector<int64_t>> &, size_t) const;

std::vector<std::vector<float>> OnnxEmbeddingModel::embedBatch(
    const std::vector<std::string_view> & texts, size_t dims) const
{
    dims = validateDims(dims);
    const size_t batch = texts.size();
    if (batch == 0) return {};

    /// Sub-batch large inputs to cap memory and improve GEMM efficiency
    static constexpr size_t MAX_BATCH = 16;
    if (batch > MAX_BATCH)
    {
        std::vector<std::vector<float>> result(batch);
        for (size_t i = 0; i < batch; i += MAX_BATCH)
        {
            size_t end = std::min(i + MAX_BATCH, batch);
            std::vector<std::string_view> sub(texts.begin() + i, texts.begin() + end);
            auto sub_embs = embedBatch(sub, dims);
            for (size_t j = 0; j < sub_embs.size(); ++j)
                result[i + j] = std::move(sub_embs[j]);
        }
        return result;
    }

    using Clock = std::chrono::steady_clock;
    using Ms = std::chrono::milliseconds;
    auto ts = [](auto a, auto b){ return std::chrono::duration_cast<Ms>(b-a).count(); };

    // Tokenize all texts
    auto t0 = Clock::now();
    std::vector<std::vector<int64_t>> all_ids(batch);
    size_t max_len = 0;
    for (size_t i = 0; i < batch; ++i)
    {
        all_ids[i] = impl->tokenizer.encode(texts[i], kMaxSeqLen);
        max_len = std::max(max_len, all_ids[i].size());
    }

    // Build padded [batch x max_len] tensors
    auto t1 = Clock::now();
    const size_t flat_size = batch * max_len;
    std::array<int64_t, 2> shape = {static_cast<int64_t>(batch), static_cast<int64_t>(max_len)};
    thread_local Ort::MemoryInfo mem = Ort::MemoryInfo::CreateCpu(OrtArenaAllocator, OrtMemTypeDefault);

    // Prepare input name pointers
    std::vector<const char *> input_name_ptrs;
    input_name_ptrs.reserve(impl->input_names.size());
    for (const auto & n : impl->input_names)
        input_name_ptrs.push_back(n.c_str());

    std::vector<Ort::Value> inputs;
    inputs.reserve(impl->input_names.size());

    /// Storage for int32 or int64 — only one set is used. The vectors must outlive
    /// session->Run because Ort::Value tensors below are non-owning views over them.
    std::vector<int32_t> ids_i32;
    std::vector<int32_t> mask_i32;
    std::vector<int32_t> ttype_i32;
    std::vector<int64_t> ids_i64;
    std::vector<int64_t> mask_i64;
    std::vector<int64_t> ttype_i64;

    auto prepare = [&]<typename I>(std::vector<I> & ids, std::vector<I> & mask, std::vector<I> & ttype)
    {
        ids.resize(flat_size, static_cast<I>(impl->tokenizer.pad_id));
        mask.resize(flat_size, 0);
        for (size_t i = 0; i < batch; ++i)
            for (size_t j = 0; j < all_ids[i].size(); ++j)
            {
                ids[i * max_len + j] = static_cast<I>(all_ids[i][j]);
                mask[i * max_len + j] = 1;
            }

        // Build tensors in model's input order
        for (const auto & name : impl->input_names)
        {
            if (name == "token_type_ids")
            {
                ttype.resize(flat_size, 0);
                inputs.push_back(Ort::Value::CreateTensor<I>(mem, ttype.data(), flat_size, shape.data(), 2));
            }
            else if (name.find("mask") != std::string::npos)
                inputs.push_back(Ort::Value::CreateTensor<I>(mem, mask.data(), flat_size, shape.data(), 2));
            else
                inputs.push_back(Ort::Value::CreateTensor<I>(mem, ids.data(), flat_size, shape.data(), 2));
        }
    };

    if (impl->inputs_are_int32)
        prepare(ids_i32, mask_i32, ttype_i32);
    else
        prepare(ids_i64, mask_i64, ttype_i64);

    const char * output_names[] = {impl->output_name.c_str()};

    Ort::RunOptions run_opts;
    auto t2 = Clock::now();
    auto outputs = impl->session->Run(
        run_opts, input_name_ptrs.data(), inputs.data(), inputs.size(), output_names, 1);
    auto t3 = Clock::now();

    // Extract embeddings, handling both pooled [batch, dims] and per-token [batch, seq, dims] output
    auto type_info = impl->session->GetOutputTypeInfo(0);
    auto tensor_info = type_info.GetTensorTypeAndShapeInfo();
    auto elem_type = tensor_info.GetElementType();
    std::vector<std::vector<float>> result;
    if (elem_type == ONNX_TENSOR_ELEMENT_DATA_TYPE_FLOAT16)
        result = extractEmbeddings<Ort::Float16_t>(outputs, batch, max_len, all_ids, dims);
    else if (elem_type == ONNX_TENSOR_ELEMENT_DATA_TYPE_FLOAT)
        result = extractEmbeddings<float>(outputs, batch, max_len, all_ids, dims);
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported output element type: {}", static_cast<int>(elem_type));

    auto t4 = Clock::now();
    LoggerPtr log = getLogger("OnnxEmbeddingModel");
    auto total_us = std::chrono::duration_cast<std::chrono::microseconds>(t4 - t0).count();
    LOG_DEBUG(log, "embedBatch batch={} max_seq={} model_dims={} req_dims={} rank={} | tokenize={}ms prepare={}ms run={}ms extract={}ms total={}ms",
        batch, max_len, impl->output_dims, dims, impl->output_rank, ts(t0,t1), ts(t1,t2), ts(t2,t3), ts(t3,t4), ts(t0,t4));

    // Update inference stats
    size_t total_tokens = 0;
    for (const auto & ids : all_ids)
        total_tokens += ids.size();
    stats.total_calls.fetch_add(1, std::memory_order_relaxed);
    stats.total_tokens.fetch_add(total_tokens, std::memory_order_relaxed);
    stats.total_time_us.fetch_add(static_cast<uint64_t>(total_us), std::memory_order_relaxed);

    return result;
}

std::string OnnxEmbeddingModel::getMetadataJSON() const
{
    return fmt::format(R"({{"producer":"{}","domain":"{}","ir_version":{},"opset_version":{}}})",
        impl->producer, impl->domain, impl->ir_version, impl->opset_version);
}

std::string OnnxEmbeddingModel::getArchitectureJSON() const
{
    std::string ops;
    for (const auto & op : impl->op_types)
    {
        if (!ops.empty()) ops += ",";
        ops += "\"" + op + "\"";
    }
    return fmt::format(
        R"({{"hidden_size":{},"default_dims":{},"num_layers":{},"num_heads":{},"max_seq_length":{},"num_parameters":{},"num_ops":{},"op_types":[{}]}})",
        impl->output_dims, kDenseDefaultDims, impl->num_layers, impl->num_heads,
        kMaxSeqLen, impl->num_parameters, impl->num_ops, ops);
}

std::string OnnxEmbeddingModel::getQuantizationJSON() const
{
    return fmt::format(R"({{"quantization":"{}","has_external_data":{}}})",
        impl->quantization, impl->has_external_data);
}

std::string OnnxEmbeddingModel::getTokenizerJSON() const
{
    return fmt::format(R"({{"type":"{}","vocab_size":{}}})",
        impl->tokenizer.type == TokenizerType::BPE ? "BPE" : "WordPiece",
        impl->tokenizer.vocab.size());
}

std::string OnnxEmbeddingModel::getRuntimeJSON() const
{
    return fmt::format(R"({{"backend":"onnxruntime","execution_provider":"CPUExecutionProvider","intra_op_threads":{}}})",
        impl->intra_op_threads);
}

std::string OnnxEmbeddingModel::getStatsJSON() const
{
    uint64_t calls = stats.total_calls.load(std::memory_order_relaxed);
    uint64_t tokens = stats.total_tokens.load(std::memory_order_relaxed);
    uint64_t time_us = stats.total_time_us.load(std::memory_order_relaxed);
    double avg_ms = calls > 0 ? static_cast<double>(time_us) / 1000.0 / static_cast<double>(calls) : 0.0;
    return fmt::format(R"({{"total_calls":{},"total_tokens":{},"total_time_ms":{},"avg_latency_ms":{:.2f}}})",
        calls, tokens, time_us / 1000, avg_ms);
}

std::vector<float> OnnxEmbeddingModel::embedText(std::string_view text, size_t dims) const
{
    auto batch = embedBatch({text}, dims);
    return batch.empty() ? std::vector<float>(validateDims(dims), 0.0f) : std::move(batch[0]);
}

}
#endif
