#include <Functions/Embedding/EmbeddingModelRegistry.h>

#if defined(USE_ONNXRUNTIME)
#include <Functions/Embedding/OnnxEmbeddingModel.h>
#endif

#if defined(USE_LLAMACPP)
#include <Functions/Embedding/LlamaCppEmbeddingModel.h>
#endif

#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Poco/Util/AbstractConfiguration.h>

#include <shared_mutex>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

EmbeddingModelRegistry & EmbeddingModelRegistry::instance()
{
    static EmbeddingModelRegistry reg;
    return reg;
}

/// Build a new model map from config. Loads all models eagerly.
/// Preserves already-loaded models whose config didn't change.
static EmbeddingModelRegistry::ModelMap buildModelMap(
    const Poco::Util::AbstractConfiguration & config,
    const EmbeddingModelRegistry::ModelMap & old_models)
{
    LoggerPtr log = getLogger("EmbeddingModelRegistry");
    EmbeddingModelRegistry::ModelMap new_models;

    if (!config.has("embedding"))
    {
        LOG_DEBUG(log, "No <embedding> config section");
        return new_models;
    }

    Poco::Util::AbstractConfiguration::Keys model_keys;
    config.keys("embedding", model_keys);

    for (const auto & key : model_keys)
    {
        if (!key.starts_with("model"))
            continue;

        std::string prefix = "embedding." + key;
        std::string id = config.getString(prefix + ".id", "");
        std::string type = config.getString(prefix + ".type", "");
        std::string model_path = config.getString(prefix + ".model_path", "");
        std::string tokenizer_path = config.getString(prefix + ".tokenizer_path", "");
        std::string model_name = config.getString(prefix + ".name", "");

        if (id.empty() || type.empty() || model_path.empty())
        {
            if (!id.empty())
                LOG_WARNING(log, "Skipping model '{}': missing type or model_path", id);
            continue;
        }

        // llamacpp bundles tokenizer in GGUF; others require separate tokenizer_path
        if (type != "llamacpp" && tokenizer_path.empty())
        {
            LOG_WARNING(log, "Skipping model '{}': missing tokenizer_path (required for type '{}')", id, type);
            continue;
        }

        // Reuse existing model if config unchanged
        auto it = old_models.find(id);
        if (it != old_models.end()
            && it->second.type == type
            && it->second.model_path == model_path
            && it->second.tokenizer_path == tokenizer_path
            && it->second.model)
        {
            new_models[id] = it->second;
            LOG_INFO(log, "Model '{}' preserved (config unchanged)", id);
            continue;
        }

        LOG_INFO(log, "Loading model '{}' (type={}, path={})", id, type, model_path);

        try
        {
            if (type == "onnx")
            {
#if defined(USE_ONNXRUNTIME)
                int intra_threads = config.getInt(prefix + ".config.intra_op_num_threads", 32);
                auto model = std::make_shared<OnnxEmbeddingModel>(model_path, tokenizer_path, intra_threads);
                LOG_INFO(log, "Model '{}' loaded as onnx (dims={})", id, model->getMaxDims());
                new_models[id] = {type, model_path, tokenizer_path, std::move(model)};
#else
                LOG_ERROR(log, "Model '{}': onnx requires ONNX Runtime", id);
#endif
            }
            else if (type == "llamacpp")
            {
#if defined(USE_LLAMACPP)
                int n_threads = config.getInt(prefix + ".config.n_threads", 32);
                auto model = std::make_shared<LlamaCppEmbeddingModel>(model_path, n_threads);
                LOG_INFO(log, "Model '{}' loaded as llamacpp (dims={})", id, model->getMaxDims());
                new_models[id] = {type, model_path, tokenizer_path, std::move(model)};
#else
                LOG_ERROR(log, "Model '{}': llamacpp requires the llama.cpp backend to be built", id);
#endif
            }
            else
            {
                LOG_WARNING(log, "Model '{}': unknown type '{}'", id, type);
            }
        }
        catch (const std::exception & e)
        {
            LOG_ERROR(log, "Failed to load model '{}': {}", id, e.what());
        }
    }

    LOG_INFO(log, "EmbeddingModelRegistry: {} models loaded", new_models.size());
    return new_models;
}

void EmbeddingModelRegistry::reloadConfig(const Poco::Util::AbstractConfiguration & config)
{
    // Build new map outside lock (model loading is slow)
    ModelMap old;
    {
        std::shared_lock lock(mutex);
        old = models;
    }

    ModelMap new_models = buildModelMap(config, old);

    // Brief exclusive lock: swap
    {
        std::unique_lock lock(mutex);
        models = std::move(new_models);
    }
}

std::shared_ptr<IEmbeddingModel> EmbeddingModelRegistry::getModel(const std::string & id) const
{
    if (id.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "No embedding model configured. Set the `embedding_model` setting to a model "
            "id registered under <embedding><model>...</model></embedding> in the server config.");

    std::shared_lock lock(mutex);

    auto it = models.find(id);
    if (it != models.end() && it->second.model)
        return it->second.model;

    throw Exception(ErrorCodes::BAD_ARGUMENTS,
        "Embedding model '{}' not found. Available models: {}", id, [this]()
        {
            std::string list;
            for (const auto & [k, v] : models)
            {
                if (!list.empty()) list += ", ";
                list += "'" + k + "'";
            }
            return list.empty() ? "(none)" : list;
        }());
}

bool EmbeddingModelRegistry::hasModel(const std::string & id) const
{
    if (id.empty()) return false;
    std::shared_lock lock(mutex);
    return models.contains(id);
}

std::vector<std::pair<std::string, EmbeddingModelRegistry::ModelEntry>> EmbeddingModelRegistry::snapshotEntries() const
{
    std::shared_lock lock(mutex);
    std::vector<std::pair<std::string, ModelEntry>> result;
    result.reserve(models.size());
    for (const auto & [id, entry] : models)
        result.emplace_back(id, entry);
    return result;
}

}
