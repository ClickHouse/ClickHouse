#pragma once

#include <Functions/Embedding/IEmbeddingModel.h>
#include <Common/SharedMutex.h>

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace Poco::Util { class AbstractConfiguration; }

namespace DB
{

/// Global registry of embedding models.
///
/// Models are configured in server config:
///   <embedding>
///       <model>
///           <id>my-model</id>
///           <type>onnx</type>  <!-- or "llamacpp" -->
///           <model_path>/path/to/model.onnx</model_path>
///           <tokenizer_path>/path/to/tokenizer.json</tokenizer_path>
///           <config>
///               <intra_op_num_threads>32</intra_op_num_threads>
///           </config>
///       </model>
///   </embedding>
///
/// A model id must be configured; there is no built-in default.
///
/// Usage from functions:
///   auto model = EmbeddingModelRegistry::instance().getModel(model_id);

class EmbeddingModelRegistry
{
public:
    struct ModelEntry
    {
        std::string type;
        std::string model_path;
        std::string tokenizer_path;
        std::shared_ptr<IEmbeddingModel> model;
    };

    using ModelMap = std::unordered_map<std::string, ModelEntry>;

    static EmbeddingModelRegistry & instance();

    /// Get model by ID. Throws BAD_ARGUMENTS if id is empty or not found.
    /// Returns a shared_ptr so callers keep the model alive across reloadConfig swaps.
    std::shared_ptr<IEmbeddingModel> getModel(const std::string & id) const;

    bool hasModel(const std::string & id) const;

    /// Snapshot of all entries under a single lock (used by system.embedding_models).
    std::vector<std::pair<std::string, ModelEntry>> snapshotEntries() const;

    /// Reload: read config, load all models, swap. Non-blocking for readers.
    void reloadConfig(const Poco::Util::AbstractConfiguration & config);

private:
    EmbeddingModelRegistry() = default;

    ModelMap models;
    mutable SharedMutex mutex;
};

}
