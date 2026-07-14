#pragma once

#include <mutex>

#include <Models/IModel.h>

namespace DB
{

/// Registry for the existing models.
/// Currently keeps all models in memory.
/// TODO: think about moving them to a database.
class ModelRegistry
{
public:
    static ModelRegistry & instance();

    /// Register a new model under the given name.
    /// Throws if a model with this name already exists.
    /// Returns the registered model.
    ModelPtr registerModel(const String & model_name, ModelPtr model);

    /// Retrieve a registered model.
    /// Throws if the model is not found.
    ModelPtr getModel(const String & model_name) const;

    /// Remove a model from the registry as a single locked operation.
    /// Throws if the model is not found, unless `if_exists` is set.
    void dropModel(const String & model_name, bool if_exists);

private:
    mutable std::mutex mutex;
    std::unordered_map<String, ModelPtr> models;
};

}
