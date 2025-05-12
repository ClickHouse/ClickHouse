#pragma once

#include <mutex>

#include <Models/IModel.h>

namespace DB
{

/// Registry for the existing models
/// Currently saves all models in Memery
/// TODO: think about moving them to DB
class ModelRegistry
{
public:
    static ModelRegistry & instance();

    /// Register a new model.
    /// Creates an IModel class using ModelSpec within registration process.
    /// Returns new registered model.
    ModelPtr registerModel(const String& model_name, ModelPtr model);

    /// Retrieve registered model.
    /// Throws if model is not found.
    ModelPtr getModel(const String & model_name) const;

    /// Check wether model exists
    bool hasModel(const String& model_name) const;

    // Unregisters existing model.
    /// Throws if model is not found.
    void unregisterModel(const String& model_name);

private:
    mutable std::mutex mutex;
    std::unordered_map<String, ModelPtr> models;
};

} // namespace DB
