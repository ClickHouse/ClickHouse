#pragma once

#include <Core/BaseSettings.h>
#include <Core/Settings.h>

namespace DB
{
    class ASTStorage;


#define MODEL_RELATED_SETTINGS(M) \
    M(Float, lambda, 0.0, "Regularization coefficient.", 0) \
    M(String, optimizer, "lbfgs", "Optimizer.", 0) \
    M(Float, delta, 1.0, "Margin of difference between correct class and other classes.", 0) \

#define LIST_OF_MODEL_SETTINGS(M) \
    MODEL_RELATED_SETTINGS(M)

DECLARE_SETTINGS_TRAITS(ModelSettingsTraits, LIST_OF_MODEL_SETTINGS)

struct ModelSettings : public BaseSettings<ModelSettingsTraits>
{
    void loadFromQuery(ASTStorage & storage_def);
};
}
