#pragma once

#include <Core/BaseSettings.h>
#include <Core/Settings.h>

namespace DB
{
    class ASTStorage;


#define MLPACK_RELATED_SETTINGS(M) \
    M(Float, lambda, 0.0, "Regularization coefficient.", 0) \

#define LIST_OF_MLPACK_SETTINGS(M) \
    MLPACK_RELATED_SETTINGS(M)
    // FORMAT_FACTORY_SETTINGS(M)

DECLARE_SETTINGS_TRAITS(MLpackSettingsTraits, LIST_OF_MLPACK_SETTINGS)

struct LinRegSettings : public BaseSettings<MLpackSettingsTraits>
{
    void loadFromQuery(ASTStorage & storage_def);
};
}
