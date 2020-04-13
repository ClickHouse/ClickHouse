#pragma once
#include <Interpreters/IExternalLoadable.h>
#include <Columns/IColumn.h>
#include <Columns/ColumnsNumber.h>


namespace DB
{

/// CatBoost wrapper interface functions.
struct CatBoostWrapperAPI;
class CatBoostWrapperAPIProvider
{
public:
    virtual ~CatBoostWrapperAPIProvider() = default;
    virtual const CatBoostWrapperAPI & getAPI() const = 0;
};

/// CatBoost model interface.
class ICatBoostModel
{
public:
    virtual ~ICatBoostModel() = default;
    /// Evaluate model. Use first `float_features_count` columns as float features,
    /// the others `cat_features_count` as categorical features.
    virtual ColumnPtr evaluate(const ColumnRawPtrs & columns) const = 0;

    virtual size_t getFloatFeaturesCount() const = 0;
    virtual size_t getCatFeaturesCount() const = 0;
    virtual size_t getTreeCount() const = 0;
};

class IDataType;
using DataTypePtr = std::shared_ptr<const IDataType>;

/// General ML model evaluator interface.
class IModel : public IExternalLoadable
{
public:
    virtual ColumnPtr evaluate(const ColumnRawPtrs & columns) const = 0;
    virtual std::string getTypeName() const = 0;
    virtual DataTypePtr getReturnType() const = 0;
};

class CatBoostModel : public IModel
{
public:
    CatBoostModel(std::string name, std::string model_path,
                  std::string lib_path, const ExternalLoadableLifetime & lifetime);

    ColumnPtr evaluate(const ColumnRawPtrs & columns) const override;
    std::string getTypeName() const override { return "catboost"; }

    size_t getFloatFeaturesCount() const;
    size_t getCatFeaturesCount() const;
    size_t getTreeCount() const;
    DataTypePtr getReturnType() const override;

    /// IExternalLoadable interface.

    const ExternalLoadableLifetime & getLifetime() const override;

    const std::string & getLoadableName() const override { return name; }

    bool supportUpdates() const override { return true; }

    bool isModified() const override;

    std::shared_ptr<const IExternalLoadable> clone() const override;

private:
    const std::string name;
    std::string model_path;
    std::string lib_path;
    ExternalLoadableLifetime lifetime;
    std::shared_ptr<CatBoostWrapperAPIProvider> api_provider;
    const CatBoostWrapperAPI * api;

    std::unique_ptr<ICatBoostModel> model;

    size_t float_features_count;
    size_t cat_features_count;
    size_t tree_count;

    void init();
};

}
