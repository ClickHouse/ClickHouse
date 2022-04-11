#pragma once

#include <Interpreters/IExternalLoadable.h>
#include <Columns/IColumn.h>
#include <Columns/ColumnsNumber.h>


namespace DB
{

class CatBoostLibHolder;
class CatBoostWrapperAPI;
class CatBoostModelImpl;

class IDataType;
using DataTypePtr = std::shared_ptr<const IDataType>;

/// General ML model evaluator interface.
class IMLModel : public IExternalLoadable
{
public:
    IMLModel() = default;
    virtual ColumnPtr evaluate(const ColumnRawPtrs & columns) const = 0;
    virtual std::string getTypeName() const = 0;
    virtual DataTypePtr getReturnType() const = 0;
    virtual ~IMLModel() override = default;
};

class CatBoostModel : public IMLModel
{
public:
    CatBoostModel(std::string name, std::string model_path,
                  std::string lib_path, const ExternalLoadableLifetime & lifetime);

    ~CatBoostModel() override;

    ColumnPtr evaluate(const ColumnRawPtrs & columns) const override;
    std::string getTypeName() const override { return "catboost"; }

    size_t getFloatFeaturesCount() const;
    size_t getCatFeaturesCount() const;
    size_t getTreeCount() const;
    DataTypePtr getReturnType() const override;

    /// IExternalLoadable interface.

    const ExternalLoadableLifetime & getLifetime() const override { return lifetime; }

    std::string getLoadableName() const override { return name; }

    bool supportUpdates() const override { return true; }

    bool isModified() const override { return true; }

    std::shared_ptr<const IExternalLoadable> clone() const override
    {
        return std::make_shared<CatBoostModel>(name, model_path, lib_path, lifetime);
    }

private:
    const std::string name;
    std::string model_path;
    std::string lib_path;
    ExternalLoadableLifetime lifetime;
    std::shared_ptr<CatBoostLibHolder> api_provider;
    const CatBoostWrapperAPI * api;

    std::unique_ptr<CatBoostModelImpl> model;

    void init();
};

}
