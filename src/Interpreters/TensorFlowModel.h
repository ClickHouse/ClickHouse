#pragma once
#include <Interpreters/CatBoostModel.h>

namespace DB
{

struct TensorFlowWrapperAPI;

class TensorFlowWrapperAPIProvider
{
public:
    virtual ~TensorFlowWrapperAPIProvider() = default;
    virtual const TensorFlowWrapperAPI & getAPI() const = 0;
};

class ITensorFlowModelImpl
{
public:
    virtual ~ITensorFlowModelImpl() = default;
    virtual ColumnPtr evaluate(const ColumnRawPtrs & columns) const = 0;
    virtual DataTypePtr getReturnType() const = 0;
};


class TensorFlowModel : public IModel
{
public:
    TensorFlowModel(std::string name, std::string model_path,
                    std::string lib_path, const ExternalLoadableLifetime & lifetime);

    ColumnPtr evaluate(const ColumnRawPtrs & columns) const override;
    std::string getTypeName() const override { return "tensorflow"; }
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

    std::unique_ptr<ITensorFlowModelImpl> model;
};

}
