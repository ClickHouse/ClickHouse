#include <Interpreters/IExternalLoadable.h>
#include <Columns/IColumn.h>
#include <Columns/ColumnsNumber.h>
#include "CatBoostModel.h"

namespace DB
{

class MlpackModelImpl;

class MlpackModel : public IMLModel
{
public:
    MlpackModel(std::string name, std::string model_path,
                std::string method,
                const ExternalLoadableLifetime & lifetime);
    
    ~MlpackModel() override;

    ColumnPtr evaluate(const ColumnRawPtrs & columns) const override;
    std::string getTypeName() const override { return "mlpack"; }

    DataTypePtr getReturnType() const override;

    const ExternalLoadableLifetime & getLifetime() const override { return lifetime; }

    std::string getLoadableName() const override { return name; }

    bool supportUpdates() const override { return true; }

    bool isModified() const override { return true; }

    std::shared_ptr<const IExternalLoadable> clone() const override
    {
        return std::make_shared<MlpackModel>(name, model_path, method, lifetime);
    }

private:
    const std::string name;
    const std::string method;
    std::string model_path;
    ExternalLoadableLifetime lifetime;

    std::unique_ptr<MlpackModelImpl> model;

    void init();
};


}
