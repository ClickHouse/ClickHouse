#pragma once
#include <Functions/IFunctionImpl.h>

namespace DB
{

class ExternalModelsLoader;

/// Evaluate external model.
/// First argument - model name, the others - model arguments.
///   * for CatBoost model - float features first, then categorical
/// Result - Float64.
class FunctionModelEvaluate final : public IFunction
{
public:
    static constexpr auto name = "modelEvaluate";

    static FunctionPtr create(const Context & context);

    explicit FunctionModelEvaluate(const ExternalModelsLoader & models_loader_) : models_loader(models_loader_) {}

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }

    bool isDeterministic() const override { return false; }

    bool useDefaultImplementationForNulls() const override { return false; }

    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override;

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) const override;

private:
    const ExternalModelsLoader & models_loader;
};

}
