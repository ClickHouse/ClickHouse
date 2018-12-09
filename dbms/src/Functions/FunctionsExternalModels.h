#pragma once
#include <Functions/IFunction.h>

namespace DB
{

class ExternalModels;

/// Evaluate external model.
/// First argument - model name, the others - model arguments.
///   * for CatBoost model - float features first, then categorical
/// Result - Float64.
class FunctionModelEvaluate final : public IFunction
{
public:
    static constexpr auto name = "modelEvaluate";

    static FunctionPtr create(const Context & context);

    explicit FunctionModelEvaluate(const ExternalModels & models) : models(models) {}

    String getName() const override { return name; }

    bool isDeterministic() const override { return false; }

    String getSignature() const override { return "f(const model String, T1, ...) -> Float64"; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override;

private:
    const ExternalModels & models;
};

}
