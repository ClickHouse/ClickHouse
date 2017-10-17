#include <Functions/IFunction.h>

namespace  DB
{


class ExternalModles;

class FunctionModelEvaluate final : public IFunction
{
public:
    static constexpr auto name = "modelEvaluate";

    static FunctionPtr create(const Context & context);

    explicit FunctionModelEvaluate(const ExternalModles & models) : models(models) {}

    String getName() const override { return name; }

private:
    bool isVariadic() const override
    { return true; }

    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override;


    const ExternalModles & models;
};

}
