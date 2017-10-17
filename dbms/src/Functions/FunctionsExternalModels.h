

class FunctionModelEvaluate final : public IFunction
{
public:
    static constexpr auto name = "modelEvaluate";

    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<FunctionModelEvaluate>(context.getExternalDictionaries());
    }

    FunctionModelEvaluate(const ExternalModles & models) : models(models) {}

    String getName() const override { return name; }

private:
    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;

    void executeImpl(Block & block, const ColumnNumbers & arguments, const size_t result) override;


    const ExternalModles & models;
};
