#include <Functions/IFunctionImpl.h>


namespace DB
{

/** Conversion to fixed string is implemented only for strings.
  */
class FunctionToFixedString : public IFunction
{
public:
    static constexpr auto name = "toFixedString";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionToFixedString>(); }
    static FunctionPtr create() { return std::make_shared<FunctionToFixedString>(); }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 2; }
    bool isInjective(const Block &) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override;

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override;

    static void executeForN(Block & block, const ColumnNumbers & arguments, const size_t result, const size_t n);
};

}

