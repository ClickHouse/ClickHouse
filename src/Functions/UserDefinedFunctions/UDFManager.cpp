#include "UDFManager.h"
#include <Functions/IFunctionImpl.h>

#include <dlfcn.h>

namespace DB
{
class FunctionDateDiff : public IFunction
{
public:
    static constexpr auto name = "Name"; /// @TODO Igr
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionDateDiff>(); } /// @TODO Igr

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED); /// @TODO Igr
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {}; } /// @TODO Igr

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        /// @TODO Igr
    }
};

void UDFManager::run()
{
}

[[noreturn]] void UDFManager::runIsolated()
{
    std::string line;
    while (true)
    {
        std::getline(inCommands_, line);
        if (line.starts_with("LoadLib "))
        {
            auto filename = std::string_view(line).substr(8);
            void * handle = dlopen(filename.data(), RTLD_LAZY);
            if (!handle)
            {
                outCommands_ << "Failed to load\n";
                continue;
            }
        }
    }
}
}
