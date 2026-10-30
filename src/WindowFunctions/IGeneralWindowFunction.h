#pragma once

#include <Core/Field.h>
#include <Core/IResolvedFunction.h>
#include <Interpreters/WindowDescription.h>
#include <WindowFunctions/Common/WindowFunctionRequirements.h>

#include <memory>
#include <optional>

namespace DB
{

class IColumn;
struct WindowBounds;
class WindowArguments;

class IGeneralWindowFunction : public IResolvedFunction
{
public:
    IGeneralWindowFunction(String name_, DataTypes argument_types_, Array parameters_, DataTypePtr result_type_);

    const String & getName() const;
    const DataTypes & getArgumentTypes() const override;
    const Array & getParameters() const override;
    const DataTypePtr & getResultType() const override;

    virtual WindowFunctionRequirements getRequirements(const WindowDescription & window) const = 0;
    virtual std::optional<WindowFrame> getDefaultFrame() const;
    virtual void checkWindowFrameType(const WindowDescription & window) const;

    virtual size_t sizeOfState() const;
    virtual size_t alignOfState() const;
    virtual void createState(char * place) const;
    virtual void destroyState(char * place) const noexcept;
    virtual void resetPartition(char * place) const;
    virtual void evaluate(char * place, const WindowBounds & bounds, const WindowArguments & arguments, IColumn & result) const = 0;

protected:
    const String name;
    const DataTypes argument_types;
    const Array parameters;
    const DataTypePtr result_type;
};

using GeneralWindowFunctionPtr = std::shared_ptr<const IGeneralWindowFunction>;

}
