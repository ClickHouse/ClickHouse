#pragma once

#include <memory>

#include <Interpreters/Context_fwd.h>
#include <Interpreters/ExternalLoader.h>
#include <Functions/UserDefined/UserDefinedExecutableFunction.h>

namespace DB
{

class IExternalLoaderConfigRepository;

/// Manages external user-defined functions.
class ExternalUserDefinedExecutableFunctionsLoader : public ExternalLoader, WithContext
{
public:

    using UserDefinedExecutableFunctionPtr = std::shared_ptr<const UserDefinedExecutableFunction>;

    /// External user-defined functions will be loaded immediately and then will be updated in separate thread, each 'reload_period' seconds.
    explicit ExternalUserDefinedExecutableFunctionsLoader(ContextPtr global_context_);

    UserDefinedExecutableFunctionPtr getUserDefinedFunction(const std::string & user_defined_function_name) const;

    UserDefinedExecutableFunctionPtr tryGetUserDefinedFunction(const std::string & user_defined_function_name) const;

    void reloadFunction(const std::string & user_defined_function_name) const;

protected:
    LoadableMutablePtr createObject(const std::string & name,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & key_in_config,
        const std::string & repository_name) const override;

};

}
