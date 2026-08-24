#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

class ASTCreateSettingsProfileQuery;
struct SettingsProfile;

class InterpreterCreateSettingsProfileQuery : public IInterpreter, WithMutableContext
{
public:
    InterpreterCreateSettingsProfileQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_) : WithMutableContext(context_), query_ptr(query_ptr_) {}

    BlockIO execute() override;

    static void updateSettingsProfileFromQuery(SettingsProfile & profile, const ASTCreateSettingsProfileQuery & query);

private:
    ASTPtr query_ptr;
};

}
