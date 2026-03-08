#pragma once

#include <Interpreters/QueryLogElement.h>
#include <Interpreters/SystemLog.h>
#include "Parsers/parseQuery.h"
#include "Storages/StorageView.h"

namespace DB
{

struct UserQueryLogElement : QueryLogElement {
    static std::string name() { return "UserQueryLog"; }
};

class UserQueryLog : public SystemLog<UserQueryLogElement >
{
public:
    using SystemLog::SystemLog;

    UserQueryLog(ContextMutablePtr context_,
              const SystemLogSettings & settings_,
              std::shared_ptr<SystemLogQueue<UserQueryLogElement >> queue_ = nullptr);

    bool mustBePreparedAtStartup() const override {
        return true;
    }

    bool isView() const override {
        return true;
    }

protected:
    StoragePtr getStorage() const override;

private:
    ASTPtr create;
};

}
