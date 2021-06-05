#pragma once
/* Copyright (c) 2018 BlackBerry Limited

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. */

#include <optional>
#include <Parsers/ASTAlterQuery.h>
#include <Storages/LiveView/StorageLiveView.h>
#include <Storages/StorageMaterializedView.h>


namespace DB
{

struct ViewCommand
{
    enum Type
    {
        REFRESH
    };

    Type type;

    ASTPtr values;

    static ViewCommand refresh(const ASTPtr & values)
    {
        ViewCommand res;
        res.type = REFRESH;
        res.values = values;
        return res;
    }

    static std::optional<ViewCommand> parse(ASTAlterCommand * command)
    {
        if (command->type == ASTAlterCommand::VIEW_REFRESH)
            return refresh(command->values);
        return {};
    }
};

typedef std::vector<ViewCommand> ViewCommands;

}
