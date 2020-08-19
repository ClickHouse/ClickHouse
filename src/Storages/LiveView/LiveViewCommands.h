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
#pragma once

#include <optional>
#include <Parsers/ASTAlterQuery.h>
#include <Storages/LiveView/StorageLiveView.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_STORAGE;
}

struct LiveViewCommand
{
    enum Type
    {
        REFRESH
    };

    Type type;

    ASTPtr values;

    static LiveViewCommand refresh(const ASTPtr & values)
    {
        LiveViewCommand res;
        res.type = REFRESH;
        res.values = values;
        return res;
    }

    static std::optional<LiveViewCommand> parse(ASTAlterCommand * command)
    {
        if (command->type == ASTAlterCommand::LIVE_VIEW_REFRESH)
            return refresh(command->values);
        return {};
    }
};


class LiveViewCommands : public std::vector<LiveViewCommand>
{
public:
    void validate(const IStorage & table)
    {
        if (!empty() && !dynamic_cast<const StorageLiveView *>(&table))
            throw Exception("Wrong storage type. Must be StorageLiveView", DB::ErrorCodes::UNKNOWN_STORAGE);
    }
};

}
