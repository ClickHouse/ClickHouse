/* Some modifications Copyright (c) 2018 BlackBerry Limited

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

#include <Storages/IStorage.h>
#include <Storages/AlterCommands.h>
#include <Storages/MutationCommands.h>
#include <Storages/PartitionCommands.h>
#include <Interpreters/Context.h>
#include <Interpreters/IInterpreter.h>
#include <Parsers/ASTAlterQuery.h>


namespace DB
{

/** Allows you add or remove a column in the table.
  * It also allows you to manipulate the partitions of the MergeTree family tables.
  */
class InterpreterAlterQuery : public IInterpreter
{
public:
    InterpreterAlterQuery(const ASTPtr & query_ptr_, const Context & context_);

    BlockIO execute() override;

private:

    struct ParameterCommand
    {
        enum Type
        {
            ADD_TO_PARAMETER,
            DROP_FROM_PARAMETER,
            MODIFY_PARAMETER,
        };

        Type type;

        ASTPtr parameter;
        ASTPtr values;

        static ParameterCommand addToParameter(const ASTPtr & parameter, const ASTPtr & values)
        {
            ParameterCommand res;
            res.type = ADD_TO_PARAMETER;
            res.parameter = parameter;
            res.values = values;
            return res;
        }

        static ParameterCommand dropFromParameter(const ASTPtr & parameter, const ASTPtr & values)
        {
            ParameterCommand res;
            res.type = DROP_FROM_PARAMETER;
            res.parameter = parameter;
            res.values = values;
            return res;
        }

        static ParameterCommand modifyParameter(const ASTPtr & parameter, const ASTPtr & values)
        {
            ParameterCommand res;
            res.type = MODIFY_PARAMETER;
            res.parameter = parameter;
            res.values = values;
            return res;
        }
    };

    struct ChannelCommand
    {
        enum Type
        {
            ADD,
            DROP,
            SUSPEND,
            RESUME,
            REFRESH,
            MODIFY
        };

        Type type;

        ASTPtr values;

        static ChannelCommand add(const ASTPtr & values)
        {
            ChannelCommand res;
            res.type = ADD;
            res.values = values;
            return res;
        }

        static ChannelCommand drop(const ASTPtr & values)
        {
            ChannelCommand res;
            res.type = DROP;
            res.values = values;
            return res;
        }

        static ChannelCommand suspend(const ASTPtr & values)
        {
            ChannelCommand res;
            res.type = SUSPEND;
            res.values = values;
            return res;
        }

        static ChannelCommand resume(const ASTPtr & values)
        {
            ChannelCommand res;
            res.type = RESUME;
            res.values = values;
            return res;
        }

        static ChannelCommand refresh(const ASTPtr & values)
        {
            ChannelCommand res;
            res.type = REFRESH;
            res.values = values;
            return res;
        }

        static ChannelCommand modify(const ASTPtr & values)
        {
            ChannelCommand res;
            res.type = MODIFY;
            res.values = values;
            return res;
        }
    };

    class PartitionCommands : public std::vector<PartitionCommand>
    {
    public:
        void validate(const IStorage * table);
    };

    class ParameterCommands : public std::vector<ParameterCommand>
    {
    public:
        void validate(const IStorage * table);
    };

    class ChannelCommands : public std::vector<ChannelCommand>
    {
    public:
        void validate(const IStorage * table);
    };

    ASTPtr query_ptr;

    const Context & context;
};

}
