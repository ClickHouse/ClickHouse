#pragma once

#include <QueryCoordination/Optimizer/Task/Task.h>

namespace DB
{

class Group;

class TransformGroup final : public Task
{
public:
    TransformGroup(Memo & memo_, std::stack<std::unique_ptr<Task>> & stack_, ContextPtr context_, Group & group_)
        : Task(memo_, stack_, context_), group(group_)
    {
    }

    void execute() override;

private:
    Group & group;
};

}
