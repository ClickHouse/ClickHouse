#pragma once

#include <QueryCoordination/Optimizer/Task/Task.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

class Memo;
class Group;

class DeriveStatGroup final : public Task
{
public:
    DeriveStatGroup(Memo & memo_, std::stack<std::unique_ptr<Task>> & stack_, ContextPtr context_, Group & group_)
        : Task(memo_, stack_, context_), group(group_)
    {
    }

    void execute() override;

private:
    std::unique_ptr<DeriveStatGroup> clone();

    Group & group;
};

}
