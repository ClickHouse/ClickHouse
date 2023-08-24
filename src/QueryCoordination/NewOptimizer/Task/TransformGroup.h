#pragma once


namespace DB
{

class Group;

class TransformGroup
{
public:
    TransformGroup(Group & group_) : group(group_) {}

    void execute();

private:
    Group & group;
};

}
