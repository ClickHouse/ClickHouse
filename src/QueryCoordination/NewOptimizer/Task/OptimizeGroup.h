#pragma once


namespace DB
{

class Group;
class PhysicalProperties;

class OptimizeGroup
{
public:
    OptimizeGroup(Group & group_, const PhysicalProperties & required_prop_) : group(group_), required_prop(required_prop_) {}

    void execute();

private:
    Group & group;
    const PhysicalProperties & required_prop;
};

}
