#pragma once

#include <iostream>

namespace DB
{


// Interface, which handles descriptor pairs, that could be attached to embedded client
class IClientDescriptorSet
{
public:
    struct DescriptorSet
    {
        int in = -1;
        int out = -1;
        int err = -1;
    };

    struct StreamSet
    {
        std::istream & in;
        std::ostream & out;
        std::ostream & err;
    };
    virtual DescriptorSet getDescriptorsForClient() = 0;

    virtual DescriptorSet getDescriptorsForServer() = 0;

    virtual StreamSet getStreamsForClient() = 0;

    virtual bool isPty() const = 0;

    virtual void closeServerDescriptors() = 0;

    virtual ~IClientDescriptorSet() = default;
};

}
