#pragma once

#include <memory>
#include <string>

namespace DB::Runtime::Wyrd
{

class IAdversary
{
public:
    virtual ~IAdversary() = default;

    virtual void reset() = 0;
    virtual std::string report() = 0;

    virtual void threadEnter() = 0;
    virtual void threadFault() = 0;
    virtual bool threadSpuriousFailure() = 0;
    virtual void threadSetLockFree(bool on) = 0;
    virtual void threadProgress() = 0;
    virtual void threadExit() = 0;
};

using AdversaryPtr = std::shared_ptr<IAdversary>;

}
