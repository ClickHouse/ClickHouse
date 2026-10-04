#include <Processors/Executors/Runtime/Tools/Wyrd/Adversary/NopAdversary.h>

namespace DB::Runtime::Wyrd
{

namespace
{

class NopAdversary : public IAdversary
{
public:
    void reset() override
    {
    }

    std::string report() override
    {
        return "nop";
    }

    void threadEnter() override
    {
    }

    void threadFault() override
    {
    }

    bool threadSpuriousFailure() override
    {
        return false;
    }

    void threadSetLockFree(bool) override
    {
    }

    void threadProgress() override
    {
    }

    void threadExit() override
    {
    }
};

}

AdversaryPtr createNopAdversary()
{
    return std::make_shared<NopAdversary>();
}

}
