#pragma once

#include <Poco/Runnable.h>
#include <Poco/Logger.h>
#include <Common/Pipe.h>
#include <ext/singleton.h>

namespace DB
{

    using Poco::Logger;

    class TraceCollector : public Poco::Runnable
    {
    private:
        Logger * log;

    public:
        explicit TraceCollector();

        void run() override;

        static const size_t buf_size;
    };
}
