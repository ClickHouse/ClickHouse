#pragma once

#include <Core/Defines.h>

#include <Poco/AutoPtr.h>
#include <Poco/RefCountedObject.h>
#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{

class HTTP2ServerParams : public Poco::RefCountedObject
{
public:
    using Ptr = Poco::AutoPtr<HTTP2ServerParams>;

    static Ptr fromConfig(const Poco::Util::AbstractConfiguration & config);

    uint32_t getMaxConcurrentStreams() const noexcept { return max_concurrent_streams; }
    uint32_t getInitialWindowSize() const noexcept { return initial_window_size; }
    uint32_t getMaxFrameSize() const noexcept { return max_frame_size; }

private:
    uint32_t max_concurrent_streams = 100;
    uint32_t initial_window_size = DBMS_DEFAULT_BUFFER_SIZE;
    uint32_t max_frame_size = 65535;
};

}
