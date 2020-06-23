#pragma once

#include <Compression/ICompressionCodec.h>

namespace DB
{



    class CompressionCodecFactory;
    void registerCodecPFor(CompressionCodecFactory & factory);

}
