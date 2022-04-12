#include <iostream>

#include <IO/MemoryReadWriteBuffer.h>
#include <Storages/MergeTree/MergeTreeWriteAheadLog.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_FORMAT_VERSION;
}
}

int main(int, char **)
{
    try
    {
        {
            std::cout << "test: dummy test" << std::endl;

            DB::MergeTreeWriteAheadLog::ActionMetadata metadata_out;
            DB::MemoryWriteBuffer buf{};

            metadata_out.write(buf);
            buf.finalize();

            metadata_out.read(*buf.tryGetReadBuffer());
        }

        {
            std::cout << "test: min compatibility" << std::endl;

            DB::MergeTreeWriteAheadLog::ActionMetadata metadata_out;
            metadata_out.min_compatible_version = DB::MergeTreeWriteAheadLog::WAL_VERSION + 1;
            DB::MemoryWriteBuffer buf{};

            metadata_out.write(buf);
            buf.finalize();

            try
            {
                metadata_out.read(*buf.tryGetReadBuffer());
            }
            catch (const DB::Exception & e)
            {
                if (e.code() != DB::ErrorCodes::UNKNOWN_FORMAT_VERSION)
                {
                    std::cerr << "Expected UNKNOWN_FORMAT_VERSION exception but got: "
                        << e.what() << ", " << e.displayText() << std::endl;
                }
            }
        }
    }
    catch (const DB::Exception & e)
    {
        std::cerr << e.what() << ", " << e.displayText() << std::endl;
        return 1;
    }

    return 0;
}
