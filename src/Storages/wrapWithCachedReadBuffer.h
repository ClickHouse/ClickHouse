#include <IO/ReadBufferFromFile.h>


namespace DB
{

using ImplementationBufferPtr = std::shared_ptr<SeekableReadBuffer>;
using ImplementationBufferCreator = std::function<ImplementationBufferPtr()>;

using ModificationTimeGetter = std::function<std::optional<size_t>()>;

struct ReadSettings;

std::unique_ptr<ReadBufferFromFileBase> wrapWithCachedReadBuffer(
    ImplementationBufferCreator impl_creator,
    const std::string & object_path,
    size_t object_size,
    time_t object_modification_time,
    const ReadSettings & read_settings);

}
