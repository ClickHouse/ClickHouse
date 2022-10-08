#include <IO/ReadBufferFromFile.h>


namespace DB
{

using ImplementationBufferPtr = std::shared_ptr<ReadBufferFromFileBase>;
using ImplementationBufferCreator = std::function<ImplementationBufferPtr()>;

using ModificationTimeGetter = std::function<std::optional<size_t>()>;

struct ReadSettings;

std::unique_ptr<ReadBufferFromFileBase> wrapWithCachedReadBuffer(
    ImplementationBufferCreator impl_creator,
    ModificationTimeGetter modification_time_getter,
    const std::string & object_path,
    size_t object_size,
    const ReadSettings & read_settings);

}
