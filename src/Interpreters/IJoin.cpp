#include <Interpreters/IJoin.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

JoinBuildContext JoinBuildContext::forStream(JoinBuildStreamKey, size_t stream, size_t num_streams)
{
    if (stream >= num_streams || num_streams > std::numeric_limits<UInt32>::max())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Build stream {} of {} is out of range", stream, num_streams);
    return JoinBuildContext(static_cast<UInt32>(stream), static_cast<UInt32>(num_streams), /*join_checks_limits_=*/true);
}

class JoinResultFromBlock : public IJoinResult
{
public:
    explicit JoinResultFromBlock(Block block_) : block(std::move(block_)) {}

    JoinResultBlock next() override
    {
        return {std::move(block), nullptr, true};
    }

private:
    Block block;
};

JoinResultPtr IJoinResult::createFromBlock(Block block)
{
    return std::make_unique<JoinResultFromBlock>(std::move(block));
}

}
