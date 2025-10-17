#include <Interpreters/IJoin.h>

namespace DB
{

class JoinResultFromBlock : public IJoinResult
{
public:
    explicit JoinResultFromBlock(Block block_) : block(std::move(block_)) {}

    JoinResultBlock next() override
    {
        return {std::move(block), true};
    }

private:
    Block block;
};

JoinResultPtr IJoinResult::createFromBlock(Block block)
{
    return std::make_unique<JoinResultFromBlock>(std::move(block));
}

}
