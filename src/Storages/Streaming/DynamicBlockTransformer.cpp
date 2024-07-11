#include <Storages/Streaming/DynamicBlockTransformer.h>

namespace DB
{

DynamicBlockTransformer::DynamicBlockTransformer(Block desired_)
  : desired_metadata(std::move(desired_)) {}

const Block & DynamicBlockTransformer::getHeader() const
{
    return desired_metadata;
}

void DynamicBlockTransformer::transform(Block & block)
{
    Block metadata_block = block.cloneEmpty();

    /// if metadata changed, we must recalculate converting actions
    if (!blocksHaveEqualStructure(current_metadata, metadata_block))
    {
        auto convert_actions_dag = ActionsDAG::makeConvertingActions(
            metadata_block.getColumnsWithTypeAndName(),
            desired_metadata.getColumnsWithTypeAndName(),
            ActionsDAG::MatchColumnsMode::Name);

        current_metadata = std::move(metadata_block);
        converter = std::make_shared<ExpressionActions>(std::move(convert_actions_dag));
    }

    chassert(converter != nullptr);
    converter->execute(block);
}

}
