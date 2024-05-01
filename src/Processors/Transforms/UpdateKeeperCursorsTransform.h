#pragma once

#include <Common/ZooKeeper/ZooKeeper.h>

#include <Processors/ISimpleTransform.h>

namespace DB
{

class UpdateKeeperCursorsTransform : public ISimpleTransform
{
public:
    UpdateKeeperCursorsTransform(Block header_, zkutil::ZooKeeperPtr zk_);

    String getName() const override { return "UpdateKeeperCursorsTransform"; }

protected:
    void transform(Chunk & chunk) override;

private:
    zkutil::ZooKeeperPtr zk;
};

}
